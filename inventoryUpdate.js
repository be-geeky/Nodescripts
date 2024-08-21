import fs from "fs";
import { parse } from "csv-parse";
import { Client } from "ssh2";

import dotenv from "dotenv";
dotenv.config();
import axios from "axios";
import _ from "lodash";

import {
  INGRAM_HOST,
  INGRAM_PORT,
  SHOP_NAME,
  INGRAM_INVENTORY_USERNAME,
  INGRAM_INVENTORY_PASSWORD,
  SHOPIFY_ACCESS_TOKEN,
  INVENTORY_LOCATION_ID,
  directoryPath,
} from "./constants.js";

const conn = new Client();
const remoteDir = "/";
const remoteFile = "/TOTAL.TXT";
const localFile = directoryPath + "/TOTAL.csv";

const shopify = axios.create({
  baseURL: `https://${SHOP_NAME}.myshopify.com/admin/api/2023-07`,
  headers: {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json",
  },
});

async function getAllProducts() {
  let products = [];
  let hasNextPage = true;
  let pageInfo = null;
  console.log("in getAllProducts");
  try {
    while (hasNextPage) {
      const params = { limit: 250 }; // Maximum limit per request
      if (pageInfo) {
        params.page_info = pageInfo;
      }
      const response = await shopify.get("/products.json", { params });

      const newProducts = response.data.products;
      products = products.concat(newProducts);

      const linkHeader = response.headers.link;
      if (linkHeader) {
        const pageInfoMatch = linkHeader.match(
          /<.*page_info=([^&]*)>; rel="next"/
        );
        if (pageInfoMatch) {
          pageInfo = pageInfoMatch[1];
        } else {
          hasNextPage = false;
        }
      } else {
        hasNextPage = false;
      }
    }
  } catch (error) {
    console.error("Error fetching products:", error);
  }

  return products;
}
const getIngramProducts = () => {
  console.log("in getIngramProducts");
  return new Promise((resolve, reject) => {
    conn
      .on("ready", () => {
        console.log("Client :: ready");
        conn.sftp((err, sftp) => {
          if (err) throw err;

          sftp.fastGet(remoteFile, localFile, (err) => {
            if (err) throw err;
            resolve(localFile);
            conn.end();
          });
        });
      })
      .connect({
        host: INGRAM_HOST,
        username: INGRAM_INVENTORY_USERNAME,
        password: INGRAM_INVENTORY_PASSWORD,
        port: INGRAM_PORT,
        secure: true, // true if you want to use FTPS
      });
  });
};

async function updateVariants(inventories) {
  let data = JSON.stringify({
    query: `mutation {
    inventoryAdjustQuantities(input: {
      name: "available",
      reason: "correction",   
      changes: [${inventories}]
      }) {
      inventoryAdjustmentGroup {
        createdAt
        reason
        app {
          id
        }
        changes {
          name
          delta
          quantityAfterChange
        }
      }
      userErrors {
        field
        message
      }
    }
  }`,
    variables: {},
  });

  let config = {
    method: "post",
    maxBodyLength: Infinity,
    url: `https://${SHOP_NAME}.myshopify.com/admin/api/2023-04/graphql.json`,
    headers: {
      "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
      "Content-Type": "application/json",
    },
    data: data,
  };

  axios
    .request(config)
    .then((response) => {
      console.log("updated");
    })
    .then(() => {})
    .catch((error) => {
      console.log(error);
    });
}
async function processProductsInBatches(inventories) {
  let batches = _.chunk(inventories, 50);
  for (let index = 0; index < batches.length; index++) {
    await updateVariants(batches[index]);
  }
}
const updateProductInventories = async () => {
  console.log("under updateProductInventories");
  let delta = 0;
  let inventoryVariant = {};
  let data = [];
  let ingraCsvFile = [];
  try {
    await getIngramProducts().then((file) => {
      ingraCsvFile.push(file);
    });
    await getAllProducts().then((products) => {
      console.log(`Retrieved ${products.length} products`);
      // Read the CSV file
      fs.createReadStream(ingraCsvFile[0])
        .pipe(
          parse({
            columns: false, // Treat the first row as headers
            skip_empty_lines: true, // Skip empty lines
          })
        )
        .on("data", (row) => {
          let sku = row[0].trim();
          let qty = row[1];
          products.forEach((product) => {
            product.variants.forEach((variant) => {
              if (variant.sku == sku) {
                if (variant.inventory_quantity != qty) {
                  delta = qty - variant.inventory_quantity;
                  inventoryVariant =
                    '{inventoryItemId: "gid://shopify/InventoryItem/' +
                    variant.inventory_item_id +
                    '",locationId: "gid://shopify/Location/' +
                    INVENTORY_LOCATION_ID +
                    '", delta:' +
                    delta +
                    "}";
                  data.push(inventoryVariant);
                }
              }
            });
          });
        })
        .on("end", () => {
          processProductsInBatches(data);
          console.log("CSV file successfully processed");
        })
        .on("error", (err) => {
          console.error("Error parsing CSV:", err);
        });
    });

    console.log("success");
    return "Success";
  } catch (error) {
    return error.message;
  }
};

updateProductInventories();
