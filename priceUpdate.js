import AdmZip from "adm-zip";

import fs from "fs";
import { parse } from "csv-parse";
import * as csv from "csv";
import Shopify from "shopify-api-node";
import {
  INGRAM_HOST,
  INGRAM_PORT,
  SHOP_NAME,
  apiVersion,
  SHOPIFY_ACCESS_TOKEN,
  INGRAM_USERNAME,
  INGRAM_PASSWORD,
  MARGIN_PERCENTAGE,
  directoryPath,
} from "./const.js";

import { Client } from "ssh2";
import axios from "axios";
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const conn = new Client();
const remoteDir = "/";
const remoteFile = remoteDir + "/PRICE.ZIP";
const localFile = directoryPath + "/PRICE.ZIP";
const baseURL = `https://${SHOP_NAME}.myshopify.com/admin/api/${apiVersion}/graphql.json`;
const shopify = new Shopify({
  shopName: SHOP_NAME,
  accessToken: SHOPIFY_ACCESS_TOKEN,
});
const shopifyN = axios.create({
  baseURL: `https://${SHOP_NAME}.myshopify.com/admin/api/2023-07`,
  headers: {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json",
  },
});
const getFTPCSVFile = () => {
  console.log("inside getFTPCSVFile");
  return new Promise((resolve, reject) => {
    conn
      .on("ready", () => {
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
        username: INGRAM_USERNAME,
        password: INGRAM_PASSWORD,
        port: INGRAM_PORT,
        secure: true, // true if you want to use FTPS
      });
  });
};

async function getAllProducts() {
  let products = [];
  let hasNextPage = true;
  let pageInfo = null;

  try {
    while (hasNextPage) {
      const params = { limit: 250 }; // Maximum limit per request
      if (pageInfo) {
        params.page_info = pageInfo;
      }
      const response = await shopifyN.get("/products.json", { params });

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

async function deletEmptyColumns(row) {
  return new Promise((resolve, reject) => {
    var value = "DELETE";

    row = row.filter(function (item) {
      return item !== value;
    });

    resolve(row);
  });
}
async function modifyCsvFile(ingramCsvFile) {
  console.log(ingramCsvFile);
  let ingramCsvFileProcessed = directoryPath + "/data-processed.csv";
  return new Promise((resolve, reject) => {
    try {
      fs.createReadStream(ingramCsvFile)
        .pipe(csv.parse({ columns: false }))
        .pipe(
          csv.transform((input) => {
            if (
              input[3].indexOf("LENOVO") >= 0 ||
              input[3].indexOf("ALOGIC") >= 0 ||
              input[3].indexOf("JABRA") >= 0 ||
              input[3].indexOf("LOGITECH") >= 0
            ) {
              //console.log(input[3]);
              for (let index = 0; index < 22; index++) {
                if (index == 1 || index == 14) continue;
                input[index] = "DELETE";
              }
              let row = deletEmptyColumns(input).then((response) => {
                return response;
              });
              return row;
            }
          })
        )
        .pipe(csv.stringify({ header: false }))
        .pipe(fs.createWriteStream(ingramCsvFileProcessed))
        .on("finish", () => {
          resolve(ingramCsvFileProcessed);
        });
    } catch (error) {
      console.error("Error modifying csv file:", error);
    }
  });
}

const calculateNewPrice = ({ sku, id }, ingramCsvFile) => {
  let inventoryVariant = {};

  return new Promise((resolve, reject) => {
    // Implement your price calculation logic here
    fs.createReadStream(ingramCsvFile)
      .pipe(
        parse({
          columns: false, // Treat the first row as headers
          skip_empty_lines: true, // Skip empty lines
        })
      )
      .on("data", (row) => {
        let csvSku = row[0].trim();
        let csvPrice = row[1];

        if (sku == csvSku) {
          csvPrice = parseFloat(csvPrice).toFixed(2) * MARGIN_PERCENTAGE;
          inventoryVariant = '{"id": "' + id + '","price": ' + csvPrice + "}";
        }
      })
      .on("end", () => {
        resolve(inventoryVariant);
      })
      .on("error", (err) => {
        console.error("Error parsing CSV:", err);
      });
  });
};

const updateVariantPriceMutation = (variantId, newPrice) => `
  mutation {
    productVariantUpdate(input: {id: "${variantId}", price: "${newPrice}"}) {
      productVariant {
        id
        price
        sku
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const updateVariantPrice = async (variantId, newPrice) => {
  try {
    const response = await axios.post(
      baseURL,
      {
        query: updateVariantPriceMutation(variantId, newPrice),
      },
      {
        headers: {
          "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
          "Content-Type": "application/json",
        },
      }
    );
    return response;
  } catch (error) {
    console.error(`Error updating variant ${variantId}:`, error);
    return error;
  }
};

async function updateVariant(variant) {
  try {
    await shopify.productVariant.update(variant.id, { price: variant.price });
    console.log(`Updated variant ${variant.id} to price ${variant.price}`);
  } catch (error) {
    console.error(`Error updating variant ${variant.id}:`, error.message);
    console.log("retrying...");
    await delay(5000);
    await updateVariant(variant);
    console.log(
      `Updated variant ${variant.id} to price ${variant.price} after retrying`
    );
    // Retry logic can be added here if needed
  }
}

async function updateVariants(variants, batchSize = 5, delayMs = 2000) {
  console.log("inside Variants!");
  for (let i = 0; i < variants.length; i += batchSize) {
    const batch = variants.slice(i, i + batchSize);
    await Promise.all(batch.map(updateVariant));
    console.log(`Processed batch ${i / batchSize + 1}`);
    if (i + batchSize < variants.length) {
      await delay(delayMs);
    }
  }
  console.log("All variants updated successfully.");
}
const getVariants = async (products, file) => {
  let variantsArray = [];
  for (const product of products) {
    for (const variant of product.variants) {
      await calculateNewPrice(variant, file).then((response) => {
        if (Object.keys(response).length) {
          let newVariant = JSON.parse(response);
          variantsArray.push(newVariant);
        }
      });
    }
  }
  return variantsArray;
};
const updateProductPrices = async () => {
  try {
    const products = await getAllProducts();

    let ingramCsvFile = directoryPath + "/PRICE.TXT";
    let files = [];
    // await getFTPCSVFile().then((zipFile) => {
    //   const zip = new AdmZip(zipFile);
    //   zip.extractAllTo(directoryPath);
    // });
    await modifyCsvFile(ingramCsvFile).then((modifiedCsvFile) => {
      files.push(modifiedCsvFile);
    });
    await getVariants(products, directoryPath + "/data-processed.csv").then(
      (variants) => {
        updateVariants(variants);
        console.log(variantsArray);
      }
    );
    console.log("success");
    return "Success";
  } catch (error) {
    return error.message;
  }
};

updateProductPrices();
