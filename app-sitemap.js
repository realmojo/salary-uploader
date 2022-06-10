import { MongoClient } from "mongodb";
import fs from "fs";
import moment from "moment";
import convert from "xml-js";

console.log("시작합니다.");
const uri = "mongodb://salaryinfo.co.kr:27017";
const client = new MongoClient(uri);

await client.connect();

const db = client.db("salaryinfo");
const companies = db.collection("companies");

async function run() {
  try {
    console.log("데이터를 DB에서 가져옵니다..");

    const json = fs.readFileSync("./sitemap.json", "utf8");
    const jsonIndex = fs.readFileSync("./sitemap_index.json", "utf8");
    // const json = fs.readFileSync("./sitemap.json", "utf8");
    let d = JSON.parse(json);
    const dIndex = JSON.parse(jsonIndex);
    const items = await companies
      .aggregate([
        {
          $group: {
            _id: { code: "$code", codeName: "$codeName" },
          },
        },
        {
          $sort: {
            code: 1,
            _id: 1,
          },
        },
      ])
      .toArray();

    dIndex.sitemapindex.sitemap.push({
      loc: {
        _text: `https://salaryinfo.co.kr/category-sitemap.xml`,
      },
    });

    for (const item of items) {
      if (item._id.code && item._id.codeName) {
        d.urlset.url.push({
          loc: {
            _text: `https://salaryinfo.co.kr/category/${
              item._id.code
            }/${encodeURIComponent(item._id.codeName)}`,
          },
          lastmod: { _text: moment().format("YYYY-MM-DD") },
        });
      }
    }

    const options = { compact: true, ignoreComment: true, spaces: 2 };

    var result = convert.json2xml(JSON.stringify(d), options);
    fs.writeFile("./sitemap/category-sitemap.xml", result, function (err) {
      if (err !== null) {
        console.log("sitemap fail");
      }
    });

    d.urlset.url = [];
    console.log("대량의 데이터를 가져옵니다.");
    let currentItems = await companies.find({}).sort({ _id: 1 }).toArray();

    let count = 0;
    let j = 1;
    const length = currentItems.length;
    for (const item of currentItems) {
      d.urlset.url.push({
        loc: {
          _text: `https://salaryinfo.co.kr/company/${
            item._id
          }/${encodeURIComponent(item.title)}`,
        },
        lastmod: { _text: moment().format("YYYY-MM-DD") },
      });

      count++;
      if (count % 3000 === 0 || length === count) {
        dIndex.sitemapindex.sitemap.push({
          loc: {
            _text: `https://salaryinfo.co.kr/post-sitemap-${j}.xml`,
          },
        });
        result = convert.json2xml(JSON.stringify(d), options);
        fs.writeFile(`./sitemap/post-sitemap-${j}.xml`, result, function (err) {
          if (err !== null) {
            console.log("sitemap fail");
          }
        });
        d.urlset.url = [];
        j++;
      }
    }

    var result2 = convert.json2xml(JSON.stringify(dIndex), options);
    fs.writeFile("./sitemap/sitemap_index.xml", result2, function (err) {
      if (err !== null) {
        console.log("sitemap fail");
      }
    });
  } finally {
    console.log(`끝났습니다.`);
    await client.close();
  }
}
run();
