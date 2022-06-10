import { MongoClient } from "mongodb";

console.log("시작합니다.");
const uri = "mongodb://salaryinfo.co.kr:27017";
const client = new MongoClient(uri);

await client.connect();

const db = client.db("salaryinfo");
const companies = db.collection("companies");
const companySize = db.collection("companysizes");

async function run() {
  try {
    console.log("데이터를 DB에서 가져옵니다..");

    let currentItems = await companies.find({}).toArray();

    console.log(`데이터 갯수 : ${currentItems.length}`);

    let t = [];
    for (let i = 0; i < 17; i++) {
      t.push({
        insertOne: {
          document: {
            min: 0,
            max: 0,
            count: 0,
          },
        },
      });
    }

    let count = 0;
    for (const item of currentItems) {
      if (item.totalEmployer >= 1 && item.totalEmployer <= 5) {
        // 0
        t[0].insertOne.document.min = 1;
        t[0].insertOne.document.max = 5;
        t[0].insertOne.document.count++;
      } else if (item.totalEmployer >= 6 && item.totalEmployer <= 10) {
        // 1
        t[1].insertOne.document.min = 6;
        t[1].insertOne.document.max = 10;
        t[1].insertOne.document.count++;
      } else if (item.totalEmployer >= 11 && item.totalEmployer <= 15) {
        // 2
        t[2].insertOne.document.min = 11;
        t[2].insertOne.document.max = 15;
        t[2].insertOne.document.count++;
      } else if (item.totalEmployer >= 16 && item.totalEmployer <= 20) {
        // 3
        t[3].insertOne.document.min = 16;
        t[3].insertOne.document.max = 20;
        t[3].insertOne.document.count++;
      } else if (item.totalEmployer >= 21 && item.totalEmployer <= 30) {
        // 4
        t[4].insertOne.document.min = 21;
        t[4].insertOne.document.max = 30;
        t[4].insertOne.document.count++;
      } else if (item.totalEmployer >= 31 && item.totalEmployer <= 40) {
        // 5
        t[5].insertOne.document.min = 31;
        t[5].insertOne.document.max = 40;
        t[5].insertOne.document.count++;
      } else if (item.totalEmployer >= 41 && item.totalEmployer <= 50) {
        // 6
        t[6].insertOne.document.min = 41;
        t[6].insertOne.document.max = 50;
        t[6].insertOne.document.count++;
      } else if (item.totalEmployer >= 51 && item.totalEmployer <= 60) {
        // 7
        t[7].insertOne.document.min = 51;
        t[7].insertOne.document.max = 60;
        t[7].insertOne.document.count++;
      } else if (item.totalEmployer >= 61 && item.totalEmployer <= 70) {
        // 8
        t[8].insertOne.document.min = 61;
        t[8].insertOne.document.max = 70;
        t[8].insertOne.document.count++;
      } else if (item.totalEmployer >= 71 && item.totalEmployer <= 80) {
        // 9
        t[9].insertOne.document.min = 71;
        t[9].insertOne.document.max = 80;
        t[9].insertOne.document.count++;
      } else if (item.totalEmployer >= 81 && item.totalEmployer <= 90) {
        // 10
        t[10].insertOne.document.min = 81;
        t[10].insertOne.document.max = 90;
        t[10].insertOne.document.count++;
      } else if (item.totalEmployer >= 91 && item.totalEmployer <= 100) {
        // 11
        t[11].insertOne.document.min = 91;
        t[11].insertOne.document.max = 100;
        t[11].insertOne.document.count++;
      } else if (item.totalEmployer >= 101 && item.totalEmployer <= 500) {
        // 12
        t[12].insertOne.document.min = 101;
        t[12].insertOne.document.max = 500;
        t[12].insertOne.document.count++;
      } else if (item.totalEmployer >= 501 && item.totalEmployer <= 1000) {
        // 13
        t[13].insertOne.document.min = 501;
        t[13].insertOne.document.max = 1000;
        t[13].insertOne.document.count++;
      } else if (item.totalEmployer >= 1001 && item.totalEmployer <= 5000) {
        // 14
        t[14].insertOne.document.min = 1001;
        t[14].insertOne.document.max = 5000;
        t[14].insertOne.document.count++;
      } else if (item.totalEmployer >= 5001 && item.totalEmployer <= 10000) {
        // 15
        t[15].insertOne.document.min = 5001;
        t[15].insertOne.document.max = 10000;
        t[15].insertOne.document.count++;
      } else if (item.totalEmployer >= 10001) {
        // 16
        t[16].insertOne.document.min = 10001;
        t[16].insertOne.document.max = 100000;
        t[16].insertOne.document.count++;
      }

      if (count % 1000 === 0) {
        console.log(t[0].insertOne.document);
      }
      count++;
      // let t = { _id: ++allCount, ...nextItem };
      // insertBulk.push({
      //   insertOne: {
      //     document: t,
      //   },
      // });
      // await companies.bulkWrite([
      //   {
      //     insertOne: {
      //       document: t,
      //     },
      //   },
      // ]);
    }
    console.log("벌크로 갱신 합니다.");

    // console.log("새로 추가된 것을 갱신합니다.");
    await companySize.bulkWrite(t);

    // console.log("이미 있는 내역을 업데이트 합니다.");
    // await companies.bulkWrite(updateBulk);

    // console.log(`${updateBulk.length} 곳이 갱신되었습니다.`);
    // console.log(`${insertBulk.length} 곳이 추가되었습니다.`);
  } finally {
    console.log(`끝났습니다.`);
    await client.close();
  }
}

run();
