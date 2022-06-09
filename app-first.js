/*
  year, month 카운트 맞추기
  excelFile 이전달꺼 보고 서식 맞추기
*/

import { MongoClient } from "mongodb";
import moment from "moment";
import xlsx from "xlsx";

console.log("시작합니다.");
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);

console.log("데이터를 읽는 중입니다..");
const year = 2022;
const month = "04";
const excelFile = xlsx.readFile(`data/${year}${month}.xlsx`);
const sheetName = excelFile.SheetNames[0]; // @details 첫번째 시트 정보 추출
const firstSheet = excelFile.Sheets[sheetName]; // @details 시트의 제목 추출
const jsonData = xlsx.utils.sheet_to_json(firstSheet);

await client.connect();

const db = client.db("salaryinfo");
const companies = db.collection("companies");

let allCount = await db.collection("companies").count();

console.log("데이터를 가공하는 중입니다..");
const newData = jsonData.map((item, index) => {
  const monthSalary = Number(
    item.pay === 0 ? 0 : ((item.pay / item.total / 9) * 100).toFixed(0)
  );
  const yearSalary = monthSalary * 12;
  return {
    title: item.title ? item.title.toString().trim() : "",
    address: item.address ? item.address.trim() : "",
    roadAddress: item.roadAddress ? item.roadAddress.trim() : "",
    businessNo: item.businessNo ? Number(item.businessNo) : "",
    code: item.code ? Number(item.code) : "",
    codeName: item.codeName ? item.codeName : "",
    totalEmployer: item.total,
    monthSalary,
    yearSalary,
    info: [
      {
        year,
        month,
        totalEmployer: item.total,
        joinEmployer: item.join,
        leaveEmployer: item.leave,
        monthSalary,
        yearSalary,
      },
    ],
    created: moment().format("YYYY-MM-DD HH:mm:ss"),
    updated: moment().format("YYYY-MM-DD HH:mm:ss"),
  };
});

async function run() {
  try {
    console.log("데이터를 DB에 삽입합니다..");
    console.log(`총 개수는 ${newData.length} 입니다.`);
    let count = 0;
    let index = 0;
    for (const nextItem of newData) {
      if (count % 100 === 0) {
        console.log(`${count}/${newData.length} 데이터 작업 중 입니다.`);
      }
      count++;
      if (nextItem.title.length > 20) {
        // console.log(`${nextItem.title} 사업자 명이 길어서 넘겨버립니다.`);
        continue;
      }
      await companies.bulkWrite([
        {
          insertOne: {
            document: { _id: ++index, ...nextItem },
          },
        },
      ]);
    }

    console.log("벌크로 갱신 합니다.");
  } finally {
    console.log(`${year}/${month} 끝났습니다.`);
    await client.close();
  }
}

run();
