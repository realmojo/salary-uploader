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
const year = 2017;
const month = "05";
const excelFile = xlsx.readFile(`data/${year}${month}.xlsx`);
const sheetName = excelFile.SheetNames[0]; // @details 첫번째 시트 정보 추출
const firstSheet = excelFile.Sheets[sheetName]; // @details 시트의 제목 추출
const jsonData = xlsx.utils.sheet_to_json(firstSheet);

await client.connect();

const db = client.db("salaryinfo");
const companies = db.collection("companies");
const companyBySalary = db.collection("companyBySalary");

let allCount = await db.collection("companies").count();

console.log("데이터를 가공하는 중입니다..");
const newData = jsonData.map((item) => {
  const monthSalary = Number(
    item.pay === 0 ? 0 : ((item.pay / item.total / 9) * 100).toFixed(0)
  );
  const yearSalary = monthSalary * 12;
  return {
    title: item.title ? item.title.trim() : "",
    address: item.address ? item.address.trim() : "",
    roadAddress: item.roadAddress ? item.roadAddress.trim() : "",
    code: item.code ? item.code : "",
    codeName: item.codeName ? item.codeName : "",
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

// const salaryTop100 = async () => {
//   console.log("연봉 순으로 내림차순 정렬 합니다.");
//   newData.sort(function (a, b) {
//     // yearSalary 기준으로 정렬
//     if (a.info[0].yearSalary > b.info[0].yearSalary) {
//       return -1;
//     }
//     if (a.info[0].yearSalary < b.info[0].yearSalary) {
//       return 1;
//     }
//     return 0;
//   });

//   console.log("벌크 정보를 구성합니다.");
//   const insertBulk = newData.map((item, index) => {
//     return {
//       insertOne: {
//         document: {
//           _id: index + 1,
//           title: item.title ? item.title.trim() : "",
//           address: item.address ? item.address.trim() : "",
//           roadAddress: item.roadAddress ? item.roadAddress.trim() : "",
//           code: item.code ? item.code : "",
//           codeName: item.codeName ? item.codeName : "",
//           year,
//           month,
//           totalEmployer: item.info[0].totalEmployer,
//           joinEmployer: item.info[0].joinEmployer,
//           leaveEmployer: item.info[0].leaveEmployer,
//           monthSalary: item.info[0].monthSalary,
//           yearSalary: item.info[0].yearSalary,
//           created: moment().format("YYYY-MM-DD HH:mm:ss"),
//           updated: moment().format("YYYY-MM-DD HH:mm:ss"),
//         },
//       },
//     };
//   });
//   await companyBySalary.bulkWrite(insertBulk);
// };

async function run() {
  try {
    console.log("데이터를 DB에 삽입합니다..");

    let prevData = await companies.find({}).toArray();

    console.log(`이전 데이터 갯수 : ${prevData.length}`);
    console.log(`새  데이터 갯수 : ${newData.length}`);

    // const updateBulk = [];
    // const insertBulk = [];

    let count = 0;

    for (const nextItem of newData) {
      const index = prevData.findIndex((item) => item.title === nextItem.title);
      if (count % 100 === 0) {
        console.log(
          `${count}/${newData.length}번째 데이터 작업중 입니다. ${
            index !== -1 ? "update" : "insert"
          }`
        );
      }
      count += 1;
      if (index !== -1) {
        const prevObject = prevData[index];
        prevObject.info.push(nextItem.info[0]);
        // updateBulk.push({
        //   updateOne: {
        //     filter: {
        //       title: nextItem.title,
        //     },
        //     update: {
        //       $set: {
        //         info: prevObject.info,
        //         updated: moment().format("YYYY-MM-DD HH:mm:ss"),
        //       },
        //     },
        //   },
        // });
        await companies.bulkWrite([
          {
            updateOne: {
              filter: {
                _id: prevObject._id,
              },
              update: {
                $set: {
                  info: prevObject.info,
                  monthSalary: nextItem.monthSalary,
                  yearSalary: nextItem.yearSalary,
                  updated: moment().format("YYYY-MM-DD HH:mm:ss"),
                },
              },
            },
          },
        ]);
        prevData.shift();
      } else {
        let t = { _id: ++allCount, ...nextItem };
        // insertBulk.push({
        //   insertOne: {
        //     document: t,
        //   },
        // });
        await companies.bulkWrite([
          {
            insertOne: {
              document: t,
            },
          },
        ]);
      }
    }

    console.log("벌크로 갱신 합니다.");

    // console.log("새로 추가된 것을 갱신합니다.");
    // await companies.bulkWrite(insertBulk);

    // console.log("이미 있는 내역을 업데이트 합니다.");
    // await companies.bulkWrite(updateBulk);

    // console.log(`${updateBulk.length} 곳이 갱신되었습니다.`);
    // console.log(`${insertBulk.length} 곳이 추가되었습니다.`);
  } finally {
    console.log(`${year}/${month} 끝났습니다.`);
    await client.close();
  }
}

// async function run2() {
//   try {
//     await salaryTop100();
//   } finally {
//     console.log(`${year}/${month} 연봉순으로 적재가 끝났습니다.`);
//     await client.close();
//   }
// }

run();
// run2();
