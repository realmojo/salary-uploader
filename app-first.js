import { MongoClient } from "mongodb";
import moment from "moment";
import xlsx from "xlsx";

console.log("시작합니다.");
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);

console.log("데이터를 읽는 중입니다..");
const excelFile = xlsx.readFile("data/201511.xlsx");
const sheetName = excelFile.SheetNames[0]; // @details 첫번째 시트 정보 추출
const firstSheet = excelFile.Sheets[sheetName]; // @details 시트의 제목 추출
const jsonData = xlsx.utils.sheet_to_json(firstSheet);

console.log("데이터를 가공하는 중입니다..");
const mapData = jsonData.map((item, index) => {
  return {
    insertOne: {
      document: {
        _id: index + 1,
        title: item.title ? item.title.trim() : "",
        address: item.address ? item.address.trim() : "",
        roadAddress: item.roadAddress ? item.roadAddress.trim() : "",
        code: item.code ? item.code : "",
        codeName: item.codeName ? item.codeName : "",
        info: [
          {
            year: 2015,
            month: "11",
            totalEmployer: item.total,
            joinEmployer: item.join,
            leaveEmployer: item.leave,
            monthSalary: Number(
              (item.pay === 0 ? 0 : (item.pay / item.total / 9) * 100).toFixed(
                0
              )
            ),
            yearSalary: Number(
              (item.pay === 0
                ? 0
                : (item.pay / item.total / 9) * 100 * 12
              ).toFixed(0)
            ),
          },
        ],
        created: moment().format("YYYY-MM-DD HH:mm:ss"),
        updated: moment().format("YYYY-MM-DD HH:mm:ss"),
      },
    },
  };
});

async function run() {
  try {
    console.log("데이터를 DB에 삽입합니다..");
    await client.connect();

    const db = client.db("salaryinfo");
    const companies = db.collection("companies");

    await companies.bulkWrite(mapData);
  } finally {
    console.log("끝났습니다.");
    await client.close();
  }
}

run();
