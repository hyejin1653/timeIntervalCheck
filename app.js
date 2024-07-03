const express = require("express");
const app = express();

const sql = require("mssql");
const axios = require("axios");
var schedule = require("node-schedule");
const geometry = require("node-geometry-library");
const iniparser = require("iniparser");

const path = require("path");
const execpath = path.dirname(process.execPath);
const fs = require("fs");

var ini = require("ini");

const config = iniparser.parseSync(execpath + "/api_config.ini");
//const config = iniparser.parseSync("./api_config.ini");

var pool1; //DB : bigData Pool
var pool1Connect;

var config1 = {
  user: config.MSSQL.user,
  password: config.MSSQL.password,
  server: config.MSSQL.server,
  database: config.MSSQL.database,
  trustServerCertificate: true,
  requestTimeout: 600000,
};

console.log(config);

pool1 = new sql.ConnectionPool(config1);
pool1Connect = pool1.connect();

/////////////////////////////////////////////////////////구역정보/////////////////////////////////////////////////////////
function getDataArr(data) {
  var dataArr = [];
  for (var r = 0; r < data.length; r++) {
    let lng = parseFloat(data[r][0]);
    let lat = parseFloat(data[r][1]);
    dataArr.push({ lat, lng });
  }
  return dataArr;
}

//관제구역
let areaPoint = getDataArr([
  [126.14916666666667, 36.84583333333334],
  [126.11749999999999, 36.863055555555555],
  [126.16805555555555, 36.95888888888889],
  [126.31138888888889, 37.05888888888889],
  [126.39305555555556, 37.09916666666667],
  [126.44472222222223, 37.1275],
  [126.4535, 37.131166666666665],
  [126.4585, 37.12394444444445],
  [126.46822222222222, 37.108111111111114],
  [126.488, 37.087361111111115],
  [126.49955555555556, 37.078138888888894],
  [126.52516666666666, 37.078138888888894],
  [126.52516666666666, 37.052166666666665],
  [126.35527777777777, 36.88972222222222],
  [126.21611111111112, 36.81083333333333],
  [126.14916666666667, 36.84583333333334],
]);

//항계선
let fixPoint = getDataArr([
  [126.44906388888889, 37.006747222222224],
  [126.452675, 37.014525],
  [126.452675, 37.018966666666664],
  [126.43184166666667, 37.02118888888889],
  [126.42212222222223, 37.024522222222224],
  [126.41128888888889, 37.05146388888888],
  [126.37934722222222, 37.05340833333333],
  [126.30963055555556, 37.031188888888884],
  [126.26574722222222, 37.010355555555556],
  [126.30463333333333, 36.97869444444444],
  [126.33518611111111, 36.97369444444445],
]);
/////////////////////////////////////////////////////////구역정보/////////////////////////////////////////////////////////
//kriso api
async function getDt() {
  let resultData = [];
  let apiIP = config.BASTDATE.apiIP;
  await axios
    .get("http://" + apiIP + "/getESData", {
      data: {
        from: fromDt,
        to: toDt,
        type: "OIS",
      },
    })
    .then((res) => {
      let dataSet = res.data.data;
      if (dataSet == undefined) return resultData;
      //pointData :  관제구역 안의 point data, lineData : 관제구역 선에 걸리는 data
      let pointData = [],
        lineData = [];

      //console.log(dataSet[0]);
      for (let row of dataSet) {
        let lng = parseFloat(row.location.lon);
        let lat = parseFloat(row.location.lat);

        //관제구역안
        if (geometry.PolyUtil.containsLocation({ lat, lng }, areaPoint)) {
          //console.log("1  ", row);
          pointData.push(row);
        }

        //관제구역 선에 걸리는 데이터
        if (geometry.PolyUtil.isLocationOnEdge({ lat, lng }, areaPoint, 10)) {
          //console.log("2  ", row);
          lineData.push(row);
        }
      }

      resultData.push(pointData);
      resultData.push(lineData);
    })
    .catch(function (error) {
      console.log("axios err : " + error);
    });

  return resultData;
}

//RESULT_DATA_ALL_SHIP 저장 구문
function createDBData(dataSet) {
  let flag = true;
  if (dataSet == undefined) return;
  console.log(dataSet.length, "저장");
  //console.log(dataSet);
  //console.log(dataSet.map(item => [item.targetID.toString(), item.LON, item.LAT]));

  let data = dataSet.map((item) => [
    item.MMSI.toString(),
    item.SHIP_TYPE.toString(),
    item.T_DT.toString(),
    item.HOR.toString(),
    item.GUBUN.toString(),
    item.MON.toString(),
  ]);

  const table = new sql.Table("RESULT_DATA_ALL_SHIP");
  table.create = false;
  table.columns.add("MMSI", sql.VarChar(50), { nullable: true });
  table.columns.add("SHIP_TYPE", sql.VarChar(50), { nullable: false });
  table.columns.add("T_DT", sql.VarChar(20), { nullable: false });
  table.columns.add("HOR", sql.VarChar(10), { nullable: false });
  table.columns.add("GUBUN", sql.VarChar(10), { nullable: false });
  table.columns.add("MON", sql.VarChar(20), { nullable: true });

  // add here rows to insert into the table
  for (let row of data) {
    table.rows.add(row[0], row[1], row[2], row[3], row[4], row[5]);
  }

  const request = pool1.request();
  request.bulk(table, (err, result) => {
    if (err) {
      console.log("RESULT_DATA_ALL_SHIP  저장", err);
      return false;
    }
  });

  console.log("RESULT_DATA_ALL_SHIP 성공");
  return flag;
}

//그룹짓기
const groupBy = function (data, key) {
  return data.reduce(function (carry, el) {
    var group = el[key];

    if (carry[group] === undefined) {
      carry[group] = [];
    }

    carry[group].push(el);
    return carry;
  }, {});
};

//날짜 변환 함수
function nowDt(dt, type, mode) {
  let tDate = new Date(dt);

  if (mode == "add") {
    //현재 날짜에서 한시간 플러스
    tDate.setHours(tDate.getHours() + 1);
    tDate.setSeconds(tDate.getSeconds() - 1);
  } else if (mode == "second") {
    tDate.setSeconds(tDate.getSeconds() + 1);
  }

  let date = ("0" + tDate.getDate()).slice(-2);
  let month = ("0" + (tDate.getMonth() + 1)).slice(-2);
  let year = tDate.getFullYear();
  let hours =
    tDate.getHours().toString().length == 1
      ? "0" + tDate.getHours().toString()
      : tDate.getHours();
  let minutes =
    tDate.getMinutes().toString().length == 1
      ? "0" + tDate.getMinutes().toString()
      : tDate.getMinutes();
  let seconds =
    tDate.getSeconds().toString().length == 1
      ? "0" + tDate.getSeconds().toString()
      : tDate.getSeconds();

  let result;

  if (type == "YYYY-MM-DD") {
    result = year + "-" + month + "-" + date;
  } else if (type == "YYYY-MM-DD HH") {
    result = year + "-" + month + "-" + date + " " + hours + ":00:00";
  } else if (type == "MM") {
    result = month;
  } else if (type == "HH") {
    result = hours;
  } else {
    result =
      year +
      "-" +
      month +
      "-" +
      date +
      " " +
      hours +
      ":" +
      minutes +
      ":" +
      seconds;
  }
  return result;
}

////////////////////////////////////////////////////////////////전처리 로직 추가/////////////////////////////////////////////////////////////////////////
//DB에 던질 dataSet만들기
function createDataSet(rData) {
  let resultData = [];
  for (let row of rData) {
    if (
      row.matchFlag == 0 &&
      row.targetID.toString().length == 6 &&
      row.matchID == 0
    ) {
      continue;
    }
    if (row.matchFlag == 1 && row.targetID.toString().length == 6) {
      row["MMSI"] = row.matchID.toString();
    } else {
      row["MMSI"] = row.targetID.toString();
    }

    let tDt = nowDt(row["@timestamp"], "YYYY-MM-DD", "");
    let targetDt = nowDt(row["@timestamp"], "", "");

    row["T_DT"] = tDt;
    row["TARGET_DT"] = targetDt;
    row["LON"] = row.location.lon;
    row["LAT"] = row.location.lat;

    resultData.push(row);
  }

  return resultData;
}

function saveRowData(dataSet, dtTable) {
  console.log(dataSet.length, "saveRowData 저장");
  if (dataSet == undefined || dataSet == null) return false;
  //console.log(dataSet);
  //console.log(dataSet.map(item => [item.targetID.toString(), item.LON, item.LAT]));

  let data = dataSet.map((item) => [
    item.targetID.toString(),
    item.shipName,
    item.IMO,
    item.shipType,
    item.LON,
    item.LAT,
    item.T_DT,
    item.TARGET_DT,
    item.SoG,
    item.CoG,
    item.RoT,
    item.matchID,
    item.MMSI,
  ]);
  let removeData = [...new Set(data.join("|").split("|"))].map((v) =>
    v.split(",")
  );

  const table = new sql.Table(dtTable);
  table.create = false;
  table.columns.add("targetID", sql.VarChar(50), { nullable: true });
  table.columns.add("shipName", sql.VarChar(50), { nullable: true });
  table.columns.add("IMO", sql.VarChar(50), { nullable: true });
  table.columns.add("shipType", sql.VarChar(50), { nullable: true });
  table.columns.add("Lon", sql.VarChar(50), { nullable: true });
  table.columns.add("Lat", sql.VarChar(50), { nullable: true });
  table.columns.add("T_DT", sql.VarChar(50), { nullable: true });
  table.columns.add("TARGET_DT", sql.VarChar(50), { nullable: true });
  table.columns.add("SoG", sql.Decimal(18, 6), { nullable: true });
  table.columns.add("CoG", sql.Decimal(18, 6), { nullable: true });
  table.columns.add("RoT", sql.Decimal(18, 6), { nullable: true });
  table.columns.add("matchID", sql.VarChar(50), { nullable: true });
  table.columns.add("MMSI", sql.VarChar(50), { nullable: true });

  // add here rows to insert into the table
  for (let row of removeData) {
    //console.log(row);
    table.rows.add(
      row[0],
      row[1],
      row[2],
      row[3],
      row[4],
      row[5],
      row[6],
      row[7],
      row[8],
      row[9],
      row[10],
      row[11],
      row[12]
    );
  }

  const request = pool1.request();
  request.bulk(table, (err, result) => {
    console.log("saveRowData error : ", err);
    return false;
  });

  return true;
}

//결과 object
function createResultData(shipType, baseDt, mmsi, flag) {
  let obj = {};
  obj["SHIP_TYPE"] = shipType.length == 1 ? "0" + shipType : shipType;
  obj["T_DT"] = nowDt(baseDt, "YYYY-MM-DD");
  obj["HOR"] = nowDt(baseDt, "HH");
  obj["GUBUN"] = flag;
  obj["MON"] = nowDt(baseDt, "MM");
  obj["MMSI"] = mmsi;
  //resultDt.push(obj);
  return obj;
}

//실동작 로직
function createAvgData(data1, data2, data3) {
  let resultData = data1;
  let lineDataC = data2;

  //결과값 저장하는 배열값
  let resultDt = [];

  console.log("resultData : ", resultData.length);
  console.log("lineData : ", lineDataC.length);

  let containData = []; //항계선 안의 데이터
  let inoutData = []; //입출항 항계선 라인에 걸리는 데이터
  let moveData = [];

  let inCnt = 0; //입항 선박 수
  let outCnt = 0; //출항 선박 수
  let moveCnt = 0; //이동 선박 수
  let passCnt = 0; //통과 선박 수

  for (let row of resultData) {
    let lat = row.LAT,
      lng = row.LON;

    //항계선 걸리는 데이터
    if (geometry.PolyUtil.isLocationOnEdge({ lat, lng }, fixPoint, 100)) {
      if (row.SoG >= 0.5) {
        inoutData.push(row);
      }
    }

    //항계선 안의 데이터
    if (geometry.PolyUtil.containsLocation({ lat, lng }, fixPoint)) {
      containData.push(row);
    }
  }

  console.log("inoutData : ", inoutData.length);
  console.log("containData : ", containData.length);

  let mmsiArr = groupBy(inoutData, "targetID");

  let inoutResult = []; //시간대별로 배열처리 (입출항)
  for (let mmsi of Object.values(mmsiArr)) {
    inoutResult = [];
    //입출항 이동 선박 구하기////////////////////////////////////////////////////////
    let baseDtArr = groupBy(mmsi, "baseDt");

    //console.log(Object.keys(baseDtArr));

    let baseDt = Object.values(baseDtArr);

    let arr = baseDt[0];

    if (baseDt.length == 1) {
      inoutResult.push(arr);
      //console.log(mmsi[0].MMSI, "한개");
    } else {
      //console.log(mmsi[0].MMSI, "여러개");
      for (let i = 0; i < baseDt.length; i++) {
        let j = i + 1;

        //console.log(baseDt.length - 1, j);

        if (baseDt.length > j) {
          let baseF = new Date(baseDt[i][0].baseDt).getTime();
          let baseT = new Date(baseDt[j][0].baseDt).getTime();

          let baseDayF = new Date(baseDt[i][0].T_DT).getTime();
          let baseDayT = new Date(baseDt[j][0].T_DT).getTime();

          let diffTime = (baseT - baseF) / (1000 * 60); //분
          let diffTimeDay = (baseDayT - baseDayF) / (24 * 60 * 60 * 1000);

          // console.log(
          //   baseDt[i][0].baseDt,
          //   baseDt[j][0].baseDt,
          //   diffTime,
          //   diffTimeDay
          // );

          //5분이상 차이나면 새로운 배열에 담는다, 다음 시간값이 있어야 입항인지 출항인지 구분 지을수 있다
          if (diffTime > 1000 && diffTimeDay == 1) {
            //console.log(baseDt[j][0].baseDt);
            //console.log(arr);
            inoutResult.push(arr);

            arr = [];
          } else if (diffTime > 10 && diffTimeDay == 0) {
            //console.log(baseDt[j][0].baseDt);
            inoutResult.push(arr);

            arr = [];
            //arr = arr.concat(baseDt[j]);
          }

          arr = arr.concat(baseDt[j]);
        } else {
          inoutResult.push(arr);
        }
      }
    }

    for (let inout of inoutResult) {
      let inData = [],
        outData = [];

      for (let row of inout) {
        let lat = row.LAT,
          lng = row.LON;
        //console.log(inout.MMSI, inout.baseDt, row.LON, row.LAT);
        if (geometry.PolyUtil.containsLocation({ lat, lng }, fixPoint)) {
          inData.push(new Date(row.TARGET_DT));
        } else {
          outData.push(new Date(row.TARGET_DT));
        }

        //항계선을 통과
        // if (geometry.PolyUtil.isLocationOnEdge({ lat, lng }, fixPoint, 100)) {
        //   linePassflag = false;

        //   continue;
        // }

        // if (linePassflag) {
        //   moveData.push(row);
        // }
      }

      //console.log(mmsi[0].MMSI, inData.length, outData.length);

      let inDT, outDT, inDate, outDate;

      if (inData.length > 0) {
        inDT = Math.min(...inData); //안에 영역의 날짜값
        inDate = new Date(inDT);
      }
      if (outData.length > 0) {
        outDT = Math.min(...outData); //바깥 영역의 날짜값
        outDate = new Date(outDT);
      }
      // if (moveData.length > 0) {
      //   console.log(moveData);
      // }

      //console.log(mmsi[0].MMSI, inDate, nowDt(inDate), outDate, nowDt(outDate));

      if (inDate == undefined) {
        let obj = createResultData(
          mmsi[0].shipType,
          outDate,
          mmsi[0].MMSI,
          "출항"
        );
        resultDt.push(obj);
      } else if (outDate == undefined) {
        let obj = createResultData(
          mmsi[0].shipType,
          inDate,
          mmsi[0].MMSI,
          "입항"
        );
        resultDt.push(obj);
      } else if (inDate < outDate) {
        let obj = createResultData(
          mmsi[0].shipType,
          outDate,
          mmsi[0].MMSI,
          "출항"
        );
        resultDt.push(obj);
        outCnt++; //출항
      } else {
        let obj = createResultData(
          mmsi[0].shipType,
          inDate,
          mmsi[0].MMSI,
          "입항"
        );
        //console.log(obj, inDate);
        resultDt.push(obj);
        inCnt++; //입항
      }

      //console.log(nowDt(inDate), nowDt(outDate));
    }
    //입출항 선박 구하기////////////////////////////////////////////////////////

    //이동 선박 구하기//////////////////////////////////////////////////////////

    //let moveArr = [];

    // for (let row of mmsi) {
    //   let lat = row.LAT,
    //     lng = row.LON;

    //   //항계선을 통과
    //   if (geometry.PolyUtil.isLocationOnEdge({ lat, lng }, fixPoint, 100)) {
    //     linePassflag = false;

    //     continue;
    //   }

    //   if (linePassflag) {
    //     moveResult.push(mmsi);
    //   }
    // }

    //항계선을 통과 하지 않은 선박만 추출

    //정선의 기준을 뽑아야 하므로 0.5 이하의 속도를 가지고 있는 항목 추출
    //console.log(mmsi[0].MMSI);

    //console.log(moveResult);

    //console.log(moveResult.length, mmsi[0].MMSI);

    //정선의 기준을 뽑아야 하므로 0.5 이하의 속도를 가지고 있는 항목 추출
    //for (let row of moveResult) {
    // let dtArr = mmsi
    //   .filter((e) => {
    //     return e.SoG <= 0.5;
    //   })
    //   .map((e) => {
    //     return new Date(e.TARGET_DT);
    //   });

    // //console.log(mmsi[0].MMSI, dtArr);

    // //0.5이하가 2분 이상 지속되야 하므로 시간의 min max 값으로 판단
    // let lineMinDt = Math.min(...dtArr);
    // //let lineMinDt = dtArr[0];

    // //console.log(lineMinDt, mmsi[0].MMSI);

    // //2분 => 1000 * 60 * 2 = 120000
    // let maxDt = lineMinDt + 120000;

    // //console.log(lineMinDt, maxDt);

    // for (let r of mmsi) {
    //   let timeFlag = false;
    //   //console.log(mmsi[0].MMSI, new Date(row.TARGET_DT).getTime(), baseDt);
    //   if (new Date(r.TARGET_DT).getTime() >= maxDt) {
    //     if (r.SoG >= 0.5) {
    //       timeFlag = true;
    //       moveData.push(r);
    //       continue;
    //     }
    //   }

    //   if (timeFlag) {
    //     moveCnt++;

    //     let obj = createResultData(
    //       mmsi[0].shipType,
    //       mmsi[0].TARGET_DT,
    //       mmsi[0].MMSI,
    //       "이동"
    //     );
    //     moveDt.push(obj);
    //     //moveData = moveData.concat(mmsi);
    //   }
    // }
    // }
    //이동 선박 구하기//////////////////////////////////////////////////////////

    //통과선박 구하기///////////////////////////////////////////////////////////
    let sogDt = groupBy(mmsi, "SoG");
    let sogArr = Object.keys(sogDt);

    //0.5이하의 속도가 있는지 확인
    if (
      sogArr.includes("0.5") ||
      sogArr.includes("0.4") ||
      sogArr.includes("0.3") ||
      sogArr.includes("0.2") ||
      sogArr.includes("0.1") ||
      sogArr.includes("0")
    ) {
      continue;
    } else {
      //let dtArr = groupBy(mmsi, "T_DT");

      let lineSData = lineDataC
        .filter((e) => {
          return e.MMSI == mmsi[0].MMSI;
        })
        .map((e) => {
          return new Date(e.TARGET_DT);
        });

      let lineMinDt = Math.min(...lineSData); //통과 선박은 들어오고 나가고가 동시적으로 발생해야 하므로 이렇게 처리
      let lineMaxDt = Math.max(...lineSData); //maxDt - minDt 가 몇시간 정도 차이가 나면 입출항의 뜻이므로!

      let baseF = new Date(lineMinDt).getTime();
      let baseT = new Date(lineMaxDt).getTime();
      let diffTime = (baseT - baseF) / (1000 * 60);

      //min == max 들어갔거나 나왔거나 둘중 하나만 존재 한다 고로 통과 선박이 아님!
      //5분 이상 차이가 날때 다른건으로 체크한다.
      if (diffTime > 5) {
        passCnt++;

        let obj = createResultData(
          mmsi[0].shipType,
          mmsi[0].TARGET_DT,
          mmsi[0].MMSI,
          "통과"
        );
        resultDt.push(obj);
      }
    }
  }
  //통과선박 구하기///////////////////////////////////////////////////////////

  //이동 선박 구하기//////////////////////////////////////////////////////////
  mmsiArr = groupBy(containData, "targetID");
  //let moveArr = [];
  for (let mmsi of Object.values(mmsiArr)) {
    moveData = [];
    let linePassflag = true;

    for (let row of mmsi) {
      let lat = row.LAT,
        lng = row.LON;

      //항계선을 통과
      if (geometry.PolyUtil.isLocationOnEdge({ lat, lng }, fixPoint, 100)) {
        linePassflag = false;
        continue;
      }
    }

    //console.log(mmsi[0].MMSI);

    //항계선을 통과 하지 않은 선박만 추출
    if (linePassflag) {
      //정선의 기준을 뽑아야 하므로 0.5 이하의 속도를 가지고 있는 항목 추출
      let dtArr = mmsi
        .filter((e) => {
          return e.SoG <= 0.5;
        })
        .map((e) => {
          return new Date(e.TARGET_DT);
        });

      let dtDt = mmsi
        .filter((e) => {
          return e.SoG <= 0.5;
        })
        .map((e) => {
          return e;
        });

      //0.5이하가 2분 이상 지속되야 하므로 시간의 min max 값으로 판단
      let lineMinDt = Math.min(...dtArr);

      //2분 => 1000 * 60 * 2 = 120000
      let baseDt = lineMinDt + 120000;

      //console.log(baseDt);

      let timeFlag = false;
      for (let row of dtDt) {
        if (new Date(row.TARGET_DT).getTime() > baseDt) {
          if (row.SoG <= 0.5) {
            timeFlag = true;
            //console.log(row);
            //moveData.push(row);
            continue;
          }
        }
      }

      //console.log(moveData);

      if (timeFlag) {
        moveCnt++;

        let obj = createResultData(
          mmsi[0].shipType,
          mmsi[0].TARGET_DT,
          mmsi[0].MMSI,
          "이동"
        );
        resultDt.push(obj);
        //moveData = moveData.concat(mmsi);
      }

      //console.log("moveData : ", moveData.length);
    }
  }
  //이동 선박 구하기//////////////////////////////////////////////////////////

  //이동선박은 하루치라서 별도로 탄다
  // let moveDt = moveData(data3);
  // moveCnt = moveDt[0];
  // resultDt.push(moveDt[1]);

  console.log(
    "inCnt : ",
    inCnt,
    ", outCnt : ",
    outCnt,
    ", moveCnt : ",
    moveCnt,
    ", passCnt : ",
    passCnt
  );

  return resultDt;
}

//저장로직
async function savePreAvg(data1, data2, data3) {
  console.log("pointData : ", data1.length);
  console.log("lineData : ", data2.length);
  if (data1.length > 0) {
    let avgData = await createAvgData(data1, data2, data3);
    console.log("avgData", avgData.length);

    if (avgData.length == 0) {
      return true;
    } else if (avgData.length > 0) {
      let saveFlag = createDBData(avgData);
      return saveFlag;
      //return true;
    }
  }

  return true;
}

async function selectNewPointData() {
  var sqls = `select MMSI as targetID, shipType, LON, LAT, T_DT, TARGET_DT, SoG, CoG, RoT, MMSI, convert(nvarchar(16), TARGET_DT, 23) as baseDt
              from point_api_data
              where TARGET_DT between '${fromDt}' and '${toDt}'
              group by MMSI, shipType, LON, LAT, T_DT, TARGET_DT, SoG, CoG, RoT, MMSI, convert(nvarchar(16), TARGET_DT, 23) `;
  var request = pool1.request();
  // let valueData = await request.query(sqls, (err, result) => {
  //   if (err) return console.log(err);

  //   let resultData = result.recordsets[0];
  //   let lineData = result.recordsets[1];
  // });
  let valueData = await request.query(sqls);
  //console.log(valueData.recordsets[0]);
  return valueData.recordsets[0];
}

async function selectNewLineData() {
  var sqls = `select MMSI as targetID, shipType, LON, LAT, T_DT, TARGET_DT, SoG, CoG, RoT, MMSI
              from line_api_data
              where TARGET_DT between DateADD(MONTH, -1, '${fromDt}') and DateADD(SECOND, -1, '${toDt}')`;
  var request = pool1.request();
  // let valueData = await request.query(sqls, (err, result) => {
  //   if (err) return console.log(err);

  //   let resultData = result.recordsets[0];
  //   let lineData = result.recordsets[1];
  // });
  let valueData = await request.query(sqls);
  //console.log(valueData.recordsets[0]);
  return valueData.recordsets[0];
}

async function selectMoveData() {
  var sqls = `select MMSI as targetID, shipType, LON, LAT, T_DT, TARGET_DT, SoG, CoG, RoT, MMSI, convert(nvarchar(16), TARGET_DT, 23) as baseDt
              from point_api_data
              where convert(nvarchar(10),TARGET_DT,23) = '${fromDt}'
              group by MMSI, shipType, LON, LAT, T_DT, TARGET_DT, SoG, CoG, RoT, MMSI, convert(nvarchar(16), TARGET_DT, 23) `;
  var request = pool1.request();
  // let valueData = await request.query(sqls, (err, result) => {
  //   if (err) return console.log(err);

  //   let resultData = result.recordsets[0];
  //   let lineData = result.recordsets[1];
  // });
  let valueData = await request.query(sqls);
  //console.log(valueData.recordsets[0]);
  return valueData.recordsets[0];
}
////////////////////////////////////////////////////////////////전처리 로직 추가/////////////////////////////////////////////////////////////////////////

let rData = [],
  pointData = [],
  lineData = [];

//본 로직
const selectData = async () => {
  console.log("start : ", fromDt, toDt);
  rData = [];
  pointData = [];
  lineData = [];

  //let baseDtSelect = await selectBaseDt();
  //console.log("base", baseDtSelect[0]);

  try {
    rData = await getDt();

    console.log("rData : ", rData.length);

    if (rData.length > 0) {
      if (
        (rData[0].length > 0 && rData[0] != null) ||
        (rData[1].length > 0 && rData[1] != null)
      ) {
        console.log(rData[0].length, rData[1].length);
        pointData = await createDataSet(rData[0]);
        lineData = await createDataSet(rData[1]);

        //row data save
        let pointFlag = await saveRowData(pointData, "point_api_data");
        let lineFlag = await saveRowData(lineData, "line_api_data");

        console.log(pointFlag, lineFlag);

        let newPointDt = await selectNewPointData();
        //fromDt-7일전부터 toDt까지 lineData를 가져온다.
        let newLineDt = await selectNewLineData();
        //이동일 경우는 하루의 데이터를 확인해야 한다.(1시간 단위로는 알수가 없음)
        let moveDt = await selectMoveData();

        //DB에 넣고 성공하면 전처리 작업을 하자!
        if (savePreAvg(newPointDt, newLineDt, moveDt)) {
          let baseDtC = nowDt(toDt, "", "second");
          config.BASTDATE.baseDt = baseDtC;
          fs.writeFileSync(execpath + "/api_config.ini", ini.stringify(config));
          //fs.writeFileSync("./api_config.ini", ini.stringify(config));

          fromDt = baseDtC;
          toDt = nowDt(fromDt, "", "add");
        }
      }
    } else {
      console.log(fromDt, toDt, "데이터 없음.");
      // let baseDtC = nowDt(toDt, "", "second");
      // config.BASTDATE.baseDt = baseDtC;
      // fs.writeFileSync("./api_config.ini", ini.stringify(config));
      // //fs.writeFileSync(execpath + "/api_config.ini", ini.stringify(config));

      // fromDt = baseDtC;
      // toDt = nowDt(fromDt, "", "add");
    }
  } catch (e) {
    console.log(e);
  }
};

function scheduleChange() {
  schedule.scheduleJob("00 00 * * * * ", function () {
    console.log(
      "한시간에 한번씩 작동합니다. nowDt : ",
      new Date().toLocaleString()
    );
    //schedule.scheduleJob("00 00 * * * *", function () {
    // if (timeDif > 59) {
    //toDt = dateChange(fromDt);

    //console.log("Interval", fromDt, toDt);
    selectData();
    // } else {
    //   console.log("한시간전 데이터만 동작합니다.");
    // }
  });
}

//2022-03-17 14:00:00
async function mainFunction() {
  let nowDate = nowDt(new Date(), "YYYY-MM-DD", "");
  let baseDt = nowDt(config.BASTDATE.baseDt, "YYYY-MM-DD", "");

  while (true) {
    if (nowDate > baseDt) {
      console.log("while 작동1");
      await selectData();
    } else {
      let nowD = nowDt(new Date(), "", "");
      let timeDif =
        (new Date(nowD).getTime() - new Date(fromDt).getTime()) / (1000 * 60);

      console.log("현재시간 : ", nowD);
      if (timeDif < 59) {
        scheduleChange();
        break;
      } else {
        console.log("while 작동2");
        await selectData();
      }
      //testFun();
    }
  }
}

let fromDt = config.BASTDATE.baseDt;
let toDt = nowDt(fromDt, "", "add");

const port = 6200;
app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);

  mainFunction();
});
