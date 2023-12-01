from fastapi import Request, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import os
import json
from datetime import datetime

from database import *
from soem import *

app = FastAPI();
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
);

@app.on_event("startup")
def startup_event():
    print("startup FastAPI");
    try:
        soemInstance.run();
    except SoemError as error:
        print(f"{os.path.basename(__file__)} failed: {error.message}");

@app.on_event("shutdown")
def shutdown_event():
    print("shutdown FastAPI");
    soemInstance.exit();

class SearchModel(BaseModel):
    sn: str;
    id: str;
    startTimestamp: str;
    endTimestamp: str;
    startScore: float;
    endScore: float;
    tags: str;

def makeResponse(success="OK", message="", data=None):
    return {"success": success, "message": message, "data": data};

def dateToTimestamp(dateString):
    return datetime.strptime(dateString, "%Y-%m-%d %H:%M:%S").timestamp();

def getMeasureId(sn):
    dt = datetime.now().strftime("%Y%m%d%H%M%S");
    return f"{sn}_{dt}";

async def isStartTask(sn):
    connection, cursor = createConnection();

    try:
        query = f"select measure_status from device where sn={sn}";
        rows = fetchall(cursor, query);

        if len(rows) == 1:
            if rows[0]["measure_status"] == 1:
                return True;
            else:
                return False;
        else:
            raise Exception(f"디바이스 '{sn}' 을 찾을 수 없습니다.");
    except Exception as error:
        raise error;
    finally:
        if connection:
            connection.close();

####################################################################################################
# API
####################################################################################################

@app.get(path="/", description="루트")
def root():
    return "Hello AETHER IRIS Server.";

@app.get(path="/test", description="테스트")
def test():
    connection, cursor = createConnection();

    try:
        query = "select * from device";
        rows = fetchall(cursor, query);

        return makeResponse("OK", "", rows);
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/device")
def device():
    connection, cursor = createConnection();

    try:
        query = "select * from device";
        rows = fetchall(cursor, query);

        return makeResponse("OK", "", rows);
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/device-measure")
def deviceMeasure():
    connection, cursor = createConnection();

    try:
        query = "select sn, measure_status, measure_id, measure_refs, measure_auto, measure_interval, measure_repeat, measure_tags, measure_count from device";
        rows = fetchall(cursor, query);

        # [{"id":11206,"values":[{"eq":"EQ1","value":92.3}]}] = db(id, eq, value)
        return makeResponse("OK", "", rows);
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/operation")
def operation():
    connection, cursor = createConnection();

    try:
        query = "select sn, score from measure_data";
        rows = fetchall(cursor, query);

        result = {};
        for row in rows:
            sn = row["sn"];
            score = row["score"];

            parse = json.loads(score);
            
            onlyScore = [];
            for score in parse:
                values = score["values"];
                for value in values:
                    onlyScore.append(value["value"]);

                if result.get(sn):
                    result[sn].append(onlyScore);
                else:
                    result[sn] = [onlyScore];

        maxLength = 0;
        for e in result.values():
            length = len(e);
            if len(e) > maxLength:
                maxLength = length;
        
        header = [];
        trows = [[] for i in range(maxLength)];
        for key, value in result.items():
            header.append(key);

            for i in range(maxLength):
                text = "";
                if len(value) > i:
                    if len(value[i]) == 0:
                        text = "-";
                    else:
                        text = json.dumps(value[i]);
                trows[i].append(text);

        return makeResponse("OK", "", { "header": header, "rows": trows });
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/search")
def search(searchModel: SearchModel):
    connection, cursor = createConnection();

    try:
        sn = searchModel.sn;
        id = searchModel.id;
        startTimestamp = searchModel.startTimestamp;
        endTimestamp = searchModel.endTimestamp;
        startScore = searchModel.startScore;
        endScore = searchModel.endScore;
        tags = searchModel.tags;

        if startScore > endScore:
            return makeResponse("FAIL", "시작 점수가 끝 점수 보다 큽니다.");

        queryList = ["select * from measure_data"];

        if sn:
            queryList.append(f"sn like '%{sn}%'");

        if id:
            queryList.append(f"id like '%{id}%'");

        if startTimestamp and endTimestamp:
            queryList.append(f"unix_timestamp(timestamp) >= {dateToTimestamp(startTimestamp)} and unix_timestamp(timestamp) <= {dateToTimestamp(endTimestamp)}");

        query = "";
        for i in range(len(queryList)):
            if i == 1:
                query += " where ";
            elif i > 1:
                query += " and ";
            query += queryList[i];

        rows = fetchall(cursor, query);

        # time format 변환
        for row in rows:
            row["timestamp"] = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S");

        # score
        dataScore = [];
        for row in rows:
            parse = json.loads(row["score"]);

            onlyScore = [];
            for e in parse:
                for v in e["values"]:
                    onlyScore.append(v["value"]);

            if len(onlyScore) != 0 and (min(onlyScore) < startScore or max(onlyScore) > endScore):
                continue;

            dataScore.append(row);

        # tags
        data = [];
        tagsClient = json.loads(tags); # ['1000', '2000']
        if len(tagsClient) == 0:
            data = dataScore;
        else:
            for e in dataScore:
                if not e["tags"]:
                    continue;

                tagsServer = json.loads(e["tags"]); # ["1000,2000"]
                if len(tagsServer) == 0:
                    continue;
                tagsServer = tagsServer[0].split(",");

                # 중복 제거
                includes = True;
                for t in tagsClient:
                    if tagsServer.__contains__ (t) == False:
                        includes = False;
                        break;

                if includes:
                    data.append(e);

        return makeResponse("OK", "", data);
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/sign-in")
async def signIn(request: Request):
    body = await request.json();
    password = body["password"];

    connection, cursor = createConnection();

    try:
        query1 = "select * from account where name='master'";
        rows = fetchall(cursor, query1);

        if len(rows) == 0:
            return makeResponse("FAIL", "Master not found.");

        row = rows[0];
        if password != row["password"]:
            return makeResponse("FAIL", "Password is incorrect.");

        query2 = f"update account set timestamp='{sqlTimestampNow()}', signin='1' where name='master'";
        cursor.execute(query2);

        return makeResponse("OK");
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/sign-out")
def signOut():
    connection, cursor = createConnection();

    try:
        query = "update account set signin='0' where name='master'";
        cursor.execute(query);

        return makeResponse("OK");
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/sign-available")
def signAvailable():
    connection, cursor = createConnection();

    try:
        query = "select * from account where name='master'";
        rows = fetchall(cursor, query);

        if len(rows) == 0:
            return makeResponse("FAIL", "Master not found.");

        row = rows[0];

        startTime = row["timestamp"];
        elapsedTime = datetime.now().timestamp() - startTime.timestamp();

        return makeResponse("OK", "", { "signin": row["signin"] == 1, "elapsed": elapsedTime > 600000 } ); # 10분
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/measure-start")
async def measureStart(request: Request):
    body = await request.json();
    sn = body["sn"];
    refs = body["refs"];
    auto = body["auto"];
    measure_auto = 1 if auto else 2;
    interval = body["interval"];
    repeat = body["repeat"];
    tags = body["tags"];

    connection, cursor = createConnection();

    try:
        if await isStartTask(sn):
            return makeResponse("FAIL", f"{sn} 는 이미 실행 중 입니다.");

        measureId = getMeasureId(sn);
        query = f"update device set measure_status='1', measure_id='{measureId}', measure_refs='{json.dumps(refs)}', measure_auto='{measure_auto}', measure_interval='{interval}', measure_repeat='{repeat}', measure_tags='{json.dumps(tags)}', measure_count='{0}' where sn='{sn}'";
        cursor.execute(query);

        soemInstance.startMeasure(measureId, sn, refs, auto, interval, repeat, tags);

        return makeResponse("OK", "", {"sn": sn, "measureId": measureId});
    except Exception as error:
        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();

@app.post("/measure-stop")
async def measureStop(request: Request):
    try:
        body = await request.json();
        sn = body["sn"];

        if await isStartTask(sn) == False:
            return makeResponse("OK", f"{sn} 는 이미 정지 상태 입니다.");

        soemInstance.stopMeasure(sn);

        return makeResponse("OK", f"{sn} 정지 하였습니다.");
    except Exception as error:
        return makeResponse("FAIL", repr(error));

@app.post("/measure-score")
async def measureScore(request: Request):
    body = await request.json();
    sn = body["sn"];
    measureId = body["measureId"];

    connection, cursor = createConnection();

    try:
        message = "";
        if await isStartTask(sn) == False:
            message = f"{sn} completed";

        query = f"select timestamp, score from measure_data where measure_id='{measureId}'";
        rows = fetchall(cursor, query);

        return makeResponse("OK", message, rows);
    except Exception as error:

        return makeResponse("FAIL", repr(error));
    finally:
        if connection:
            connection.close();