"""Toggles the state of a digital output on an EL1259.

Usage: python basic_example.py <adapter>

This example expects a physical slave layout according to _expected_slave_layout, seen below.
Timeouts are all given in us.
"""

import os
import struct
import time
import threading
from dataclasses import dataclass, field
import typing
import argparse
import numpy as np
from datetime import datetime;
import random
import json
from enum import Enum

import pysoem

from database import *

#
rows = 6;
cols = 0;

# 폴더 생성
UPLOAD_DIR = "D:/IRIS_FTP";
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
createFolder(UPLOAD_DIR);

class State(Enum):
    STOP = 0;
    START = 1;
    DATA_FULL = 2;

# IRIS 정보
IRIS_NAME = "128+128";
IRIS_VENDOR_ID = [0x79a];
IRIS_PRODUCT_CODE = [0xdefed128];

@dataclass
class IrisData:
    fileName: str;
    data: list = field(default_factory=lambda: [[0 for j in range(0)] for i in range(6)]);

    def isFull(self):
        count = 0;
        for i in range(1, 6):
            if len(self.data[i]) != 0:
                count += 1;
        return count == 5;

    def convertData(self):
        data = [];
        for i in range(1, 6):
            data += self.data[i];
        return np.reshape(data, (-1, 3));
    
    def writeFile(self):
        data = self.convertData();
        path = f"{UPLOAD_DIR}/{self.fileName}";
        file = open(path, "w");
        for i in range(0, len(data)):
            file.write(f"{data[i][0]}\t{data[i][1]}\t{data[i][2]}\n");
        file.close();

    def save(self):
        self.writeFile();

@dataclass
class Device:
    name: str;
    vendor_id: int;
    product_code: int;
    config_func: typing.Callable = None;

class Soem:
    def __init__(self, ifname):
        self._ifname = ifname;
        self._pd_thread_stop_event = threading.Event();
        self._ch_thread_stop_event = threading.Event();
        self._actual_wkc = 0;
        self._master = pysoem.Master();
        self._master.in_op = False;
        self._master.do_check_state = False;
        self._expected_slave_layout = {
            0: Device(IRIS_NAME, IRIS_VENDOR_ID[0], IRIS_PRODUCT_CODE[0]),
        };

        self.proc_thread = None;
        self.check_thread = None;
        self.state = State.STOP;
        self.irisData = None;
        self.measure_thread = None;
        self._measure_thread_stop_event = threading.Event();

    def _processdata_thread(self):
        """Background thread that sends and receives the process-data frame in a 10ms interval."""

        slave = self._master.slaves[0];

        while not self._pd_thread_stop_event.is_set():
            self._master.send_processdata();
            self._actual_wkc = self._master.receive_processdata(timeout=100_000);

            code = slave.input[0];
            index = slave.input[1]; # 22 33 기본값

            if self.state == State.START:
                if code == 22:
                    if index <= 5: # 1, 2, 3, 4, 5
                        if len(self.irisData.data[index]) == 0:
                            self.irisData.data[index] = list(slave.input)[2:];
                    elif index == 33:
                        if self.irisData.isFull():
                            self.state = State.DATA_FULL;

            if not self._actual_wkc == self._master.expected_wkc:
                print("incorrect wkc");
            time.sleep(0.01);

    def _check_thread(self):
        while not self._ch_thread_stop_event.is_set():
            if self._master.in_op and ((self._actual_wkc < self._master.expected_wkc) or self._master.do_check_state):
                self._master.do_check_state = False;
                self._master.read_state();
                for i, slave in enumerate(self._master.slaves):
                    if slave.state != pysoem.OP_STATE:
                        self._master.do_check_state = True;
                        Soem._check_slave(slave, i);
                if not self._master.do_check_state:
                    print("OK : all slaves resumed OPERATIONAL.");
            time.sleep(0.01);

    def _measureThread(self, measureId, sn, refs, auto, interval, repeat, tags):
        first = True;

        while not self._measure_thread_stop_event.is_set():
            if self.state == State.STOP:
                if first:
                    first = False;
                else:
                    time.sleep(interval);

                self._writeStart(sn);
            # elif self.state == State.START: # 대기
            elif self.state == State.DATA_FULL:
                filename = self.irisData.fileName;
                self.irisData.save();
                self.irisData = None;
                try:
                    measure_status = self._insertMeasureScore(measureId, sn, refs, tags, filename);
                except Exception as error:
                    print(error);
                    break;

                self._writeStop();

                if measure_status == False:
                    break;

            time.sleep(0.001);

        self._writeStop();
        print("_measureThread exit.");

        self._stopMeasure(sn);
        self.measure_thread = None;
        self._measure_thread_stop_event.clear();

    def _insertMeasureScore(self, measureId, sn, refs, tags, filename):
        connection, cursor = createConnection();

        try:
            # select count
            query = f"select measure_repeat, measure_count from device where sn='{sn}'";
            rows = fetchall(cursor, query);
            if len(rows) == 0:
                raise Exception("해당 디바이스를 찾을 수 없습니다.");
            row = rows[0];
            measure_repeat = row["measure_repeat"];
            measure_count = row["measure_count"];

            # socre insert
            score = [];
            for ref in refs:
                id = ref["id"];
                eq = ref["eq"];

                values = [];
                for e in eq:
                    values.append({"eq": e, "value": random.randint(1, 100)}); # random score value

                score.append({ "id": id, "values": values });

            query = f"insert into measure_data(measure_id, sn, timestamp, filename, score, tags) values('{measureId}', {sn}, '{sqlTimestampNow()}', '{filename}', '{json.dumps(score)}', '{json.dumps(tags)}')";
            cursor.execute(query);

            # update count
            next = measure_count + 1;
            query = f"update device set measure_count='{next}' where sn={sn}";
            cursor.execute(query);

            if next == measure_repeat:
                return False;

            return True;
        except Exception as error:
            raise error;
        finally:
            if connection:
                connection.close();

    def _stopMeasure(self, sn):
        connection, cursor = createConnection();

        try:
            query = f"update device set measure_status='0' where sn='{sn}'";
            cursor.execute(query);
        except Exception as error:
            raise error;
        finally:
            if connection:
                connection.close();

    def run(self):
        self._master.open(self._ifname);

        if not self._master.config_init() > 0:
            self._master.close();
            raise SoemError("no slave found");

        for i, slave in enumerate(self._master.slaves):
            if not ((slave.man == self._expected_slave_layout[i].vendor_id) and (slave.id == self._expected_slave_layout[i].product_code)):
                self._master.close();
                raise SoemError("unexpected slave layout");
            slave.config_func = self._expected_slave_layout[i].config_func;
            slave.is_lost = False;

        self._master.config_map();

        if self._master.state_check(pysoem.SAFEOP_STATE, timeout=50_000) != pysoem.SAFEOP_STATE:
            self._master.close();
            raise SoemError("not all slaves reached SAFEOP state");

        self._master.state = pysoem.OP_STATE;

        self.check_thread = threading.Thread(target=self._check_thread, daemon=True);
        self.check_thread.start();
        self.proc_thread = threading.Thread(target=self._processdata_thread, daemon=True);
        self.proc_thread.start();

        # send one valid process data to make outputs in slaves happy
        self._master.send_processdata();
        self._master.receive_processdata(timeout=2000);
        # request OP state for all slaves

        self._master.write_state();

        self.all_slaves_reached_op_state = False;
        for i in range(40):
            self._master.state_check(pysoem.OP_STATE, timeout=50_000);
            if self._master.state == pysoem.OP_STATE:
                self.all_slaves_reached_op_state = True;
                break;

    def startMeasure(self, measureId, sn, refs, auto, interval, repeat, tags):
        if not self.measure_thread:
            self.measure_thread = threading.Thread(target=self._measureThread, args=(measureId, sn, refs, auto, interval, repeat, tags,), daemon=True);
            self.measure_thread.start();

    def stopMeasure(self, sn):
        if self.measure_thread:
            self._measure_thread_stop_event.set();

    def _writeStart(self, sn):
        if self.state == State.STOP and self.proc_thread and self.check_thread:
            self.state = State.START;
            dt = datetime.now().strftime("%Y%m%d%H%M%S");
            filename = f"{sn}_{dt}.txt";
            self.irisData = IrisData(filename);
            self._master.in_op = True;
            output_len = len(self._master.slaves[0].output);
            write = bytearray([0 for i in range(output_len)]);
            write[1] = 11;
            self._master.slaves[0].output = bytes(write);

    def _writeStop(self):
        if self.state != State.STOP and self.proc_thread and self.check_thread:
            self.state = State.STOP;
            self._master.in_op = True;
            output_len = len(self._master.slaves[0].output);
            write = bytearray([0 for i in range(output_len)]);
            self._master.slaves[0].output = bytes(write);

    def exit(self):
        if self.proc_thread:
            self._pd_thread_stop_event.set();
            self.proc_thread.join();
        
        if self.check_thread:
            self._ch_thread_stop_event.set();
            self.check_thread.join();

        self._master.state = pysoem.INIT_STATE;
        # request INIT state for all slaves
        self._master.write_state();
        self._master.close();

        if not self.all_slaves_reached_op_state:
            raise SoemError("not all slaves reached OP state");

    @staticmethod
    def _check_slave(slave, pos):
        if slave.state == (pysoem.SAFEOP_STATE + pysoem.STATE_ERROR):
            print(f"ERROR : slave {pos} is in SAFE_OP + ERROR, attempting ack.")
            slave.state = pysoem.SAFEOP_STATE + pysoem.STATE_ACK;
            slave.write_state();
        elif slave.state == pysoem.SAFEOP_STATE:
            print(f"WARNING : slave {pos} is in SAFE_OP, try change to OPERATIONAL.");
            slave.state = pysoem.OP_STATE;
            slave.write_state();
        elif slave.state > pysoem.NONE_STATE:
            if slave.reconfig():
                slave.is_lost = False;
                print(f"MESSAGE : slave {pos} reconfigured");
        elif not slave.is_lost:
            slave.state_check(pysoem.OP_STATE);
            if slave.state == pysoem.NONE_STATE:
                slave.is_lost = True;
                print(f"ERROR : slave {pos} lost");
        if slave.is_lost:
            if slave.state == pysoem.NONE_STATE:
                if slave.recover():
                    slave.is_lost = False;
                    print(f"MESSAGE : slave {pos} recovered");
            else:
                slave.is_lost = False;
                print(f"MESSAGE : slave {pos} found")

class SoemError(Exception):
    def __init__(self, message):
        super().__init__(message);
        self.message = message;

# instance
soemInstance = Soem("\\Device\\NPF_{7F9CCEDA-2998-45C6-B47E-614E03DCF170}");