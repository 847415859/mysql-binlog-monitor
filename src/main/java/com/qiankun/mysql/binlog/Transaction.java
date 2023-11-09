package com.qiankun.mysql.binlog;

import com.qiankun.mysql.Config;
import com.qiankun.mysql.position.BinlogPosition;

import java.util.LinkedList;
import java.util.List;

public class Transaction {
    private BinlogPosition nextBinlogPosition;

    public Transaction() {
    }

    public Transaction(BinlogPosition nextBinlogPosition) {
        this.nextBinlogPosition = nextBinlogPosition;
    }

    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

    public void setNextBinlogPosition(BinlogPosition nextBinlogPosition) {
        this.nextBinlogPosition = nextBinlogPosition;
    }
}