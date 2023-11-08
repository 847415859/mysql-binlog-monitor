package com.qiankun.mysql.binlog;

import com.alibaba.fastjson2.JSONObject;
import com.qiankun.mysql.Config;
import com.qiankun.mysql.position.BinlogPosition;
import com.qiankun.mysql.schemma.Table;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Transaction {
    private BinlogPosition nextBinlogPosition;
    private Long xid;

    private Config config;

    private List<DataImageRow> list = new LinkedList<>();

    public Transaction(Config config) {
        this.config = config;
    }

    // /**
    //  * 添加变更行记录
    //  * @param type
    //  * @param table
    //  * @param row
    //  * @return
    //  */
    // public boolean addRow(String type, Table table, List<Map.Entry<Serializable[],Serializable[]>> list) {
    //     // 到达阈值，则需要提交，不在添加进去
    //     if (list.size() == config.maxTransactionRows) {
    //         return false;
    //     } else {
    //
    //         DataImageRow dataRow = new DataImageRow(type, table, row);
    //         list.add(dataRow);
    //         return true;
    //     }
    // }
    //
    //
    // /**
    //  * 添加变更行记录
    //  * @param type
    //  * @param table
    //  * @param row
    //  * @return
    //  */
    // public boolean addRow(String type, Table table, List<Serializable[]> list) {
    //     // 到达阈值，则需要提交，不在添加进去
    //     if (list.size() == config.maxTransactionRows) {
    //         return false;
    //     } else {
    //         DataRow dataRow = new DataRow(type, table, row);
    //         list.add(dataRow);
    //         return true;
    //     }
    // }

    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

    public void setNextBinlogPosition(BinlogPosition nextBinlogPosition) {
        this.nextBinlogPosition = nextBinlogPosition;
    }

    public void setXid(Long xid) {
        this.xid = xid;
    }

    public Long getXid() {
        return xid;
    }
}