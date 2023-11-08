package com.qiankun.mysql.schemma.column;


import java.io.Serializable;

/**
 * @Description: 列信息
 * @Date : 2023/11/07 15:52
 * @Auther : tiankun
 */
public class Column implements Serializable {


    public int inx;
    // 列名
    public String colName;
    // 类型 比如 bigint
    public String dataType;
    // 数据库类型 比如: bigint(20)
    public String columnType;
    // 数据库
    public String schema;
    // 表
    public String table;

    public Column(String schema, String table, int idx, String colName, String dataType,String columnType) {
        this.schema = schema;
        this.table = table;
        this.colName = colName;
        this.dataType = dataType;
        this.inx = idx;
        this.columnType = columnType;
    }


    public int getInx() {
        return inx;
    }

    public String getColName() {
        return colName;
    }

    public String getDataType() {
        return dataType;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }
}
