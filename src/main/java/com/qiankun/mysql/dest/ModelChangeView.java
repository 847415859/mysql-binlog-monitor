package com.qiankun.mysql.dest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description:
 * @Date : 2023/11/08 19:32
 * @Auther : tiankun
 */
public class ModelChangeView{
    /**
     * 库名
     */
    String database;

    /**
     * 表名
     */
    String table;

    /**
     * 时间
     */
    String date;

    /**
     * 列改变
     * key: 属性名
     * value: before
     *        after
     */
    Map<String,Pair> changeColumnMap = new LinkedHashMap<>();

    public ModelChangeView() {
    }

    public ModelChangeView(String database, String table, String date, Map<String, Pair> changeColumnMap) {
        this.database = database;
        this.table = table;
        this.date = date;
        this.changeColumnMap = changeColumnMap;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Map<String, Pair> getChangeColumnMap() {
        return changeColumnMap;
    }

    public void setChangeColumnMap(Map<String, Pair> changeColumnMap) {
        this.changeColumnMap = changeColumnMap;
    }
}
