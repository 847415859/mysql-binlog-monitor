package com.qiankun.mysql.binlog;

import com.google.common.collect.Maps;
import com.qiankun.mysql.schemma.Table;
import com.qiankun.mysql.schemma.column.ColumnParser;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Date : 2023/11/08 11:34
 * @Auther : tiankun
 */
public class DataImageRow{

    /**
     * id
     */
    private Object id;

    /**
     * 数据库
     */
    private String database;

    /**
     * 数据表
     */
    private Table table;

    /**
     * 输出操作类型
     * @see RowEventType
     */
    private String type;

    /**
     * 数据事件类型
     * @see com.github.shyiko.mysql.binlog.event.EventType
     */
    private String mysqlType;


    /**
     * before image
     * 删除和修改操作有
     */
    Map<String,Object> before = Maps.newHashMap();

    /**
     * after image
     * 新增和修改操作有
     */
    Map<String,Object> after = Maps.newHashMap();


    /**
     * 数据变更时间戳
     */
    long changeTimestamp = System.currentTimeMillis();

    long nextPosition;

    public DataImageRow() {
    }

    public DataImageRow(String database, Table table, String type, String mysqlType) {

        this.database = database;
        this.table = table;
        this.type = type;
        this.mysqlType = mysqlType;
    }

    public  String getCamelTableName(){
        if(table == null || StringUtils.isBlank(table.getName())){
            return null;
        }
        String[] words = table.getName().split("_");
        StringBuilder result = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            result.append(words[i].substring(0, 1).toUpperCase()).append(words[i].substring(1));
        }
        return result.toString();
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }


    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Long getChangeTimestamp() {
        return changeTimestamp;
    }

    public void setChangeTimestamp(long changeTimestamp) {
        this.changeTimestamp = changeTimestamp;
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    @Override
    public String toString() {
        return "DataImageRow{" +
                "id=" + id +
                ", database='" + database + '\'' +
                ", table=" + table +
                ", type='" + type + '\'' +
                ", mysqlType='" + mysqlType + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", changeTimestamp=" + changeTimestamp +
                ", nextPosition=" + nextPosition +
                '}';
    }
}
