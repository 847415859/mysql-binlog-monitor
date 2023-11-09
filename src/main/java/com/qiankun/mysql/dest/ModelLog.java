package com.qiankun.mysql.dest;

import com.google.common.collect.Maps;
import com.qiankun.mysql.binlog.DataImageRow;
import com.qiankun.mysql.binlog.RowEventType;

import java.io.Serializable;
import java.util.*;

/**
 * @Description: 存储解析后的数据
 * @Date : 2023/11/08 10:56
 * @Auther : tiankun
 */
public class ModelLog {

    /**
     * 库名
     */
    String database;

    /**
     * 表名
     */
    String table;


    /**
     * 属性id
     * 格式： model:id
     */
    String modelId;

    /**
     * PO
     */
    String model;

    /**
     * 数据变更类型
     * @see com.qiankun.mysql.binlog.RowEventType
     */
    String type;

    /**
     * before image
     * 删除和修改操作有
     */
    Map<String, Object> before = Maps.newHashMap();
    /**
     * after image
     * 新增和修改操作有
     */
    Map<String, Object> after = Maps.newHashMap();


    /**
     * 数据变更时间戳
     */
    long changeTimestamp = System.currentTimeMillis();

    public ModelLog() {
    }

    public ModelLog(String database, String table, String modelId, String model, String type, Map<String, Object> before, Map<String, Object> after,long changeTimestamp) {
        this.database = database;
        this.table = table;
        this.modelId = modelId;
        this.model = model;
        this.type = type;
        this.before = before;
        this.after = after;
        this.changeTimestamp = changeTimestamp;
    }

    public RowEventType getRowEventType(){
        return  type == null ? null : RowEventType.valueOf(type);
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
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

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public void setChangeTimestamp(long changeTimestamp) {
        this.changeTimestamp = changeTimestamp;
    }


    @Override
    public String toString() {
        return "ModelLog{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", modelId='" + modelId + '\'' +
                ", model='" + model + '\'' +
                ", type='" + type + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", changeTimestamp=" + changeTimestamp +
                '}';
    }
}
