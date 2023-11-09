package com.qiankun.mysql.schemma;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.qiankun.mysql.schemma.column.Column;
import com.qiankun.mysql.schemma.column.ColumnParser;

/**
 * 表结构信息
 */
public class Table  {

    // 数据库
    private String database;
    // 表名
    private String name;
    // 列名
    private List<String> colList = new LinkedList<String>();
    // 列信息
    private List<Column> columnList = new LinkedList<>();
    // 列解析
    private List<ColumnParser> parserList = new LinkedList<ColumnParser>();

    public Table(String database, String table) {
        this.database = database;
        this.name = table;
    }

    public void addCol(String column) {
        colList.add(column);
    }

    public void addColumn(Column column){
        columnList.add(column);
    }

    public void addParser(ColumnParser columnParser) {
        parserList.add(columnParser);
    }

    public List<String> getColList() {
        return colList;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public List<ColumnParser> getParserList() {
        return parserList;
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    @Override
    public String toString() {
        return "Table{" +
                "database='" + database + '\'' +
                ", name='" + name + '\'' +
                ", colList=" + colList +
                ", columnList=" + columnList +
                ", parserList=" + parserList +
                '}';
    }
}