package com.qiankun.mysql.schemma;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import com.qiankun.mysql.schemma.column.Column;
import com.qiankun.mysql.schemma.column.ColumnParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据库
 */
public class Database {
    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    /**
     * 查询某个库所有的表信息
     * <img src="https://qiankun98.oss-cn-beijing.aliyuncs.com/Snipaste_2023-11-07_17-18-51.png"/>
     */
    private static final String SQL = "select table_name,column_name,data_type,column_type,character_set_name,table_schema,ordinal_position " +
        "from information_schema.columns " +
        "where table_schema = ?";

    /**
     * 库名
     */
    private String name;

    /**
     * 数据源
     */
    private DataSource dataSource;

    /**
     * 表名信息
     */
    private Map<String, Table> tableMap = new HashMap<String, Table>();

    public Database(String name, DataSource dataSource) {
        this.name = name;
        this.dataSource = dataSource;
    }

    public void init() throws SQLException {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();

            ps = conn.prepareStatement(SQL);
            ps.setString(1, name);
            rs = ps.executeQuery();

            while (rs.next()) {
                String tableName = rs.getString(1);
                String colName = rs.getString(2);
                String dataType = rs.getString(3);
                String colType = rs.getString(4);
                String charset = rs.getString(5);
                String db = rs.getString(6);
                int idx = rs.getInt(7) - 1;
                // 根据mysql的数据类型，匹配数据解析器
                ColumnParser columnParser = ColumnParser.getColumnParser(dataType, colType, charset);
                // 添加表缓存
                if (!tableMap.containsKey(tableName)) {
                    addTable(tableName);
                }
                Table table = tableMap.get(tableName);
                table.addCol(colName);
                table.addColumn(new Column(db,tableName,idx,colName,dataType,colType));
                table.addParser(columnParser);
            }

        } finally {
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (rs != null) {
                rs.close();
            }
        }

    }

    private void addTable(String tableName) {

        LOGGER.info("Schema load -- DATABASE:{},\tTABLE:{}", name, tableName);

        Table table = new Table(name, tableName);
        tableMap.put(tableName, table);
    }

    public Table getTable(String tableName) {

        return tableMap.get(tableName);
    }
}
