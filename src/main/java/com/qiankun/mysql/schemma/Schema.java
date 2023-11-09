package com.qiankun.mysql.schemma;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Schema {
    private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);

    /**
     * 获取到所有的库名
     * <img src="https://qiankun98.oss-cn-beijing.aliyuncs.com/mysql%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E7%9A%84%E5%BA%93%E5%90%8D.png">
     */
    private static final String SQL = "select schema_name from information_schema.schemata";
    // 忽略的库名
    private static final List<String> IGNORED_DATABASES = new ArrayList<>(
        Arrays.asList(new String[] {"information_schema", "mysql", "performance_schema", "sys"})
    );

    List<String> dbs = Lists.newArrayList();

    private DataSource dataSource;

    private Map<String, Database> dbMap;

    public Schema(DataSource dataSource) {
        this(dataSource,null);
    }

    public Schema(DataSource dataSource,List<String> dbs) {
        this.dataSource = dataSource;
        if(dbs != null && !dbs.isEmpty()) {
            this.dbs.addAll(dbs);
        }
    }

    /**
     * 加载库所有的库信息
     * @throws SQLException
     */
    public void load() throws SQLException {

        dbMap = new HashMap<>();

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();

            ps = conn.prepareStatement(SQL);
            rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                if (!IGNORED_DATABASES.contains(dbName) && ( dbs.isEmpty() || dbs.contains(dbName))) {
                    Database database = new Database(dbName, dataSource);
                    dbMap.put(dbName, database);
                }
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
        // 初始化库源信息
        for (Database db : dbMap.values()) {

            db.init();
        }

    }

    /**
     * 获取表信息
     * @param dbName
     * @param tableName
     * @return
     */
    public Table getTable(String dbName, String tableName) {

        if (dbMap == null) {
            reload();
        }

        Database database = dbMap.get(dbName);
        if (database == null) {
            return null;
        }

        Table table = database.getTable(tableName);
        if (table == null) {
            return null;
        }

        return table;
    }

    /**
     * 重新加载表源信息
     */
    private void reload() {

        while (true) {
            try {
                load();
                break;
            } catch (Exception e) {
                LOGGER.error("Reload schema error.", e);
            }
        }
    }

    public void reset() {
        dbMap = null;
    }
}
