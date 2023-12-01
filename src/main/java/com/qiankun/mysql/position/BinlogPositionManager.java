package com.qiankun.mysql.position;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import javax.sql.DataSource;

import com.qiankun.mysql.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogPositionManager {
    private Logger LOGGER = LoggerFactory.getLogger(BinlogPositionManager.class);

    private DataSource dataSource;
    private Config config;
    // binlog 文件名
    private String binlogFilename;
    // 偏移量
    private Long nextPosition;

    public BinlogPositionManager(Config config, DataSource dataSource) {
        this.config = config;
        this.dataSource = dataSource;
    }

    public void initBeginPosition() throws Exception {
        // 初始化 binlog 的偏移量
        initPositionFromBinlogTail();
        if ("SPECIFIED".equals(config.startType)) {
            if(StringUtils.isNotBlank(config.binlogFilename)) {
                binlogFilename = config.binlogFilename;
            }
            if(config.nextPosition != null) {
                nextPosition = config.nextPosition;
            }
        }

        if (binlogFilename == null || nextPosition == null) {
            throw new Exception("binlogFilename | nextPosition is null.");
        }
    }

    /**
     * 初始化 binlog的偏移量
     * <img src="https://qiankun98.oss-cn-beijing.aliyuncs.com/%E6%B5%8B%E8%AF%95.png">
     * @throws SQLException
     */
    private void initPositionFromBinlogTail() throws SQLException {

        // try {
        //     RandomAccessFile raf  = new RandomAccessFile(new File("./position"),"rw");
        //     short binlogNameLength = raf.readShort();
        //     byte[] filenameBytes = new byte[binlogNameLength];
        //     raf.read(filenameBytes,1,binlogNameLength);
        //     long position = raf.readLong();
        //     System.out.println("读取到的binlog文件名：" + new String(filenameBytes));
        //     System.out.println("读取到的binlog.position：" + position);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }

        String sql = "SHOW MASTER STATUS";
        Connection conn = null;
        ResultSet rs = null;
        try {
            // 获取数据库链接
            Connection connection = dataSource.getConnection();
            // 执行sql
            rs = connection.createStatement().executeQuery(sql);
            while (rs.next()) {
                // binlog 文件
                binlogFilename = rs.getString("File");
                // 当前偏移量
                nextPosition = rs.getLong("Position");
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (rs != null) {
                rs.close();
            }
        }

    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public Long getPosition() {
        return nextPosition;
    }
}
