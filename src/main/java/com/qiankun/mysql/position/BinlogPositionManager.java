package com.qiankun.mysql.position;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import javax.sql.DataSource;

import com.qiankun.mysql.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinlogPositionManager {
    private Logger logger = LoggerFactory.getLogger(BinlogPositionManager.class);

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

        if (config.startType.equals("SPECIFIED")) {
            binlogFilename = config.binlogFilename;
            nextPosition = config.nextPosition;

        }

        if (binlogFilename == null || nextPosition == null) {
            throw new Exception("binlogFilename | nextPosition is null.");
        }
    }

    // private void initPositionDefault() throws Exception {
    //
    //     try {
    //         initPositionFromMqTail();
    //     } catch (Exception e) {
    //         logger.error("Init position from mq error.", e);
    //     }
    //
    //     if (binlogFilename == null || nextPosition == null) {
    //         initPositionFromBinlogTail();
    //     }
    //
    // }

    // private void initPositionFromMqTail() throws Exception {
    //     DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("BINLOG_CONSUMER_GROUP");
    //     consumer.setNamesrvAddr(config.mqNamesrvAddr);
    //     consumer.setMessageModel(MessageModel.valueOf("BROADCASTING"));
    //     consumer.start();
    //
    //     Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(config.mqTopic);
    //     MessageQueue queue = queues.iterator().next();
    //
    //     if (queue != null) {
    //         Long offset = consumer.maxOffset(queue);
    //         if (offset > 0)
    //             offset--;
    //
    //         PullResult pullResult = consumer.pull(queue, "*", offset, 100);
    //
    //         if (pullResult.getPullStatus() == PullStatus.FOUND) {
    //             MessageExt msg = pullResult.getMsgFoundList().get(0);
    //             String json = new String(msg.getBody(), "UTF-8");
    //
    //             JSONObject js = JSON.parseObject(json);
    //             binlogFilename = (String) js.get("binlogFilename");
    //             nextPosition = js.getLong("nextPosition");
    //         }
    //     }
    //
    // }

    /**
     * 初始化 binlog的偏移量
     * <img src="https://qiankun98.oss-cn-beijing.aliyuncs.com/%E6%B5%8B%E8%AF%95.png">
     * @throws SQLException
     */
    private void initPositionFromBinlogTail() throws SQLException {
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
