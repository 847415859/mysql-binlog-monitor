package com.qiankun.mysql;

import com.qiankun.mysql.binlog.EventProcessor;
import com.qiankun.mysql.binlog.Transaction;
import com.qiankun.mysql.position.BinlogPosition;
import com.qiankun.mysql.position.BinlogPositionLogThread;
import com.qiankun.mysql.schemma.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Replicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private static final Logger POSITION_LOGGER = LoggerFactory.getLogger("PositionLogger");

    private Config config;

    private Schema schema;

    private EventProcessor eventProcessor;


    private final Object lock = new Object();
    private BinlogPosition nextBinlogPosition;
    private long nextQueueOffset;
    private long xid;

    public static void main(String[] args) {
        Replicator replicator = new Replicator();
        replicator.start();
    }

    public void start() {

        try {
            config = new Config();
            config.load();

            // 开启一个线程记录binlog文件
            BinlogPositionLogThread binlogPositionLogThread = new BinlogPositionLogThread(this);
            binlogPositionLogThread.start();

            eventProcessor = new EventProcessor(this);
            eventProcessor.start();

        } catch (Exception e) {
            LOGGER.error("Start error.", e);
            System.exit(1);
        }
    }

    /**
     * 提交数据
     * @param transaction
     * @param isComplete
     */
    public void commit(Transaction transaction, boolean isComplete) {
        // String json = transaction.toJson();
        // LOGGER.info("commit json :{}",json);
        for (int i = 0; i < 3; i++) {
            try {
                if (isComplete) {
                    synchronized (lock) {
                        xid = transaction.getXid();
                        nextBinlogPosition = transaction.getNextBinlogPosition();
                    }

                } else {

                }
                break;

            } catch (Exception e) {
                LOGGER.error("Push error,retry:" + (i + 1) + ",", e);
            }
        }
    }

    /**
     * 记录偏移量
     */
    public void logPosition() {
        String binlogFilename = null;
        long xid = 0L;
        long nextPosition = 0L;
        long nextOffset = 0L;

        LOGGER.info("Replicator.logPosition :{}",nextBinlogPosition);
        synchronized (lock) {
            // 说明没有 binlog 的变化
            if (nextBinlogPosition != null) {
                xid = this.xid;
                binlogFilename = nextBinlogPosition.getBinlogFilename();
                nextPosition = nextBinlogPosition.getPosition();
                nextOffset = nextQueueOffset;
            }
        }

        if (binlogFilename != null) {
            POSITION_LOGGER.info(" XID: {},   BINLOG_FILE: {},   NEXT_POSITION: {},   NEXT_OFFSET: {}", xid, binlogFilename, nextPosition, nextOffset);
        }

    }

    public Config getConfig() {
        return config;
    }

    public BinlogPosition getNextBinlogPosition() {
        return nextBinlogPosition;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
