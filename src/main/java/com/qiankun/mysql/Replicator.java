package com.qiankun.mysql;

import com.qiankun.mysql.binlog.EventProcessor;
import com.qiankun.mysql.binlog.Transaction;
import com.qiankun.mysql.position.BinlogPosition;
import com.qiankun.mysql.position.BinlogPositionLogThread;
import com.qiankun.mysql.disruptor.schemma.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Properties;


public class Replicator {

    public static Replicator replicator;

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private static final Logger POSITION_LOGGER = LoggerFactory.getLogger("PositionLogger");

    private Config config;

    private Schema schema;

    private EventProcessor eventProcessor;


    public Replicator() {
        synchronized (Replicator.class) {
            if(Replicator.replicator != null){
                throw new RuntimeException("Replicator already started! ");
            }
            config = new Config();
            try {
                config.load();
            } catch (IOException e) {
                LOGGER.error("Load setting file error：{}", e);
            }
            // 默认以mongo的形式来保存数据
            Replicator.replicator = this;
        }
    }

    public Replicator(Properties properties) {
        synchronized (Replicator.class) {
            if(Replicator.replicator != null){
                throw new RuntimeException("Replicator already started! ");
            }
            config = new Config();
            try {
                config.load(properties);
            } catch (IOException e) {
                LOGGER.error("Load setting file error：{}", e);
            }
            // 默认以mongo的形式来保存数据
            Replicator.replicator = this;
        }
    }

    private final Object lock = new Object();
    private BinlogPosition nextBinlogPosition;
    private long xid;

    public static void main(String[] args) throws IOException {
        InputStream in = Config.class.getClassLoader().getResourceAsStream("mysql-monitor.conf");
        Properties properties = new Properties();
        properties.load(in);

        Replicator replicator = new Replicator(properties);
        replicator.start();
    }

    public void start() {
        try {
            // 开启一个线程记录binlog文件
            BinlogPositionLogThread binlogPositionLogThread = new BinlogPositionLogThread(this);
            binlogPositionLogThread.start();
            // 事件处理
            eventProcessor = new EventProcessor(this);
            eventProcessor.start();
        } catch (Exception e) {
            LOGGER.error("Start error.", e);
            System.exit(1);
        }
    }

    /**
     * 提交数据（表明当前偏移量的数据已经消费）
     * @param transaction
     */
    public void commit(Transaction transaction) {
        LOGGER.debug("Flush Successful , position :{} ", transaction.getNextBinlogPosition());
        nextBinlogPosition = transaction.getNextBinlogPosition();
    }

    /**
     * 记录偏移量
     */
    public void logPosition() {
        String binlogFilename = null;
        long xid = 0L;
        long nextPosition = 0L;
        long nextOffset = 0L;
        LOGGER.debug("Replicator.logPosition :{}",nextBinlogPosition);
        synchronized (lock) {
            // 说明没有 binlog 的变化
            if (nextBinlogPosition != null) {
                xid = this.xid;
                binlogFilename = nextBinlogPosition.getBinlogFilename();
                nextPosition = nextBinlogPosition.getPosition();
                try {
                    RandomAccessFile raf  = new RandomAccessFile(new File("./position"),"rw");
                    raf.writeShort(binlogFilename.length());
                    raf.writeBytes(binlogFilename);
                    raf.writeLong(nextOffset);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (binlogFilename != null) {
            POSITION_LOGGER.debug(" XID: {},   BINLOG_FILE: {},   NEXT_POSITION: {},   NEXT_OFFSET: {}", xid, binlogFilename, nextPosition, nextOffset);
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


    public EventProcessor getEventProcessor() {
        return eventProcessor;
    }
}
