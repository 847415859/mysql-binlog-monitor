package com.qiankun.mysql.position;

import com.qiankun.mysql.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * binlog偏移量记录线程
 */
public class BinlogPositionLogThread extends Thread {
    private Logger logger = LoggerFactory.getLogger(BinlogPositionLogThread.class);

    private Replicator replicator;

    public BinlogPositionLogThread(Replicator replicator) {
        this.replicator = replicator;
        setDaemon(true);
    }
    @Override
    public void run() {

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Offset thread interrupted.", e);
            }
            replicator.logPosition();
        }
    }
}
