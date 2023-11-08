package com.qiankun.mysql.position;

/**
 * 记录binlog的位置
 */
public class BinlogPosition {

    // binlog 文件名
    private String binlogFilename;
    // 偏移量
    private Long position;

    public BinlogPosition(String binlogFilename, Long position) {
        this.binlogFilename = binlogFilename;
        this.position = position;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "BinlogPosition{" +
                "binlogFilename='" + binlogFilename + '\'' +
                ", position=" + position +
                '}';
    }
}


