package com.qiankun.mysql.disruptor.factory;

import com.lmax.disruptor.EventFactory;
import com.qiankun.mysql.binlog.DataImageRow;

/**
 * @Description: 数据镜像 Row
 * @Date : 2023/11/30 12:29
 * @Auther : tiankun
 */
public class DataImageRowEventFactory implements EventFactory<DataImageRow> {
    @Override
    public DataImageRow newInstance() {
        return new DataImageRow();
    }
}
