package com.qiankun.mysql.disruptor.producer;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.qiankun.mysql.binlog.DataImageRow;

/**
 * @Description: 事件消费者
 * @Date : 2023/11/30 22:31
 * @Auther : tiankun
 */
public interface EventProducer  {

    /**
     * 发送消息
     * @param dataImageRow
     */
    void onData(DataImageRow dataImageRow);
}
