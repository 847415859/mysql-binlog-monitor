package com.qiankun.mysql.disruptor.producer.impl;

import com.lmax.disruptor.RingBuffer;
import com.qiankun.mysql.binlog.DataImageRow;
import com.qiankun.mysql.disruptor.producer.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.hutool.core.bean.BeanUtil.copyProperties;

/**
 * @Description:
 * @Date : 2023/11/30 22:35
 * @Auther : tiankun
 */
public class DefaultEventProducer implements EventProducer {

    private static Logger logger = LoggerFactory.getLogger(DefaultEventProducer.class);

    //事件队列
    private RingBuffer<DataImageRow> ringBuffer;

    public DefaultEventProducer(RingBuffer<DataImageRow> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void onData(DataImageRow from) {
        //获取到下一个序号
        long sequence = ringBuffer.next();
        try {
            //获取消息（事件）
            DataImageRow to = ringBuffer.get(sequence);
            // 写入消息数据
            copyProperties(from,to);
        } catch (Exception e) {
            logger.error("onDate error sequence :{}  reason: {}",sequence,e);
        } finally {
            //发布事件,标识当前进度的事件可用
            ringBuffer.publish(sequence);
        }
    }
}
