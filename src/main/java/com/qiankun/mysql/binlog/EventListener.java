package com.qiankun.mysql.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * binlog 监听器
 */
public class EventListener implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener {

    private Logger LOGGER = LoggerFactory.getLogger(EventListener.class);

    /**
     * 监听 Binlog 事件
     */
    private BlockingQueue<Event> queue;

    public EventListener(BlockingQueue<Event> queue) {
        this.queue = queue;
    }

    @Override
    public void onEvent(Event event) {
        try {
            while (true) {
                if (queue.offer(event, 1000, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onConnect(BinaryLogClient client) {
        LOGGER.info("onConnect :{}",client);
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception e) {
        LOGGER.info("onCommunicationFailure :{}",client);
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient client, Exception e) {
        LOGGER.info("onEventDeserializationFailure :{}",client);
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        LOGGER.info("onDisconnect :{}",client);
    }
}
