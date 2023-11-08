package com.qiankun.mysql.dest;

import com.qiankun.mysql.Config;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.binlog.DataImageRow;
import com.qiankun.mysql.dest.mongo.MongoAdmin;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @Description: 数据处理
 * 对获取到的数据进行解析
 * @Date : 2023/11/08 10:51
 * @Auther : tiankun
 */
public abstract class AbstractProcess {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractProcess.class);

    private Replicator replicator;

    int batchSize;

    long interval;

    /**
     * 容器数量
     */
    int cn = 2;

    int cnm = cn -1;

    AtomicBoolean isRunning = new AtomicBoolean(false);

    long lastUpdateTime = System.currentTimeMillis();

    List<ModelLog>[] c = new ArrayList[cn];

    AtomicInteger increment = new AtomicInteger(0);

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();


    public AbstractProcess(Replicator replicator) {
        this.replicator = replicator;
        Config config = replicator.getConfig();
        batchSize = config.batchSize;
        interval = config.interval;
        // 初始化容器
        for (int i = 0; i < cn; i++) {
            c[i] = new ArrayList<>(batchSize << 1);
        }

        executorService.scheduleAtFixedRate(() -> {
            LOGGER.debug("Thread 当前容器：{}， lastUpdateTime ：{}, 事件数量：{} data:{}",increment.get() & 1,lastUpdateTime,c[increment.get() & 1].size(),c[increment.get() & 1]);
            if(lastUpdateTime + interval <= System.currentTimeMillis() && !c[increment.get() & cnm].isEmpty()) {
                batchHandle();
            }
        },0,interval, TimeUnit.MILLISECONDS);
    }

    public void process(DataImageRow dataImageRow){
        process(Collections.singletonList(dataImageRow));
    }


    public void process(List<DataImageRow> dataImageRowList){
        List<ModelLog> modelLogList = dataImageRowList.stream().map(this::buildModel).collect(Collectors.toList());
        try {
            c[increment.get() & cnm].addAll(modelLogList);
            if(c[increment.get() & cnm].size() >= batchSize) {
                batchHandle();
            }
        }catch (Exception e){
            LOGGER.error("handler error :{}",e);
        }
    }

    public void batchHandle(){
        if (!isRunning.compareAndSet(false,true)) {
            return;
        }
        try {
            List<ModelLog> batchInsertEventInfos = c[increment.get() & cnm];
            // 切换接受容器
            increment.incrementAndGet();
            doHandle(batchInsertEventInfos);
            batchInsertEventInfos.clear();
            lastUpdateTime = System.currentTimeMillis();
        } finally {
            isRunning.set(false);
        }
    }


    public abstract void doHandle(List<ModelLog> modelLogList);


    private ModelLog buildModel(DataImageRow dataImageRow) {
        return new ModelLog(
                dataImageRow.getDatabase(),
                dataImageRow.getTable().getName(),
                dataImageRow.getCamelTableName() + ":" + dataImageRow.getId(),
                dataImageRow.getCamelTableName(),
                dataImageRow.getType(),
                dataImageRow.getBefore(),
                dataImageRow.getAfter()
        );
    }

}
