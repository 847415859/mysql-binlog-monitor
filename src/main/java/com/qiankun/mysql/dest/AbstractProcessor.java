package com.qiankun.mysql.dest;
import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.druid.sql.visitor.functions.Char;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.collect.Maps;

import com.qiankun.mysql.Config;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.binlog.DataImageRow;
import com.qiankun.mysql.binlog.Transaction;
import com.qiankun.mysql.dest.mongo.MongoAdmin;
import com.qiankun.mysql.position.BinlogPosition;
import com.qiankun.mysql.schemma.Schema;
import com.qiankun.mysql.schemma.Table;
import com.qiankun.mysql.schemma.column.Column;
import com.qiankun.mysql.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
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
public abstract class AbstractProcessor {

    protected static Logger LOGGER = LoggerFactory.getLogger(AbstractProcessor.class);

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

    /**
     * binlog事件的下个偏移量
     */
    long nextPosition;


    public AbstractProcessor(Replicator replicator) {
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

    /**
     * 处理row变更事件
     * @param dataImageRow
     */
    public void process(DataImageRow dataImageRow){
        process(Collections.singletonList(dataImageRow));
    }


    /**
     * 处理rows变更事件
     * @param dataImageRowList
     */
    public void process(List<DataImageRow> dataImageRowList){
        if (CollectionUtil.isEmpty(dataImageRowList)) {
            return;
        }
        List<ModelLog> modelLogList = dataImageRowList.stream().map(this::buildModel).collect(Collectors.toList());
        try {
            c[increment.get() & cnm].addAll(modelLogList);
            // 记录最后一条消息的 binlog 偏移量
            nextPosition = dataImageRowList.get(dataImageRowList.size() - 1).getNextPosition();
            if(c[increment.get() & cnm].size() >= batchSize) {
                batchHandle();
            }
        }catch (Exception e){
            LOGGER.error("handler error :{}",e);
        }
    }

    /**
     * 批量处理
     */
    public void batchHandle(){
        if (!isRunning.compareAndSet(false,true)) {
            return;
        }
        try {
            List<ModelLog> batchInsertEventInfos = c[increment.get() & cnm];
            // 切换接受容器
            increment.incrementAndGet();
            doHandle(batchInsertEventInfos);
            // 记录提交binlog的偏移量
            BinaryLogClient binaryLogClient = replicator.getEventProcessor().getBinaryLogClient();
            BinlogPosition binlogPosition = new BinlogPosition(binaryLogClient.getBinlogFilename(), nextPosition);
            replicator.commit(new Transaction(binlogPosition));

            batchInsertEventInfos.clear();
            lastUpdateTime = System.currentTimeMillis();
        } finally {
            isRunning.set(false);
        }
    }


    /**
     * 根据ModelId查询数据具体哪些字段进行了变更
     * @param modelId
     * @return
     */
    public List<ModelChangeView> viewChange(String modelId){
        List<ModelChangeView> modelChangeViewList = new LinkedList<>();
        List<ModelLog> modelLogList = queryByModelId(modelId);
        for (ModelLog modelLog : modelLogList) {
            ModelChangeView modelChangeView = new ModelChangeView();
            modelChangeView.setDatabase(modelLog.getDatabase());
            modelChangeView.setTable(modelLog.getTable());
            modelChangeView.setDate(DateUtils.dateStr(new Date(modelLog.getChangeTimestamp())));
            modelChangeView.setChangeColumnMap(buildChangeColumnMap(modelLog));
            modelChangeViewList.add(modelChangeView);
        }
        return modelChangeViewList;
    }


    /**
     * 装装哪些字段变更记录
     * @param modelLog
     * @return
     */
    private Map<String, Pair> buildChangeColumnMap(ModelLog modelLog) {
        Map<String,Pair> changeColumnMap = new LinkedHashMap<>();
        Schema schema = replicator.getSchema();
        Table table = schema.getTable(modelLog.getDatabase(), modelLog.getTable());
        if(table != null){
            List<Column> columnList = table.getColumnList();
            Map<String, Object> after = modelLog.getAfter();
            Map<String, Object> before = modelLog.getBefore();
            for (Column column : columnList) {
                String colName = column.getColName();
                Object beforeVal = before.get(colName);
                Object afterVal = after.get(colName);
                // 只保留变更的字段信息
                if((beforeVal != null || afterVal != null) && !equalsVal(beforeVal,afterVal)) {
                    changeColumnMap.put(colName, new Pair(beforeVal,afterVal));
                }
            }
        }
        return changeColumnMap;
    }


    private boolean equalsVal(Object beforeVal, Object afterVal) {
        if(beforeVal == null || afterVal == null){
            return false;
        }else {
            if(beforeVal instanceof String && afterVal instanceof String){
                return beforeVal.toString().equals(afterVal.toString());
            }else if(beforeVal instanceof Long && afterVal instanceof Long){
                return Long.valueOf(beforeVal.toString()).equals(Long.valueOf(afterVal.toString()));
            }else if(beforeVal instanceof Integer && afterVal instanceof Integer){
                return Integer.valueOf(beforeVal.toString()).equals(Integer.valueOf(afterVal.toString()));
            }else if(beforeVal instanceof Double && afterVal instanceof Double){
                return Double.valueOf(beforeVal.toString()).equals(Double.valueOf(afterVal.toString()));
            }else if(beforeVal instanceof Float && afterVal instanceof Float){
                return Float.valueOf(beforeVal.toString()).equals(Float.valueOf(afterVal.toString()));
            }else if(beforeVal instanceof Short && afterVal instanceof Short){
                return Short.valueOf(beforeVal.toString()).equals(Short.valueOf(afterVal.toString()));
            }else if(beforeVal instanceof Boolean && afterVal instanceof Boolean){
                return Boolean.valueOf(beforeVal.toString()).equals(Boolean.valueOf(afterVal.toString()));
            }else{
               // 判断是否是 BigDecimal类型
                try {
                    return new BigDecimal(beforeVal.toString()).compareTo(new BigDecimal(afterVal.toString())) == 0;
                }catch (Exception ignored){
                }
            }
        }
        return false;
    }


    /**
     * 数据持久化
     * @param modelLogList
     */
    public abstract void doHandle(List<ModelLog> modelLogList);

    /**
     * 根据 modelId,查询变更记录
     * @param modelId
     * @return
     */
    public abstract List<ModelLog> queryByModelId(String modelId);


    private ModelLog buildModel(DataImageRow dataImageRow) {
        return new ModelLog(
                dataImageRow.getDatabase(),
                dataImageRow.getTable().getName(),
        dataImageRow.getCamelTableName() + ":" + dataImageRow.getId(),
                dataImageRow.getCamelTableName(),
                dataImageRow.getType(),
                dataImageRow.getBefore(),
                dataImageRow.getAfter(),
                dataImageRow.getChangeTimestamp()
        );
    }

}
