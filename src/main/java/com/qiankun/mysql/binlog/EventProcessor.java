package com.qiankun.mysql.binlog;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import com.qiankun.mysql.Config;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcess;
import com.qiankun.mysql.dest.mongo.MongoProcess;
import com.qiankun.mysql.position.BinlogPosition;
import com.qiankun.mysql.position.BinlogPositionManager;
import com.qiankun.mysql.schemma.Schema;
import com.qiankun.mysql.schemma.Table;
import com.qiankun.mysql.schemma.column.Column;
import com.qiankun.mysql.schemma.column.ColumnParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事件处理器
 */
public class EventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private static final Pattern createTablePattern = Pattern.compile("^(CREATE|ALTER)\\s+TABLE", Pattern.CASE_INSENSITIVE);

    // 副本器
    private Replicator replicator;

    private Config config;
    // 监听的数据源
    private DataSource dataSource;
    // binlog 的位移管理器
    private BinlogPositionManager binlogPositionManager;
    // 事件阻塞队列（获取 binlog 事件）
    private BlockingQueue<Event> queue = new LinkedBlockingQueue<>(2000);
    // binlog 的客户端对象
    private BinaryLogClient binaryLogClient;
    // 事件监听器，处理 binlog 事件
    private EventListener eventListener;

    private Schema schema;

    /**
     * tableId : Table
     */
    private Map<Long, Table> tableMap = new HashMap<>();

    private Transaction transaction;

    private AbstractProcess process;



    public EventProcessor(Replicator replicator) {

        this.replicator = replicator;
        this.config = replicator.getConfig();
        this.process =  new MongoProcess(replicator);
    }

    public void start() throws Exception {
        // 初始化数据源（默认通过 durid 数据源）
        initDataSource();

        binlogPositionManager = new BinlogPositionManager(config, dataSource);
        binlogPositionManager.initBeginPosition();
        // 加载监听的数据库信息
        schema = new Schema(dataSource);
        schema.load();
        // mysql 事件监听器
        eventListener = new EventListener(queue);

        binaryLogClient = new BinaryLogClient(config.mysqlAddr,
            config.mysqlPort,
            config.mysqlUsername,
            config.mysqlPassword);
        binaryLogClient.setBlocking(true);
        binaryLogClient.setServerId(1001);

        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
            EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        binaryLogClient.setEventDeserializer(eventDeserializer);
        binaryLogClient.registerEventListener(eventListener);
        // 文件名称
        binaryLogClient.setBinlogFilename(binlogPositionManager.getBinlogFilename());
        // binlog偏移量
        binaryLogClient.setBinlogPosition(binlogPositionManager.getPosition());
        // 连接 mysql 服务
        binaryLogClient.connect(3000);

        LOGGER.info("Started.");
        doProcess();
    }

    /**
     * 事件处理
     */
    private void doProcess() {
        while (true) {
            try {
                Event event = queue.poll(1000, TimeUnit.MILLISECONDS);
                if (event == null) {
                    checkConnection();
                    continue;
                }
                switch (event.getHeader().getEventType()) {
                    case TABLE_MAP:
                        LOGGER.info("EVENT [TABLE_MAP] ..." + event);
                        processTableMapEvent(event);
                        break;
                    // 新增记录
                    case WRITE_ROWS:
                    case EXT_WRITE_ROWS:
                        LOGGER.info("EVENT [WRITE_ROWS|EXT_WRITE_ROWS]  ..." + event);
                        processInsertOrDeleteEvent(event,RowEventType.INSERT);
                        break;
                    // 修改记录
                    case UPDATE_ROWS:
                    case EXT_UPDATE_ROWS:
                        LOGGER.info("EVENT [UPDATE_ROWS|EXT_UPDATE_ROWS] ..." + event);
                        processUpdateEvent(event);
                        break;
                    // 删除记录
                    case DELETE_ROWS:
                    case EXT_DELETE_ROWS:
                        LOGGER.info("EVENT [DELETE_ROWS|EXT_DELETE_ROWS] ..." + event);
                        processInsertOrDeleteEvent(event,RowEventType.DELETE);
                        break;
                    // 查询时间
                    case UNKNOWN:
                        break;
                    case START_V3:
                        break;
                    case QUERY:
                        LOGGER.info("EVENT [QUERY] ..." + event);
                        processQueryEvent(event);
                        break;

                    case STOP:
                        break;
                    case ROTATE:
                        break;
                    case INTVAR:
                        break;
                    case LOAD:
                        break;
                    case SLAVE:
                        break;
                    case CREATE_FILE:
                        break;
                    case APPEND_BLOCK:
                        break;
                    case EXEC_LOAD:
                        break;
                    case DELETE_FILE:
                        break;
                    case NEW_LOAD:
                        break;
                    case RAND:
                        break;
                    case USER_VAR:
                        break;
                    case FORMAT_DESCRIPTION:
                        break;
                    case XID:
                        LOGGER.info("EVENT [XID] ..." + event);
                        // processXidEvent(event);
                        break;

                    case BEGIN_LOAD_QUERY:
                        break;
                    case EXECUTE_LOAD_QUERY:
                        break;
                    case PRE_GA_WRITE_ROWS:
                        break;
                    case PRE_GA_UPDATE_ROWS:
                        break;
                    case PRE_GA_DELETE_ROWS:
                        break;
                    case INCIDENT:
                        break;
                    case HEARTBEAT:
                        break;
                    case IGNORABLE:
                        break;
                    case ROWS_QUERY:
                        break;
                    case GTID:
                        break;
                    case ANONYMOUS_GTID:
                        break;
                    case PREVIOUS_GTIDS:
                        break;
                    case TRANSACTION_CONTEXT:
                        break;
                    case VIEW_CHANGE:
                        break;
                    case XA_PREPARE:
                        break;
                }
            } catch (Exception e) {
                LOGGER.error("Binlog process error.", e);
            }

        }
    }

    /**
     * 检查连接，如果切换了 binlog 文件
     * @throws Exception
     */
    private void checkConnection() throws Exception {

        if (!binaryLogClient.isConnected()) {
            BinlogPosition binlogPosition = replicator.getNextBinlogPosition();
            if (binlogPosition != null) {
                binaryLogClient.setBinlogFilename(binlogPosition.getBinlogFilename());
                binaryLogClient.setBinlogPosition(binlogPosition.getPosition());
            }
            binaryLogClient.connect(3000);
        }
    }

    /**
     * 缓存 tableId, 和 Table 已经 数据库 的关联关系
     * @param event
     */
    private void processTableMapEvent(Event event) {
        TableMapEventData data = event.getData();
        String dbName = data.getDatabase();
        String tableName = data.getTable();
        Long tableId = data.getTableId();

        Table table = schema.getTable(dbName, tableName);

        tableMap.put(tableId, table);
    }

    private void processInsertOrDeleteEvent(Event event,RowEventType rowEventType) {
        Long tableId = null;
        List<Serializable[]> rows = null;
        if(RowEventType.INSERT.equals(rowEventType)){
            WriteRowsEventData data = event.getData();
            tableId = data.getTableId();
            rows = data.getRows();
        }else if (RowEventType.DELETE.equals(rowEventType)){
            DeleteRowsEventData data = event.getData();
            tableId = data.getTableId();
            rows = data.getRows();
        }else {
            LOGGER.warn("event type not match : {}",rowEventType);
            return;
        }

        Table table = tableMap.get(tableId);
        if(table == null){
            return;
        }
        for (Serializable[] row : rows) {
            // 封装数据修改记录
            DataImageRow dataImageRow = new DataImageRow();
            dataImageRow.setDatabase(table.getDatabase());
            dataImageRow.setTable(table);
            dataImageRow.setType(rowEventType.name());
            dataImageRow.setMysqlType(event.getHeader().getEventType().name());
            List<Column> columnList = table.getColumnList();
            List<ColumnParser> parserList = table.getParserList();
            for (Column column : columnList) {
                ColumnParser columnParser = parserList.get(column.inx);
                dataImageRow.before.put(column.getColName(),columnParser.getValue(row[column.inx]));
            }
            // 默认第一个值就是id
            if(row.length > 0){
                dataImageRow.setId(parserList.get(0).getValue(row[0]));
            }
            process.process(dataImageRow);
            LOGGER.info("{} :{}",rowEventType.name(),dataImageRow);
        }

    }


    private void processUpdateEvent(Event event) {
        UpdateRowsEventData data = event.getData();
        Long tableId = data.getTableId();
        Table table = tableMap.get(tableId);
        if(table == null){
            return;
        }
        List<Map.Entry<Serializable[], Serializable[]>> rows = data.getRows();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            // 封装数据修改记录
            DataImageRow dataImageRow = new DataImageRow();
            dataImageRow.setDatabase(table.getDatabase());
            dataImageRow.setTable(table);
            dataImageRow.setType(RowEventType.UPDATE.name());
            dataImageRow.setMysqlType(event.getHeader().getEventType().name());
            List<Column> columnList = table.getColumnList();
            List<ColumnParser> parserList = table.getParserList();
            for (Column column : columnList) {
                ColumnParser columnParser = parserList.get(column.getInx());
                dataImageRow.before.put(column.getColName(),columnParser.getValue(row.getKey()[column.inx]));
                dataImageRow.after.put(column.getColName(),columnParser.getValue(row.getValue()[column.inx]));
            }
            // 默认第一个值就是id
            if(row.getKey().length > 0){
                dataImageRow.setId(parserList.get(0).getValue(row.getKey()[0]));
            }
            process.process(dataImageRow);
            LOGGER.info("insert or update :{}",dataImageRow);
        }
        // TODO
    }


    private void processQueryEvent(Event event) {
        QueryEventData data = event.getData();
        String sql = data.getSql();
        if (createTablePattern.matcher(sql).find()) {
            schema.reset();
        }
    }


    private void processXidEvent(Event event) {
        EventHeaderV4 header = event.getHeader();
        XidEventData data = event.getData();

        String binlogFilename = binaryLogClient.getBinlogFilename();
        Long position = header.getNextPosition();
        Long xid = data.getXid();

        BinlogPosition binlogPosition = new BinlogPosition(binlogFilename, position);
        transaction.setNextBinlogPosition(binlogPosition);
        transaction.setXid(xid);

        replicator.commit(transaction, true);
        transaction = new Transaction(config);
    }





    /**
     * 初始化线程池
     * @throws Exception
     */
    private void initDataSource() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("driverClassName", "com.mysql.jdbc.Driver");
        map.put("url", "jdbc:mysql://" + config.mysqlAddr + ":" + config.mysqlPort + "?useSSL=true&verifyServerCertificate=false");
        map.put("username", config.mysqlUsername);
        map.put("password", config.mysqlPassword);
        map.put("initialSize", "2");
        map.put("maxActive", "2");
        map.put("maxWait", "60000");
        map.put("timeBetweenEvictionRunsMillis", "60000");
        map.put("minEvictableIdleTimeMillis", "300000");
        map.put("validationQuery", "SELECT 1 FROM DUAL");
        map.put("testWhileIdle", "true");
        dataSource = DruidDataSourceFactory.createDataSource(map);

    }

    public Config getConfig() {
        return config;
    }

}
