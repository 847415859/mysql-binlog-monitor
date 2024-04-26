package com.qiankun.mysql.dest.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcessor;
import com.qiankun.mysql.dest.ModelLog;
import com.qiankun.mysql.utils.DateUtils;
import org.bson.Document;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Date : 2023/11/08 16:25
 * @Auther : tiankun
 */
public class MongoProcessor extends AbstractProcessor {

    MongoCollection<Document> collection;

    public MongoProcessor() {
        MongoClient mongoClient = MongoAdmin.getMongoClient(Replicator.replicator.getConfig());
        MongoDatabase mongoClientDatabase = mongoClient.getDatabase(Replicator.replicator.getConfig().mongoDb);
        this.collection = mongoClientDatabase.getCollection(Replicator.replicator.getConfig().mongoCollection);
    }

    @Override
    public void doHandle(List<ModelLog> modelLogList) {
        if(!CollectionUtils.isEmpty(modelLogList)) {
            // 过滤下重复的数据
            Map<String, ModelLog> modelLogMap = modelLogList.stream().collect(Collectors.toMap(ModelLog::getAfterDigest, Function.identity(), (i1,i2) ->i2));
            List<Document> documentList = buildDocument(modelLogMap.values());
            collection.insertMany(documentList);
        }
    }

    @Override
    public List<ModelLog> queryByModelId(String modelId) {
        FindIterable<Document> documents = collection.find(Filters.eq("modelId", modelId));
        List<ModelLog> modelLogList = new LinkedList<>();
        for (Document document : documents) {
            ModelLog modelLog = new ModelLog();
            modelLog.setDatabase(document.getString("database"));
            modelLog.setTable(document.getString("table"));
            modelLog.setType(document.getString("type"));
            modelLog.setModelId(document.getString("modelId"));
            modelLog.setModel(document.getString("model"));
            modelLog.setBefore(document.get("before", Map.class));
            modelLog.setAfter(document.get("after", Map.class));
            modelLog.setChangeTimestamp(DateUtils.parseDate(document.getString("date")).getTime());
            modelLogList.add(modelLog);
        }
        return modelLogList;
    }

    private List<Document> buildDocument(Collection<ModelLog> modelLogList) {
        List<Document> documentList = new LinkedList<>();
        for (ModelLog modelLog : modelLogList) {
            Document document = new Document();
            document.put("model",modelLog.getModel());
            document.put("modelId",modelLog.getModelId());
            document.put("database",modelLog.getDatabase());
            document.put("table",modelLog.getTable());
            document.put("type",modelLog.getType());
            document.put("before",modelLog.getBefore());
            document.put("after",modelLog.getAfter());
            document.put("date", DateUtils.dateStr(new Date(modelLog.getChangeTimestamp())));
            document.put("afterDigest", modelLog.getAfterDigest());
            documentList.add(document);
        }
        return documentList;
    }
}
