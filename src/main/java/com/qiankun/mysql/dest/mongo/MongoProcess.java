package com.qiankun.mysql.dest.mongo;
import com.google.common.collect.Maps;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcess;
import com.qiankun.mysql.dest.ModelLog;
import com.qiankun.mysql.utils.DateUtils;
import com.sun.org.apache.xpath.internal.operations.Mod;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Date : 2023/11/08 16:25
 * @Auther : tiankun
 */
public class MongoProcess extends AbstractProcess {

    MongoCollection<Document> collection;

    public MongoProcess(Replicator replicator) {
        super(replicator);
        MongoClient mongoClient = MongoAdmin.getMongoClient(replicator.getConfig());
        MongoDatabase mongoClientDatabase = mongoClient.getDatabase(replicator.getConfig().mongoDb);
        this.collection = mongoClientDatabase.getCollection(replicator.getConfig().mongoCollection);
    }

    @Override
    public void doHandle(List<ModelLog> modelLogList) {
        List<Document> documentList = buildDocument(modelLogList);
        collection.insertMany(documentList);
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

    private List<Document> buildDocument(List<ModelLog> modelLogList) {
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
            documentList.add(document);
        }
        return documentList;
    }
}
