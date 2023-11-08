package com.qiankun.mysql.dest.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcess;
import com.qiankun.mysql.dest.ModelLog;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

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
            documentList.add(document);
        }
        return documentList;
    }
}
