package com.qiankun.mysql.dest.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qiankun.mysql.Config;
import org.bson.Document;


/**
 * @Description:
 * @Date : 2023/11/08 15:14
 * @Auther : tiankun
 */
public class MongoAdmin {

    private static MongoClient mongoClient = null;

    public static MongoClient getMongoClient(Config config){
        if(mongoClient == null){
            synchronized (MongoClient.class){
                if(mongoClient == null){
                    MongoCredential credential = MongoCredential.createCredential(config.mongoUsername, config.mongoDb, config.mongoPassword.toCharArray());
                    mongoClient = new MongoClient(new ServerAddress(config.mongoAddr,config.mongoPort),credential,MongoClientOptions.builder().build());
                }
            }
        }
        return mongoClient;
    }
}
