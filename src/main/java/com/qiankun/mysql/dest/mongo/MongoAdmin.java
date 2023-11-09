package com.qiankun.mysql.dest.mongo;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClientFactory;
import com.mongodb.client.MongoClients;
import com.qiankun.mysql.Config;


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
                    String addr="mongodb://"+config.mongoAddr+":"+config.mongoPort;
                    MongoCredential credential = MongoCredential.createCredential(config.mongoUsername, config.mongoDb, config.mongoPassword.toCharArray());
                    MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                            .credential(credential)
                            .applyConnectionString(new ConnectionString(addr))
                            .build();
                     mongoClient = MongoClients.create(mongoClientSettings);
                }
            }
        }
        return mongoClient;
    }
}
