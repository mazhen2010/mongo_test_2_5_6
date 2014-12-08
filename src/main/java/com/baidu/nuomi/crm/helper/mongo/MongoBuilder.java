package com.baidu.nuomi.crm.helper.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: mazhen01
 * Date: 2014/11/26
 * Time: 15:27
 */
public class MongoBuilder {

    private static MongoClient mongoClient;
    private final static Boolean lock = true;

    public static MongoClient getMongoClient() throws Exception {
        if (mongoClient != null) {
            return mongoClient;
        }

        synchronized (lock) {
            if (mongoClient != null) {
                return mongoClient;
            }

            MongoClientOptions options = MongoClientOptions.builder().maxWaitTime(1000 * 60 * 2).connectionsPerHost(500).build();
            mongoClient = new MongoClient(Arrays.asList(new ServerAddress("10.205.68.57", 8700), new ServerAddress("10.205.68.15", 8700), new ServerAddress("10.205.69.13", 8700)), options);
            mongoClient.setReadPreference(ReadPreference.secondaryPreferred());

            //mongoClient = new MongoClient("localhost", 27017);
            //mongoClient = new MongoClient(new ServerAddress("localhost", 27017), options);

            return mongoClient;
        }
    }

    public static synchronized void closeMongoClient() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
