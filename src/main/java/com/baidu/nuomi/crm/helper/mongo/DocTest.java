package com.baidu.nuomi.crm.helper.mongo;

import com.mongodb.*;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: mazhen01
 * Date: 2014/12/4
 * Time: 19:30
 */
public class DocTest {

    private static final String DB_NAME = "nuomi_crm";
    private static final String COLLECTION_DOC_NAME = "firm_doc";
    private static final String COLLECTION_ARRAY_NAME = "firm_array";

    public static void main(String[] args) {
        System.out.println("start");
        DocTest test = new DocTest();
        try {
            //test.doInsert5();
            //test.doInsert20();
            //test.createIndex();
            //test.doInsertText();
            test.doInsertDoc();
            //test.doSelect();

            //test.selectMultiple(test.getCollection(COLLECTION_DOC_NAME));
            //test.countMultiple(test.getCollection(COLLECTION_DOC_NAME));
            //test.orderMultiple(test.getCollection(COLLECTION_DOC_NAME));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void doInsertDoc() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_ARRAY_NAME);
        firmIndexCollection.drop();

        ExecutorService executorService = new ThreadPoolExecutor(50, 50, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        long totalStart = System.currentTimeMillis();
        System.out.println("doInsert start");

        final int dataCount = 2000000;
        final Random idRandom = new Random();
        for (int i =0; i < dataCount; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    int firmId = idRandom.nextInt(dataCount);
                    BasicDBObject[] clue = {new BasicDBObject("firmId", firmId), new BasicDBObject("dealCount", firmId),
                            new BasicDBObject("categoryName", "静观勿扰"), new BasicDBObject("originFlag", 1), new BasicDBObject("level", 10), new BasicDBObject("staffId", firmId),
                            new BasicDBObject("createDate", new Date()), new BasicDBObject("phone", 110), new BasicDBObject("desc", "我知道你不知道我知道你不知道")};
                    DBObject aFirmIndex = new BasicDBObject("firmName", "覆雪清泉" + firmId)
                            .append("stationName", Thread.currentThread().getName())
                            .append("stationId", idRandom.nextInt(50))
                            .append("clue", clue);
                    firmIndexCollection.insert(aFirmIndex);
                }
            });
        }


        System.out.println("total cost:" + (System.currentTimeMillis() - totalStart));
        System.out.println("doInsert end");
        Thread.sleep(5000);
        executorService.shutdown();
        MongoBuilder.closeMongoClient();
        System.out.println("doInsert close");
    }

    public void doSelect() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_DOC_NAME);

        ExecutorService executorService = new ThreadPoolExecutor(100, 100, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        long totalStart = System.currentTimeMillis();
        System.out.println("doSelect start");

        int dataCount = 1000;
        for (int i =0; i < dataCount; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    orderMultiple(firmIndexCollection);
                }
            });
        }


        System.out.println("total cost:" + (System.currentTimeMillis() - totalStart));
        System.out.println("doSelect end");
        Thread.sleep(5000);
        executorService.shutdown();
        MongoBuilder.closeMongoClient();
        System.out.println("doSelect close");
    }

    public void selectMultiple(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(100);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{19, 20}))
                .append("clue.firmId", new BasicDBObject("$gt", 5000))
                .append("clue.level", 10)
                .append("firmName", pattern);
        DBCursor cursor = collection.find(queryObject).limit(10).skip(2);

        while(cursor.hasNext()) {
            DBObject result = cursor.next();
//            System.out.println(result.toString());
            //break;
        }

//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void countMultiple(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(100);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{19, 20}))
                .append("clue.firmId", new BasicDBObject("$gt", 5000))
                .append("clue.level", 10)
                .append("firmName", pattern);
        long count = collection.count(queryObject);
        //Integer count = (Integer) collection.find(queryObject).explain().get("n");

//        System.out.println("count:" + count);
//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void orderMultiple(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(100);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{20}))
                .append("clue.firmId", new BasicDBObject("$gt", 5000))
                .append("clue.level", 10)
                .append("firmName", pattern);
        DBCursor cursor = collection.find(queryObject).sort(new BasicDBObject("clue.firmId", 1)).limit(10).skip(2);

        while(cursor.hasNext()) {
            DBObject result = cursor.next();
//            System.out.println(result.toString());
            //break;
        }

//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    private DB getDB() throws Exception {
        DB db = MongoBuilder.getMongoClient().getDB(DB_NAME);
        return db;
    }

    private DBCollection getCollection(String collectionName) throws Exception {
        return getDB().getCollection(collectionName);
    }
}
