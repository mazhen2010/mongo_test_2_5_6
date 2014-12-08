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
 * Date: 2014/11/25
 * Time: 18:10
 */
public class MainTest {

    private static final String DB_NAME = "nuomi_crm";
    private static final String COLLECTION_NAME = "firm_index";
    private static final String COLLECTION_TEXT_NAME = "firm_text";

    public static void main(String[] args) {
        System.out.println("start");
        MainTest test = new MainTest();
        try {
            //test.doInsert5();
            //test.doInsert20();
            //test.createIndex();
            //test.doInsertText();
            //test.doInsertDoc();
            test.doSelect();

            //test.selectRegular(test.getCollection(COLLECTION_NAME));
            //test.countRegular(test.getCollection(COLLECTION_NAME));
            //test.orderRegular(test.getCollection(COLLECTION_NAME));
            //test.orderMultiple(test.getCollection(COLLECTION_NAME));
            //test.countMultiple(test.getCollection(COLLECTION_NAME));
            //test.orderMultiple(test.getCollection(COLLECTION_NAME));
            //test.selectText(test.getCollection(COLLECTION_TEXT_NAME));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void test(Integer ...doTimes) {
        System.out.println(doTimes.length);
    }

    private DB getDB() throws Exception {
        DB db = MongoBuilder.getMongoClient().getDB(DB_NAME);
        return db;
    }

    private DBCollection getCollection(String collectionName) throws Exception {
        return getDB().getCollection(collectionName);
    }

    public void doInsert5() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_NAME);
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
                    DBObject aFirmIndex = new BasicDBObject("firmId", firmId)
                            .append("firmName", "覆雪清泉" + firmId)
                            .append("dealCount", firmId)
                            .append("categoryName", "静观勿扰")
                            .append("originFlag", 1)
                            ;
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

    public void doInsert20() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_NAME);
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
                    DBObject aFirmIndex = new BasicDBObject("firmId", firmId)
                            .append("firmName", "覆雪清泉" + firmId)
                            .append("dealCount", firmId)
                            .append("categoryName", "静观勿扰")
                            .append("originFlag", 1)
                            .append("level", 10)
                            .append("staffId", firmId)
                            .append("stationId", idRandom.nextInt(50))
                            .append("stationName", Thread.currentThread().getName())
                            .append("createDate", new Date())
                            .append("phone", 110)
                            .append("desc", "我知道你不知道我知道你不知道")
                            ;
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

    public void doInsertText() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_TEXT_NAME);
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
                    DBObject aFirmIndex = new BasicDBObject("firmId", firmId)
                            .append("firmName", ChineseToPinYin.chineseToPinyinWithOutPlus("覆雪清泉" + firmId))
                            .append("dealCount", firmId)
                            .append("categoryName", "静观勿扰")
                            .append("originFlag", 1)
                            .append("level", 10)
                            .append("staffId", firmId)
                            .append("stationId", idRandom.nextInt(50))
                            .append("stationName", Thread.currentThread().getName())
                            .append("createDate", new Date())
                            .append("phone", 110)
                            .append("desc", "我知道你不知道我知道你不知道")
                            ;
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




    public void createIndex() throws Exception {
        DBCollection firmIndexCollection = getCollection(COLLECTION_TEXT_NAME);
        DBObject index = new BasicDBObject("firmName", "text");
        firmIndexCollection.createIndex(index);
        MongoBuilder.closeMongoClient();
    }

    public void doSelect() throws Exception {
        final DBCollection firmIndexCollection = getCollection(COLLECTION_NAME);

        ExecutorService executorService = new ThreadPoolExecutor(200, 200, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        long totalStart = System.currentTimeMillis();
        System.out.println("doSelect start");

        int dataCount = 20000;
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

    public void selectRegular(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        //DBObject queryObject = new BasicDBObject("firmName", "/雪清/");
        //Pattern pattern = Pattern.compile("^\\w*" + "雪清"+ "\\w*$");
        //        Pattern pattern = Pattern.compile("^.*" + "雪清"+ ".*$");
        Random r = new Random();
        String key = "清泉" + r.nextInt(10000);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("firmName", pattern);
        DBCursor cursor = collection.find(queryObject).limit(10).skip(10);
//            .append("firmId", new BasicDBObject("$gt", 5000))
//            .append("stationId", new BasicDBObject("$in", new Integer[]{15, 20}));

//        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{15, 20}))
//                .append("firmId", new BasicDBObject("$gt", 5000))
//                .append("firmName", pattern);

//                BasicDBObject queryObject = new BasicDBObject("firmId", new BasicDBObject("$gt", 5000))
//            .append("stationId", new BasicDBObject("$in", new Integer[]{15, 20}));

        //DBCursor cursor = collection.find(queryObject).sort(new BasicDBObject("createDate", 1)).limit(10).skip(10);

        //DBCursor cursor = collection.find(queryObject);
        while(cursor.hasNext()) {
            DBObject result = cursor.next();
            //System.out.println(result.toString());
            //break;
        }
//        System.out.println(cursor.count());
        //int count = collection.find(queryObject).count();
        //System.out.println(cursor.length());
//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void countRegular(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(10000);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("firmName", pattern);
        Integer count = collection.find(queryObject).count();
//        DBCursor cursor = collection.find(queryObject);   server看查询语句和count()方法一样
//        Integer count = cursor.count();

//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void orderRegular(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(10000);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("firmName", pattern);
        DBCursor cursor = collection.find(queryObject).sort(new BasicDBObject("createDate", 1)).limit(10).skip(10);
        while(cursor.hasNext()) {
            DBObject result = cursor.next();
        }

//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void selectMultiple(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = "清泉" + r.nextInt(100);
        Pattern pattern = Pattern.compile(key);
        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{20}))
                .append("firmId", new BasicDBObject("$gt", r.nextInt(10000)))
                .append("dealCount", new BasicDBObject("$gt", r.nextInt(1000000)))
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
        BasicDBObject queryObject = new BasicDBObject("stationId", new BasicDBObject("$in", new Integer[]{20}))
                .append("firmId", new BasicDBObject("$gt", r.nextInt(10000)))
                .append("dealCount", new BasicDBObject("$gt", r.nextInt(1000000)))
                .append("firmName", pattern);
        BasicDBObject resultObject = new BasicDBObject("firmId", 1);
        //long count = collection.getCount(queryObject, resultObject);
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
                .append("firmId", new BasicDBObject("$gt", r.nextInt(10000)))
                .append("dealCount", new BasicDBObject("$gt", r.nextInt(1000000)))
                .append("firmName", pattern);
        DBCursor cursor = collection.find(queryObject).sort(new BasicDBObject("firmId", 1)).limit(10).skip(2);

        while(cursor.hasNext()) {
            DBObject result = cursor.next();
//            System.out.println(result.toString());
            //break;
        }

//        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
//        MongoBuilder.closeMongoClient();
    }

    public void selectText(DBCollection collection) {
        long startTime = System.currentTimeMillis();
        Random r = new Random();
        String key = ChineseToPinYin.chineseToPinyinWithPlus("清泉" + r.nextInt(10000));
       // String[] kk = {"E8A686", "E6B885"};
        BasicDBObject search = new BasicDBObject("$search", "\"E6B389 00000091\"");
        //BasicDBObject search = new BasicDBObject("$search", "\"" + key + "\"");
        //BasicDBObject search = new BasicDBObject("$search", key);
        BasicDBObject textSearch = new BasicDBObject("$text", search);
        DBCursor cursor = collection.find(textSearch).limit(10);
        while(cursor.hasNext()) {
            DBObject result = cursor.next();
            System.out.println(result);
        }

        System.out.println("key:" + key);
        //System.out.println("count:" + cursor.count());
        System.out.println("total cost:" + (System.currentTimeMillis() - startTime));
        MongoBuilder.closeMongoClient();
    }
}
