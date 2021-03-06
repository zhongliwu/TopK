package edu.usc.cs.util;


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.ArrayList;

public class TopKDatabaseRecorder {
    private static MongoClient client;
    private static DB db;
    private static long recordID;
    private static DBCollection coll;
    private static Logger log = Logger.getLogger(TopKDatabaseRecorder.class);
    private static TopKDatabaseRecorder instance;

    public static TopKDatabaseRecorder getInstance(String address, Integer port, String dbName) {
        if (instance == null) {
            instance = new TopKDatabaseRecorder();
        }

        return instance;
    }


    private TopKDatabaseRecorder () {
        recordID = 1L;
        openDBConnection(null, 0, null);
    }

    private TopKDatabaseRecorder (String address, Integer port, String dbName) {
        recordID = 1L;
        openDBConnection(address, port, dbName);
    }


    private boolean openDBConnection (String address, Integer port, String dbName) {
        if (address == null) {
            address = "localhost";
            port = 27017;
            dbName = "TopK";
        }

        try {
            client = new MongoClient(address, port);
            db = client.getDB(dbName);
            coll = db.createCollection("records", null);
            log.info("LOG_INFO_SUCCESS: Succeeded to connect to database: " + dbName);
        } catch (Exception e) {
            log.info("LOG_INFO_ERROR: Failed to connect to database.");
            log.info(e.getMessage());
            return false;
        }

        return true;
    }


    public void writeToDisk (ArrayList<TopKDataRecord> topList) {
        BasicDBObject recordsDoc = new BasicDBObject();
        for (int i = 0; i < topList.size(); i++) {
            TopKDataRecord record = topList.get(i);
            recordsDoc.append(record.getName(), record.getScore());
        }

        LocalDateTime time = new DateTime().toLocalDateTime();
        String recordTime = time.getMonthOfYear() + "/"
                + time.getDayOfMonth() +
                "/" + time.getYear() +
                ", " + time.getHourOfDay() +
                ":" + time.getMinuteOfHour() +
                ":" + time.getSecondOfMinute() +
                "-" + time.getMillisOfSecond();

        BasicDBObject doc = new BasicDBObject();
        doc.append("_id", recordID).append("Record Time", recordTime).append("records", recordsDoc);
        recordID++;

        try {
            coll.insert(doc);
        } catch (Exception e) {
            log.info("LOG_INFO_ERROR: Failed to insert document into collection: records");
            log.info(e.getMessage());
        }
    }

    public void closeWrite () {
        client.close();
    }
}
