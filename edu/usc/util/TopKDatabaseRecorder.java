package edu.usc.util;


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

import java.util.ArrayList;

public class TopKDatabaseRecorder extends TopKRecorder {
    private MongoClient client;
    private DB db;
    private long recordID;
    private DBCollection coll;
    private static Logger log = Logger.getLogger(TopKDatabaseRecorder.class);;

    public TopKDatabaseRecorder () {
        recordID = 1L;
        openDBConnection(null, 0, null);
    }

    public TopKDatabaseRecorder (String address, Integer port, String dbName) {
        recordID = 1L;
        openDBConnection(address, port, dbName);
    }

    @Override
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

    @Override
    public void closeWrite () {

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
}
