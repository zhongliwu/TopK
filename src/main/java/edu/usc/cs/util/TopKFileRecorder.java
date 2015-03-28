package edu.usc.cs.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;

public class TopKFileRecorder {
    private static String fileName;
    private static BufferedWriter writer;
    private static TopKFileRecorder instance;

    public static TopKFileRecorder getInstance (String s) {
        if (instance == null) {
            instance = new TopKFileRecorder(s);
        }

        return instance;
    }


    private TopKFileRecorder () {
        fileName = "";
        initialize(fileName);
    }

    private TopKFileRecorder (String s) {
        fileName = s;
        initialize(fileName);
    }

    private void initialize(String file) {
        try {
            writer = new BufferedWriter(new FileWriter(file));
            writer.write("");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void writeToDisk (ArrayList<TopKDataRecord> topList) {
        try {
            LocalDateTime time = new DateTime().toLocalDateTime();
            String recordTime = time.getMonthOfYear() + "/"
                    + time.getDayOfMonth() +
                    "/" + time.getYear() +
                    ", " + time.getHourOfDay() +
                    ":" + time.getMinuteOfHour() +
                    ":" + time.getSecondOfMinute() +
                    "-" + time.getMillisOfSecond();
            writer.write("=============================================\n");
            writer.write("Recorded on: \n");
            writer.write(recordTime + "\n");
            writer.write("=============================================\n");
            int i = 1;
            for(TopKDataRecord record : topList) {
                writer.write(Integer.toString(i) + ". " + record.toString() + "\n");
                i++;
            }
            writer.write("---------------------------------------------\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeWrite() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
