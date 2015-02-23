package edu.usc.util;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;

import org.json.JSONArray;
import org.json.JSONObject;

public class TopKRESTfulDataExtractor extends TopKDataExtractor {
    public TopKRESTfulDataExtractor() {}

    @Override
    public ArrayList<TopKDataRecord> extractData() {
        ArrayList<TopKDataRecord> recordsList = new ArrayList<TopKDataRecord>(20);
        try {
            StringBuilder JSONText = new StringBuilder();

            URL url = new URL("http://laurenceusc-test.apigee.net/v1/datarecord/next");
            InputStream input = url.openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charset.forName("UTF-8")));
            String line;
            while ((line = reader.readLine()) != null) {
                JSONText.append(line);
            }

            JSONObject object = new JSONObject(JSONText.toString());
            JSONArray array = (JSONArray)object.get("Records");
            for (int i = 0; i < array.length(); i++) {
                JSONObject obj = (JSONObject) array.get(i);
                String uid = obj.get("player_uid").toString();
                Float score = Float.parseFloat(obj.get("score").toString());
                TopKDataRecord record = new TopKDataRecord(uid, score);
                recordsList.add(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return recordsList;
        }


        return recordsList;
    }
}