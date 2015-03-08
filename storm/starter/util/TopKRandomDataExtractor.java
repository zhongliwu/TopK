package storm.starter.util;

import java.util.ArrayList;

public class TopKRandomDataExtractor extends TopKDataExtractor implements java.io.Serializable {
    public TopKRandomDataExtractor() {}

    @Override
    public ArrayList<TopKDataRecord> extractData() {
        ArrayList<TopKDataRecord> recordsList = new ArrayList<TopKDataRecord>(20);
        for(int i = 0; i < 20; i++) {
            String name = TopKRandomGenerator.generateName();
            float x = TopKRandomGenerator.generateFloat((float) 0.1, (float) 999999.99);
            recordsList.add(new TopKDataRecord(name, x));
        }
        return recordsList;
    }
}