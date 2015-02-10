package edu.usc.util;

import edu.usc.util.DataExtractor;
import edu.usc.util.DataRecord;
import edu.usc.util.RandomGenerator;

import java.util.ArrayList;

public class RandomDataExtractor extends DataExtractor implements java.io.Serializable {
    public RandomDataExtractor() {}

    @Override
    public ArrayList<DataRecord> extractData() {
        ArrayList<DataRecord> recordsList = new ArrayList<DataRecord>(20);
        for(int i = 0; i < 20; i++) {
            String name = RandomGenerator.generateName();
            float x = RandomGenerator.generateFloat((float)0.1, (float)99999999.99);
            recordsList.add(new DataRecord(name, x));
        }
        return recordsList;
    }
}