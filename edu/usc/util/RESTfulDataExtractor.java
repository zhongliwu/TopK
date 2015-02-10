package edu.usc.util;

import edu.usc.util.DataExtractor;
import edu.usc.util.DataRecord;

import java.util.ArrayList;

public class RESTfulDataExtractor extends DataExtractor {
    public RESTfulDataExtractor() {}

    @Override
    public ArrayList<DataRecord> extractData() {
        ArrayList<DataRecord> recordsList = new ArrayList<DataRecord>(20);
        //RESTful APIs here

        return recordsList;
    }
}