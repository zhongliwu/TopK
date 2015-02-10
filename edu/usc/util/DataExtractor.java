package edu.usc.util;

import java.util.ArrayList;

public abstract class DataExtractor implements java.io.Serializable {
    public DataExtractor(){}

    public abstract ArrayList<DataRecord> extractData();
}