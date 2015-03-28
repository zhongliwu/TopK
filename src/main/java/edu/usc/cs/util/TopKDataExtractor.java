package edu.usc.cs.util;

import java.util.ArrayList;

public abstract class TopKDataExtractor implements java.io.Serializable {
    public TopKDataExtractor(){}

    public abstract ArrayList<TopKDataRecord> extractData();
}