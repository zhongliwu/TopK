package edu.usc.util;

import java.util.ArrayList;

public abstract class TopKRecorder {
    public TopKRecorder() {}

    public abstract void writeToDisk(ArrayList<DataRecord> topList);

    public abstract void closeWrite();
}
