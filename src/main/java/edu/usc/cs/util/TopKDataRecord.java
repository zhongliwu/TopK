package edu.usc.cs.util;

public class TopKDataRecord implements Comparable<TopKDataRecord> {
    private String name;
    private float score;

    public TopKDataRecord() {
        name = "";
        score = 0.0f;
    }

    public TopKDataRecord(String n, float s) {
        name = n;
        score = s;
    }

    public TopKDataRecord(String n, String s) {
        name = n;
        score = Float.parseFloat(s);
    }

    public TopKDataRecord(TopKDataRecord d) {
        name = new StringBuilder(d.getName()).toString();
        score = d.getScore();
    }

    public float getScore() {
        return score;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return name + " " + Float.toString(score);
    }

    @Override
    public int compareTo(TopKDataRecord topKDataRecord) {
        if(this.score > topKDataRecord.score) {
            return 1;
        } else if(this.score < topKDataRecord.score) {
            return -1;
        } else {
            return 0;
        }
    }
}
