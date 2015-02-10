package storm.starter.util;

public class DataRecord implements Comparable<DataRecord> {
    private String name;
    private float score;

    public DataRecord() {
        name = "";
        score = 0.0f;
    }

    public DataRecord(String n, float s) {
        name = n;
        score = s;
    }

    public DataRecord(String n, String s) {
        name = n;
        score = Float.parseFloat(s);
    }

    public DataRecord(DataRecord d) {
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
    public int compareTo(DataRecord dataRecord) {
        if(this.score > dataRecord.score) {
            return 1;
        } else if(this.score < dataRecord.score) {
            return -1;
        } else {
            return 0;
        }
    }
}
