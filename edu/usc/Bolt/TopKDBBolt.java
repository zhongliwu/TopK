package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import storm.starter.util.TopKDataRecord;
import storm.starter.util.TopKDatabaseRecorder;
import storm.starter.util.TopKFileRecorder;

import java.util.*;

public class TopKDBBolt extends BaseRichBolt {
    private PriorityQueue<TopKDataRecord> heap;
    private static final int k_size = 15;
    private TopKDatabaseRecorder recorder;
    private Set<String> set;
    private OutputCollector _collector;

    public TopKDBBolt() {}

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        heap = new PriorityQueue<TopKDataRecord>(k_size);
        recorder = TopKDatabaseRecorder.getInstance("", 0, "");
        set = new HashSet();
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        TopKDataRecord record = new TopKDataRecord(tuple.getString(0), tuple.getFloat(1));
        if(recordIsNeeded(record) && set.add(record.toString())) {
            insertRecord(record);
            ArrayList<TopKDataRecord> topList = new ArrayList<TopKDataRecord>(k_size);
            for(TopKDataRecord topKDataRecord : heap) {
                topList.add(topKDataRecord);
            }
            Collections.sort(topList, new Comparator<TopKDataRecord>() {
                @Override
                public int compare(TopKDataRecord o1, TopKDataRecord o2) {
                    return (int)(o1.getScore() - o2.getScore()) * -1;
                }
            });

            recorder.writeToDisk(topList);
            _collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "Score"));
    }

    @Override
    public void cleanup() {
        recorder.closeWrite();
    }

    private boolean recordIsNeeded(TopKDataRecord record) {
        if(heap.size() < k_size || heap.peek().getScore() < record.getScore()) {
            return true;
        } else {
            return false;
        }
    }

    private void insertRecord(TopKDataRecord record) {
        if(heap.size() < k_size) {
            heap.offer(record);
        } else {
            heap.poll();
            heap.offer(record);
        }
    }
}
