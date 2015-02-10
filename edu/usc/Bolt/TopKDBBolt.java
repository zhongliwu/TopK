package edu.usc.bolt;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import backtype.storm.utils.Utils;
import edu.usc.util.DataRecord;
import edu.usc.util.TopKDatabaseRecorder;
import edu.usc.util.TopKRecorder;
import edu.usc.util.TopKFileRecorder;

import java.util.*;

public class TopKDBBolt extends BaseRichBolt {
    private PriorityQueue<DataRecord> heap;
    private static final int k_size = 15;
    private TopKRecorder recorder;
    private Set<String> set;
    private OutputCollector _collector;

    public TopKDBBolt() {}

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        heap = new PriorityQueue<DataRecord>(k_size);
        recorder = new TopKDatabaseRecorder();
        set = new HashSet();
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        DataRecord record = new DataRecord(tuple.getString(0), tuple.getFloat(1));
        if(recordIsNeeded(record) && set.add(record.toString())) {
            insertRecord(record);
            ArrayList<DataRecord> topList = new ArrayList<DataRecord>(k_size);
            for(DataRecord dataRecord : heap) {
                topList.add(dataRecord);
            }
            Collections.sort(topList, new Comparator<DataRecord>() {
                @Override
                public int compare(DataRecord o1, DataRecord o2) {
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

    private boolean recordIsNeeded(DataRecord record) {
        if(heap.size() < k_size || heap.peek().getScore() < record.getScore()) {
            return true;
        } else {
            return false;
        }
    }

    private void insertRecord(DataRecord record) {
        if(heap.size() < k_size) {
            heap.offer(record);
        } else {
            heap.poll();
            heap.offer(record);
        }
    }
}
