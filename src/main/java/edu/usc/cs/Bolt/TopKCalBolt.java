package edu.usc.cs.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.PriorityQueue;

import edu.usc.cs.util.TopKDataRecord;

import org.apache.log4j.Logger;

public class TopKCalBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private static final int k_size = 15;
    private static Logger log = Logger.getLogger(TopKCalBolt.class);
    private PriorityQueue<TopKDataRecord> heap;

    public TopKCalBolt() {}

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        heap = new PriorityQueue<TopKDataRecord>(k_size);
        log.info("LOG_INFO: TopKCalBolt class succeeds to prepare!");
    }

    @Override
    public void execute(Tuple anchor) {
        TopKDataRecord record = new TopKDataRecord(anchor.getString(0), anchor.getFloat(1));
        if(recordIsNeeded(record)) {
            insertRecord(record);
            for(TopKDataRecord r : heap) {
                _collector.emit(anchor, new Values(r.getName(), r.getScore()));
                _collector.ack(anchor);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "Score"));
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
