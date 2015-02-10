package edu.usc.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import edu.usc.util.DataExtractor;
import edu.usc.util.DataRecord;
import edu.usc.util.RandomDataExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TopKDataSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private DataExtractor extractor;
    private ArrayList<DataRecord> recordsList;
    private Long tupleCount;
    private Long failure;
    private Map<Long, DataRecord> tupleMap;

    private static final long MAX_FAILURE = 1000000000L;

    public TopKDataSpout() {}

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        extractor = new RandomDataExtractor();
        recordsList = new ArrayList<DataRecord>(30);
        tupleMap = new HashMap<Long, DataRecord>(100);

        tupleCount = 1L;
        failure = 1L;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(200);
        recordsList.addAll(extractor.extractData());
        for(DataRecord record : recordsList) {
            _collector.emit(new Values(record.getName(), record.getScore()), tupleCount);
            tupleCount++;
            tupleMap.put(tupleCount, new DataRecord(record));
        }
        recordsList.clear();
    }

    @Override
    public void ack(Object msgId) {
        tupleMap.remove((Long)msgId);
    }

    @Override
    public void fail(Object msgId) {
        failure++;
        Long id = (Long)msgId;
        if(failure > MAX_FAILURE) {
            throw new RuntimeException("Too many tuple failures happened! Please check the internet!");
        } else {
            recordsList.add(tupleMap.get(id));
            tupleMap.remove(id);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "Score"));
    }
}
