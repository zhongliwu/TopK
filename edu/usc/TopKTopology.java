package edu.usc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

//import edu.usc.bolt.TestBolt;
import edu.usc.bolt.TopKCalBolt;
import edu.usc.bolt.TopKDBBolt;
import edu.usc.spout.TopKDataSpout;


public class TopKTopology {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("RecordSpout", new TopKDataSpout(), 2);
        builder.setBolt("CalculateBolt", new TopKCalBolt(), 4).shuffleGrouping("RecordSpout");
        builder.setBolt("DatabaseBolt", new TopKDBBolt(), 1).shuffleGrouping("CalculateBolt");
        //builder.setBolt("TestBolt", new TestBolt(), 1).shuffleGrouping("RecordSpout");
        //builder.setBolt("TestBolt", new TestBolt(), 1).shuffleGrouping("CalculateBolt");

        Config conf = new Config();
        conf.setDebug(true);

        if(args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(5000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

    }
}
