package com.ywf.storm;

import com.ywf.storm.bolt.SystemLogBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * kafka 日志kafka拓扑
 * @Author:ywf
 */
public class LogKafkaTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String zkConnStr = "node1:2181,node2:2181,node3:2181";
        BrokerHosts brokerHosts = new ZkHosts(zkConnStr);
        String topicName = "ywf-system-log";
        String zkRoot = "/ywf-system-log";
        String id = UUID.randomUUID().toString();
        /**
         * BrokerHosts hosts zk集群地址
         * String topic, 主题
         * String zkRoot, zk主题地址
         * String id 唯一标识
         */
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicName, zkRoot, id);
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        topologyBuilder.setSpout("kafkaSpout", kafkaSpout).setNumTasks(2);
        topologyBuilder.setBolt("systemLogBolt", new SystemLogBolt(),2).shuffleGrouping("kafkaSpout")
                .setNumTasks(2);

        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);
        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("logKafkaTopology", config, topologyBuilder.createTopology());
    }
}
