package com.ywf.storm;

import com.ywf.storm.bolt.WordCountBolt;
import com.ywf.storm.spout.WordCountSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 单词统计拓扑
 *
 * @Author:ywf
 */
public class WordCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // 创建拓扑
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 设置spout
        // parallelism_hint 执行线程数 setNumTasks 所有线程运行任务总数，以下配置是运行一个任务
        topologyBuilder.setSpout("wordCountSpout", new WordCountSpout(), 1)
                .setNumTasks(1);
        // 设置bolt
        // tuple随机分发给下一阶段的bolt ; parallelism_hint 执行线程数  ;
        // setNumTasks 所有线程运行任务总数，以下配置是1个线程运行2个Bolt任务
        topologyBuilder.setBolt("wordCountBolt", new WordCountBolt(), 1)
                .setNumTasks(2).shuffleGrouping("wordCountSpout");

        Config conf = new Config();
        // 本地模式
//        LocalCluster localCluster = new LocalCluster();
        // 提交拓扑
//        localCluster.submitTopology("myTopology", conf, topologyBuilder.createTopology());

        // 集群模式
        StormSubmitter.submitTopology("myTopology", conf, topologyBuilder.createTopology());
    }
}
