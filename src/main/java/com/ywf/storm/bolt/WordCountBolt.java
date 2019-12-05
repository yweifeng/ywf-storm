package com.ywf.storm.bolt;

import com.ywf.YwfStormApplication;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统计每个单词出现的次数
 *
 * @Author:ywf
 */
public class WordCountBolt extends BaseBasicBolt {

    /**
     * 单词频次map
     */
    private Map<String, Integer> wordCountMap = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // 启动spring容器
        YwfStormApplication.run();
        super.prepare(stormConf, context);
    }

    /**
     * 执行业务逻辑
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 获取单词
        String word = tuple.getStringByField("word");
        int count = wordCountMap.getOrDefault(word, 0) + 1;
        wordCountMap.put(word, count);
        System.out.println("线程：" + Thread.currentThread().getName() + ", 单词：" + word + " 出现了 " + count + "次");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
