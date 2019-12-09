package com.ywf.storm.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author:ywf
 */
public class SystemLogBolt extends BaseRichBolt {
    private OutputCollector collector;

    /**
     * 记录每个用户接口访问量
     */
    private Map<String, Integer> userMethodMap = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // !!! 一定要用bytes 上游kafkaspout 传递过来的数据
        byte[] bytes = tuple.getBinaryByField("bytes");
        String value = new String(bytes);
        JSONObject jsonObject = JSON.parseObject(value);
        String paramsStr = jsonObject.getString("params");
        JSONObject params = JSONObject.parseObject(paramsStr);
        String username = params.getString("username");

        int count = userMethodMap.getOrDefault(username, 0) + 1;
        userMethodMap.put(username, count);

        System.out.println(userMethodMap);
        // ack
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
