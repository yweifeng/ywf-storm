package com.ywf.storm.spout;

import com.ywf.YwfStormApplication;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 字母统计Spout 要开启ack机制，必须设置ack>0，并且传递消息时，带上msgId
 *
 * @Author:ywf
 */
public class WordCountSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    /**
     * 单词队列
     */
    private Queue<String> wordQueue = new ConcurrentLinkedDeque<>();

    /**
     * 所有发送的集合
     */
    private Map<String, Object> allMap = new ConcurrentHashMap<>();

    /**
     * 发送成功的集合
     */
    private Map<String, Object> successMap = new ConcurrentHashMap<>();

    /**
     * 发送失败的集合
     */
    private Map<String, Object> failMap = new ConcurrentHashMap<>();

    /**
     * 初始化数据源
     */
    private void initWordQueue() {
        this.wordQueue.add("ywf");
        this.wordQueue.add("yang");
        this.wordQueue.add("wyp");
        this.wordQueue.add("ywf");
        this.wordQueue.add("wyp");
        this.wordQueue.add("ywf");
        this.wordQueue.add("wyp");
        this.wordQueue.add("ywf");
        this.wordQueue.add("ywf");
        this.wordQueue.add("ywf");
    }

    /**
     * 在任务集群的工作进程内被初始化,提供spout执行所需要的环境
     *
     * @param conf                 是这个spout的storm配置,提供给拓扑与这台主机上的集群配置一起合并
     * @param topologyContext      主要用来获取这个任务在拓扑中的位置信息,包括该任务的id,该任务的组件id,输入和输出消息等
     * @param spoutOutputCollector collector是收集器,用于从spout发送用,
     *                             收集器是线程安全的,应该作为这个spout对象的实例变量进行保存。
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 初始化数据
        initWordQueue();
        // 启动spring容器
        YwfStormApplication.run();
        this.collector = spoutOutputCollector;

    }

    /**
     * nextTuple()方法是Spout实现的核心。 也就是主要执行方法，用于输出信息,通过collector.emit方法发射。
     */
    @Override
    public void nextTuple() {

        while (!wordQueue.isEmpty()) {
            // 移除并取出队列头部元素，如果不存在返回null
            String word = wordQueue.poll();
            if (Optional.ofNullable(word).isPresent()) {
                // 设置key
                String key = UUID.randomUUID().toString().replace("-", "");

                // 存储发送记录
                allMap.put(key, word);
                // 发射消息
                this.collector.emit(new Values(word), key);
            }
        }

    }

    /**
     * 用于声明数据格式。 即输出的一个Tuple中，包含几个字段。
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields("word");
        declarer.declare(fields);
    }

    /**
     * 当一个Tuple处理成功时，会调用这个方法 param obj emit方法中的msgId
     *
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        System.out.println("msgId = [" + msgId + "] word =[" + allMap.get(msgId) + "] 发送成功");
        successMap.put(String.valueOf(msgId), allMap.get(msgId));
    }

    /**
     * 当一个Tuple处理失败时，会调用这个方法
     *
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        Object word = allMap.get(msgId);
        System.out.println("msgId = [" + msgId + "] word =[" + word + "] 发送失败");
        failMap.put(String.valueOf(msgId), allMap.get(msgId));
        // 再重发一次
        if (Optional.ofNullable(word).isPresent()) {
            collector.emit(new Values(word));
        }
    }

    @Override
    public void close() {
        System.out.println("关闭...");
    }
}
