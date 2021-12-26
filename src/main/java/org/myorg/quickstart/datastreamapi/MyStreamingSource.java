package org.myorg.quickstart.datastreamapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * ClassName: MyStreamingSource
 * Description:实现自定义实时数据源
 * date: 2021/12/26 13:19
 *
 * @author ran
 */
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning){
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    class Item{
        private String name;
        private Integer id;
        Item(){

        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }
    }

    private Item generateItem(){
        int i = new Random().nextInt(100);
        Item item = new Item();
        item.setName("name"+i);
        item.setId(i);
        return item;
    }
}

class StreamingDemo{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MyStreamingSource.Item> text = env.addSource(new MyStreamingSource()).setParallelism(1);
        SingleOutputStreamOperator<String> mapItem = text.map(new MyMapFunction());
        SingleOutputStreamOperator<Object> flapmapItem = text.flatMap(new FlatMapFunction<MyStreamingSource.Item, Object>() {
            @Override
            public void flatMap(MyStreamingSource.Item value, Collector<Object> out) throws Exception {
                String name = value.getName();
                out.collect(name);
            }
        });
        SingleOutputStreamOperator<MyStreamingSource.Item> filterItems = text.filter(new FilterFunction<MyStreamingSource.Item>() {
            @Override
            public boolean filter(MyStreamingSource.Item value) throws Exception {
                return value.getId() % 2 == 0;
            }
        });
        flapmapItem.print().setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
    static class MyMapFunction extends RichMapFunction<MyStreamingSource.Item,String>{

        @Override
        public String map(MyStreamingSource.Item value) throws Exception {
            return value.getName();
        }
    }
}