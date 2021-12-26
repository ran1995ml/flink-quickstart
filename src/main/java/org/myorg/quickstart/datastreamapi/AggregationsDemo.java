package org.myorg.quickstart.datastreamapi;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: AggregationsDemo
 * Description:Aggregations需要指定一个key进行聚合
 * date: 2021/12/26 14:16
 *
 * @author ran
 */
public class AggregationsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(1,2,12));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,15));

        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromCollection(data);
//        source.keyBy(0).max(2).printToErr();
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> reduce = source.keyBy(0).reduce(new ReduceFunction<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) throws Exception {
                Tuple3<Integer, Integer, Integer> newTuple = new Tuple3<>();
                newTuple.setFields(0, 0, (Integer) value1.getField(2) + (Integer) value2.getField(2));
                return newTuple;
            }
        });
        reduce.printToErr().setParallelism(1);

        env.execute();
    }
}
