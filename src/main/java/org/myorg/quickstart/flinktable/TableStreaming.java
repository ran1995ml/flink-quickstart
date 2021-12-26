package org.myorg.quickstart.flinktable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ClassName: TableStreaming
 * Description:对实时商品数据分流，分成even和odd两个流进行join，输出join结果
 * Projo必须是共有的，且必须有无参构造器
 * date: 2021/12/26 21:49
 *
 * @author ran
 */
public class TableStreaming {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource()).map(new MapFunction<Item, Item>() {
            @Override
            public Item map(Item value) throws Exception {
                return value;
            }
        });

        DataStream<Item> evenSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("even");

        DataStream<Item> oddSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");

//        oddSelect.printToErr();

        bsTableEnv.createTemporaryView("evenTable",evenSelect,"name,id");
        bsTableEnv.createTemporaryView("oddTable",oddSelect,"name,id");

        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable a join oddTable b on a.name = b.name");
        queryTable.printSchema();
        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer,String,Integer,String>>() {})).print();
        bsEnv.execute("streaming sql job");


    }
}
class MyStreamingSource implements SourceFunction<Item> {
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){
            Item item = generateItem();
            ctx.collect(item);

            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    private Item generateItem() {
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

