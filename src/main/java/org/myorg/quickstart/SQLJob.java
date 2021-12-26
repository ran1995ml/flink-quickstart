package org.myorg.quickstart;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


import java.util.ArrayList;
import java.util.List;


/**
 * ClassName: SQLJob
 * Description:Flink SQL实现WordCount
 * date: 2021/12/15 23:07
 *
 * @author ran
 */
public class SQLJob {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        String words = "Hello Flink";

        List<WordCount> list = new ArrayList<>();
        for(String word:words.split("\\W+")){
            WordCount wc = new WordCount(word,1);
            list.add(wc);
        }
        DataSet<WordCount> input = fbEnv.fromCollection(list);
        //转SQL，指定表字段
        Table table = fbTableEnv.fromDataSet(input, "word,frequency");
        table.printSchema();
        //注册成表
        fbTableEnv.createTemporaryView("WordCount",table);

        Table table1 = fbTableEnv.sqlQuery("select word as word,sum(frequency) as frequency from WordCount group by word");
        DataSet<WordCount> wordCountDataSet = fbTableEnv.toDataSet(table1, WordCount.class);
        wordCountDataSet.printToErr();

    }
    public static class WordCount{
        public String word;
        public long frequency;

        public WordCount(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public WordCount() {
        }

        @Override
        public String toString() {
            return word + ":" + frequency;
        }
    }
}
