/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//监听本地9000端口
		DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");
		DataStream<WordCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordCount>() {
			@Override
			public void flatMap(String value, Collector<WordCount> out) throws Exception {
				for(String word:value.split("\\s")){
					out.collect(new WordCount(word,1));
				}
			}
		}).keyBy("word").timeWindow(Time.seconds(5),Time.seconds(1)).reduce(new ReduceFunction<WordCount>() {
			@Override
			public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
				return new WordCount(value1.word,value1.count+value2.count);
			}
		});
		windowCounts.print().setParallelism(1);
		env.execute("Socket Window WordCount");

	}

	public static class WordCount{
		public String word;
		public long count;

		public WordCount() {
		}

		public WordCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + ":" + count;
		}
	}
}
