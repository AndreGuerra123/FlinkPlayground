package org.andre.guerra.examples.Aggregation;

import org.andre.guerra.examples.AverageProfit.MonthProfit;
import org.andre.guerra.examples.AverageProfit.MonthProfitSplitter;
import org.andre.guerra.examples.WordCountStream.WordCountStreamExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class AggregationExample {
    public static void main(String[] args) throws Exception { //there are 3 agg func: sum, min, max and their By correspondents which mantain the ramian tuples information

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env
                .readTextFile(WordCountStreamExample.class.getResource("../Resources/avg").getPath());

        DataStream<MonthProfit> profits = text.map(new MonthProfitSplitter());
        
        DataStream<Tuple2<String, Integer>> min = profits.map(new MapFunction<MonthProfit,Tuple2<String,Integer>>() {

			/**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
			public Tuple2<String, Integer> map(MonthProfit value) throws Exception {
				return new Tuple2<String, Integer>(value.month,value.profit);
			}



        }).keyBy(new KeySelector<Tuple2<String,Integer>,String>() {

			/**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				
				return value.f0;
			}
        }).min(1);
        
        min.print();
   
        env.execute("Minimum Profit Example");
    
  }
}