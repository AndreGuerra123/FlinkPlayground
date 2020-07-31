package org.andre.guerra.examples.AverageProfit;

import org.andre.guerra.examples.WordCountStream.WordCountStreamExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfitFoldExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env
                .readTextFile(WordCountStreamExample.class.getResource("../Resources/avg").getPath());

        DataStream<MonthProfit> profits = text.map(new MonthProfitSplitter());

        @SuppressWarnings( "deprecation" )
        DataStream<Tuple2<String,Double>> average = profits.keyBy(new MonthSelector()).fold(new Tuple3<String,Integer,Integer>("", 0, 0),new FoldProfitCounter()).map(new MapFunction<Tuple3<String,Integer,Integer>,Tuple2<String,Double>>(){

            /**
             *
             */
            private static final long serialVersionUID = -3758367618908926425L;

            @Override
            public Tuple2<String,Double> map(Tuple3<String,Integer,Integer> value) throws Exception {
                return new Tuple2<String,Double>(value.f0, ((double) value.f2 / (double) value.f1));
            }
            
        });
        
        average.print();

   
        env.execute("Average Profit Example");
    
  }
  
}