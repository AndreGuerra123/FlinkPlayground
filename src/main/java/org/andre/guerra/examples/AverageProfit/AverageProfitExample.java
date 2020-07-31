package org.andre.guerra.examples.AverageProfit;

import org.andre.guerra.examples.WordCountStream.WordCountStreamExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfitExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env
                .readTextFile(WordCountStreamExample.class.getResource("../Resources/avg").getPath());

        DataStream<MonthProfit> profits = text.map(new MonthProfitSplitter());

        DataStream<Tuple2<String,Double>> average = profits.keyBy(new MonthSelector()).reduce(new ProfitCounter()).map(new MonthAverage());
        
        average.print();

   
        env.execute("Average Profit Example");
    
  }
  
}