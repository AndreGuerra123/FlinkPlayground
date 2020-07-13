package org.andre.guerra.examples.AverageProfit;

import org.andre.guerra.examples.WordCount.NWordFilter;
import org.andre.guerra.examples.WordCount.Tokenizer;
import org.andre.guerra.examples.WordCountStream.NameKeySelector;
import org.andre.guerra.examples.WordCountStream.WordCountStreamExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfitExample
{

    public static void main(String[] args)
    throws Exception
  {
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    ParameterTool params = ParameterTool.fromArgs(args);
    
    env.getConfig().setGlobalJobParameters(params);
    
    DataStream<String> text = env.readTextFile(WordCountStreamExample.class.getResource("../resources/avg").getPath());
    
    DataStream<String> filtered = text.filter(new NWordFilter());

    DataStream<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
    
    DataStream<Tuple2<String, Integer>> counts = tokenized.keyBy(new NameKeySelector()).sum(1); //Similar to the groupBy but instead of putting on the same datastructure just creates mini streams by key.
    
    counts.print();

    env.execute("WordCountStream Example");
    
  }
  
}