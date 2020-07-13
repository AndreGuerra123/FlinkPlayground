package org.andre.guerra.examples.WordCount;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCountExample
{

    public static void main(String[] args)
    throws Exception
  {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    ParameterTool params = ParameterTool.fromArgs(args);
    
    env.getConfig().setGlobalJobParameters(params);
    
    DataSet<String> text = env.readTextFile(WordCountExample.class.getResource("./resources/wc").getPath());
    
    DataSet<String> filtered = text.filter(new NWordFilter());

    DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
    
    DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
  
    counts.writeAsText("word_count_solution");
      
    env.execute("WordCount Example");
    
  }
  
}