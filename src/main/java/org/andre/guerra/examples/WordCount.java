package org.andre.guerra.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
    private static final String INPUT = null;
    private static final String OUTPUT = null;

    public static void main(String[] args)
    throws Exception
  {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    ParameterTool params = ParameterTool.fromArgs(args);
    
    env.getConfig().setGlobalJobParameters(params);
    
    DataSet<String> text = env.readTextFile(INPUT);
    
    DataSet<String> filtered = text.filter(new NWordFilter());

    DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
    
    DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
  
    counts.writeAsCsv(OUTPUT, "\n", " ");
      
    env.execute("WordCount Example");
    
  }
  
}
