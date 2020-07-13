package org.andre.guerra.examples.WordCount;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
  
      /**
      *
      */
      private static final long serialVersionUID = 2082493156363222455L;

      public Tuple2<String, Integer> map(String value)
    {
      return new Tuple2<String,Integer>(value,1);
    }
  }