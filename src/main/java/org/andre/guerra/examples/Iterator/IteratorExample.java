package org.andre.guerra.examples.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IteratorExample {
    public static void main(String[] args) throws Exception { // there are 3 agg func: sum, min, max and their By
                                                              // correspondents which mantain the ramian tuples
                                                              // information

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Integer>> init = env.generateSequence(0, 5).map(new Initiallizer());

        IterativeStream<Tuple2<Long, Integer>> iterativeStream = init.iterate();

        iterativeStream.closeWith(iterativeStream.filter(new IsNot10()).map(new IncrementByOne()));
    
        iterativeStream.filter(new Is10()).print();
   
        env.execute("Iterator Example");
    
  }
}