package org.andre.guerra.examples.Split;

import java.util.ArrayList;
import java.util.List;

import org.andre.guerra.examples.WordCountStream.WordCountStreamExample;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitExample {
    public static void main(String[] args) throws Exception { // there is no explanation why there is an iterator at the return of select. this is confusing and is derecated use filtering instead

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env
                .readTextFile(WordCountStreamExample.class.getResource("../Resources/oddeven").getPath());

        DataStream<Integer> all = text.map(new ToInteger());
        

        // This is deprecated...use filter and anti-filter
        @SuppressWarnings("deprecation")
        SplitStream<Integer> tagged = all.split(new OutputSelector<Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> select(Integer value) {            
                final List<String> iterator = new ArrayList<String>();
                if(value%2 == 0){
                    iterator.add("even");
                }else{
                    iterator.add("odd");
                }
                return iterator; 
            }

        });
        tagged.select("even").print();


        env.execute("Stream Split Example");
    
  }
}