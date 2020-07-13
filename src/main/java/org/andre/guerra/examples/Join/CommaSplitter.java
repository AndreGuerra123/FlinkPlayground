package org.andre.guerra.examples.Join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CommaSplitter implements MapFunction<String,Tuple2<String,String>>{

    /**
     *
     */
    private static final long serialVersionUID = 2903121183071521219L;

    @Override
    public Tuple2<String, String> map(String value) throws Exception {
        String[] splitted = value.split(",");
        return new Tuple2<String,String>(splitted[0], splitted[1]);
    }


 
    
}