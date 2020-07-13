package org.andre.guerra.examples.JoinTables;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class RightOuterJoiner implements JoinFunction<Tuple2<String,String>,Tuple2<String,String>,Tuple3<String,String,String>>{

    /**
     *
     */
    private static final long serialVersionUID = 4420551922421097614L;

    @Override
    public Tuple3<String, String, String> join(Tuple2<String, String> first, Tuple2<String, String> second)
            throws Exception {
        
        return new Tuple3<String,String,String>(second.f0,first == null ? "NULL" : first.f1, second.f1);
    }
    
}