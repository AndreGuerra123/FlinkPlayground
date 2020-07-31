package org.andre.guerra.examples.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Is10 implements FilterFunction<Tuple2<Long,Integer>>{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(Tuple2<Long, Integer> value) throws Exception {
        return value.f0 == 10;
    }
    
}