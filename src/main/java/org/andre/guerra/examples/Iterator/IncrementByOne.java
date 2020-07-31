package org.andre.guerra.examples.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class IncrementByOne implements MapFunction<Tuple2<Long,Integer>,Tuple2<Long,Integer>>{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Long, Integer> map(Tuple2<Long, Integer> value) throws Exception {
        value.f0 += 1;
        value.f1 += 1;
        return value;
    }

}
