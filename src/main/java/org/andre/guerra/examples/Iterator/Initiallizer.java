package org.andre.guerra.examples.Iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Initiallizer implements MapFunction<Long, Tuple2<Long,Integer>> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<Long, Integer> map(Long value) throws Exception {
        return new Tuple2<Long, Integer>(value,0);
    }

   

}
