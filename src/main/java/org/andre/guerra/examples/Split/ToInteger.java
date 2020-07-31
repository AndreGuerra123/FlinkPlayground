package org.andre.guerra.examples.Split;

import org.apache.flink.api.common.functions.MapFunction;

public class ToInteger implements MapFunction<String,Integer>{

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public Integer map(String value) throws Exception {
        return Integer.parseInt(value);
    }
    
}
