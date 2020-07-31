package org.andre.guerra.examples.AverageProfit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MonthAverage implements MapFunction<MonthProfit,Tuple2<String,Double>>{

    /**
     *
     */
    private static final long serialVersionUID = 2668270393318167421L;

    @Override
    public Tuple2<String, Double> map(MonthProfit value) throws Exception {
       
        return new Tuple2<String,Double>(value.month, (double) value.profit / (double) value.counter);
    }
    
    
}
