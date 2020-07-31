package org.andre.guerra.examples.AverageProfit;

import org.apache.flink.api.common.functions.MapFunction;

public class MonthProfitSplitter implements MapFunction<String, MonthProfit>{

    /**
     *
     */
    private static final long serialVersionUID = -2032643500287725806L;

    @Override
    public MonthProfit map(String value) throws Exception {
        String[] values = value.split(",");
        return new MonthProfit(values[1],Integer.valueOf(values[4]),1);
    }
    
}
