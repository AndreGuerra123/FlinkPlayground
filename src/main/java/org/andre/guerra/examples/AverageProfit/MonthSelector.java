package org.andre.guerra.examples.AverageProfit;

import org.apache.flink.api.java.functions.KeySelector;

public class MonthSelector implements KeySelector<MonthProfit, String>{

    /**
     *
     */
    private static final long serialVersionUID = 1357521016457548376L;

    @Override
    public String getKey(MonthProfit value) throws Exception {
        return value.month;
    }
    
}
