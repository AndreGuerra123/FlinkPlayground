package org.andre.guerra.examples.AverageProfit;

import org.apache.flink.api.common.functions.ReduceFunction;

public class ProfitCounter implements ReduceFunction<MonthProfit>{

    /**
     *
     */
    private static final long serialVersionUID = -2250757736148114356L;

    @Override
    public MonthProfit reduce(MonthProfit value1, MonthProfit value2) throws Exception {

        return new MonthProfit(value1.month, value1.profit + value2.profit, value1.counter + value2.counter);

    }
    
}
