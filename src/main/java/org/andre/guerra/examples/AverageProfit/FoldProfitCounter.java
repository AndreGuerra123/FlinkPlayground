package org.andre.guerra.examples.AverageProfit;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings( "deprecation" )
public class FoldProfitCounter implements FoldFunction<MonthProfit,Tuple3<String,Integer, Integer>>{

    /**
	 *
	 */
	private static final long serialVersionUID = 2088212944887219064L;

	@Override
    public Tuple3<String, Integer, Integer> fold(Tuple3<String, Integer, Integer> accumulator, MonthProfit value) throws Exception {

        return new Tuple3<String,Integer, Integer>(value.month, accumulator.f1+1, accumulator.f2 + value.profit);

    }
    
}
