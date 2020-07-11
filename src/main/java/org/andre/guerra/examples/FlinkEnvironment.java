package org.andre.guerra.examples;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class FlinkEnvironment {
    public static void main(String... argvs) {
        
        //This gets the execution environment (there are different ways to set uo execution environments)
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //This gets the parameters (there are different ways to input parameters)
        final ParameterTool params = ParameterTool.fromArgs(argvs);

        //This sets the parameters at the job level, in each node.
        env.getConfig().setGlobalJobParameters(params);

    }
}
