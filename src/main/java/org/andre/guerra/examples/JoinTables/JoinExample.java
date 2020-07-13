package org.andre.guerra.examples.JoinTables;

import org.andre.guerra.examples.Join.RightOuterJoiner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class JoinExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> persons = env.readTextFile(JoinExample.class.getResource("../resources/persons").getPath());

        DataSet<Tuple2<String, String>> personSet = persons.map(new CommaSplitter());

        DataSet<String> locations = env
                .readTextFile(JoinExample.class.getResource("../resources/locations").getPath());

        DataSet<Tuple2<String, String>> locationSet = locations.map(new CommaSplitter());

        DataSet<Tuple3<String,String,String>> innerJoin = personSet
                .join(locationSet).where(0).equalTo(0).with(new InnerJoiner());


        DataSet<Tuple3<String,String,String>> leftOuterJoin = personSet
                .leftOuterJoin(locationSet).where(0).equalTo(0).with(new LeftOuterJoiner());


        DataSet<Tuple3<String,String,String>> rightOuterJoin = personSet
                .rightOuterJoin(locationSet).where(0).equalTo(0).with(new RightOuterJoiner());


        DataSet<Tuple3<String,String,String>> outerJoin = personSet
                .fullOuterJoin(locationSet).where(0).equalTo(0).with(new FullOuterJoiner());

        innerJoin.writeAsText("inner_join_solution");

        leftOuterJoin.writeAsText("left_outer_join_solution");

        rightOuterJoin.writeAsText("right_outer_join_solution");
        
        outerJoin.writeAsText("outer_join_solution");

        env.execute("Join Example");
    
  }
  
}
