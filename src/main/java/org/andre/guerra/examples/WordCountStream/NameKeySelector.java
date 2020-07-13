package org.andre.guerra.examples.WordCountStream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class NameKeySelector implements KeySelector<Tuple2<String, Integer>,String>{

        /**
         *
         */
        private static final long serialVersionUID = -3560134098031557400L;

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }

}