package org.andre.guerra.examples.WordCount;

import org.apache.flink.api.common.functions.FilterFunction;

public final class NWordFilter implements FilterFunction<String> {

      /**
      *
      */
      private static final long serialVersionUID = -5113846584902657744L;

      @Override
      public boolean filter(String value) throws Exception {

          return value.startsWith("N");
              
    }
      
}
