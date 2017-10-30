package com.neu.hive.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class ToUpperCaseUDF extends UDF{
    private Text result = new Text();

    public Text evaluate(Text str) {
        if (str == null) {
            return null;
        }
        result.set(str.toString().toUpperCase());
        return result;
    }

}
