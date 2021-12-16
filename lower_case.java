package com.databricks.examples;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

public class ExampleUDF extends UDF {
    // Accept a string input
    public String evaluate(String input) {
        // If the value is null, return a null
        if(input == null)
            return null;
        // Lowercase the input string and return it
        return input.toLowerCase();
    }
}
