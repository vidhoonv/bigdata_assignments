package com.refactorlabs.cs378.assign2;

/**
 * Created by vidhoonv on 2/8/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordStatistics {

    /*
        Defining a LongArrayWritable class that extends ArrayWritable
        This is used to pack the output of mapper which consists of the
        word, sum of counts and sum of square of counts
     */
    public static class LongArrayWritable extends ArrayWritable{
        /*
            Defining a constructor to set the type
            of ArrayWritable

         */
        public LongArrayWritable(){
            super(LongWritable.class);
        }
        /*
            Defining a method to get the values
            as a long array from the LongArrayWritable type
            to facilitate retrieval of values for processing

         */
        public long[] getValueArray(){
            Writable[] wVals = get();
            long[] rValues = new long[wVals.length];

            for(int i=0;i<wVals.length;i++){
                rValues[i] = ((LongWritable)wVals[i]).get();
            }
            return rValues;
        }

    }
    /*
        Defining MapClass as an extension of Mapper
        Input: Takes LongWritable Key and Text Value
        Output: LongWritable key and {Text, LongWritable, LongWritable} Value - LongArrayWritable



     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable>{
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        }
    }
}
