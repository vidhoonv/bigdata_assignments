package com.refactorlabs.cs378.assign2;

/**
 * Created by vidhoonv on 2/8/15.
 */

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordStatistics {

    /*
        Defining a LongArrayWritable class that extends ArrayWritable
        This is used to pack the output of mapper which consists of the
        word, sum of counts and sum of square of counts
     */
    public static class LongArrayWritable extends ArrayWritable{
        /*
            a public member to hold size
         */
       // public Long sz = new Long();
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

        /*
            Defining a method to set the values
            of LongArrayWritable type

         */

        public void setValueArray(long[] vArr){
            Writable[] wVals = new Writable[vArr.length];
           // sz = new Long(vArr.length);
            LongWritable tempVal;
            for(int i=0;i<vArr.length;i++){
                tempVal = new LongWritable(vArr[i]);
                wVals[i] = (Writable)tempVal;
            }

            set(wVals);
        }

    };



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

            /*
                process the input line
             */
            Map<String,Long> wordCount = new HashMap<String,Long>();
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();

                /*
                    insert all preprocessing of punctuations logic here
                 */
                if(wordCount.containsKey(token) == false){
                    wordCount.put(token,1L);
                }
                else{
                    wordCount.put(token,wordCount.get(token)+1);
                }

                context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            }

            /*
                create mapper output
             */
            long[] vArr = new long[2];
            for(String k : wordCount.keySet()){

                vArr[0] = wordCount.get(k);
                vArr[1] = vArr[0]*vArr[0];

                LongArrayWritable mOutput = new LongArrayWritable();
                mOutput.setValueArray(vArr);

                word.set(k);
                context.write(word, mOutput);
            }
        }
    }


}
