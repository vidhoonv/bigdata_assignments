package com.refactorlabs.cs378.assign2;

/**
 * Created by vidhoonv on 2/8/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
        Defining a DoubleArrayWritable class that extends ArrayWritable
        This is used to pack the output of reducer which consists of the
        word, mean and variance of counts
     */
    public static class DoubleArrayWritable extends ArrayWritable{
         /*
            Defining a constructor to set the type
            of ArrayWritable

         */
        public DoubleArrayWritable(){
            super(DoubleWritable.class);
        }
        /*
            Defining a method to set the values
            of DoubleArrayWritable type

         */

        public void setValueArray(double[] vArr){
            Writable[] wVals = new Writable[vArr.length];
            // sz = new Long(vArr.length);
            DoubleWritable tempVal;
            for(int i=0;i<vArr.length;i++){
                tempVal = new DoubleWritable(vArr[i]);
                wVals[i] = (Writable)tempVal;
            }

            set(wVals);
        }

        /*
            override toStrings method to tell how to display the output
            from DoubleArrayWritable

         */

        public String[] toStrings(){
            Writable[] wVals = get();
            String[] outArr = new String[wVals.length];

            for(int i=0;i<wVals.length;i++){
                outArr[i] = ((DoubleWritable)wVals[i]).toString();
            }

            return outArr;
        }

        public String toString(){
            Writable[] wVals = get();
           // String[] outArr = new String[wVals.length];
            String outText = new String("");
            for(int i=0;i<wVals.length;i++){
                outText += ((DoubleWritable)wVals[i]).toString();
                outText += "\t";
            }

            outText.trim();

            return outText;
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

    public static class ReduceClass extends Reducer<Text,LongArrayWritable,Text, DoubleArrayWritable> {
        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {


            context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);


            double[] res = new double[2]; //0 - mean 1- variance
            int count = 0;
            for(LongArrayWritable lArr : values){
                long[] vals = lArr.getValueArray();
                res[0] += vals[0]; //summing count of word occurrences
                res[1] += vals[1]; //summing count of square of word occurrences
                count++;
            }

            res[0] /= count; //computes mean for the word
            double var = 0.0;
            var = (res[1]/count) - (res[0]*res[0]); //computes variance for the given word
            res[1] = var;

            DoubleArrayWritable redOutput = new DoubleArrayWritable();
            redOutput.setValueArray(res);
            context.write(key, redOutput);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration confWordStatistics = new Configuration();
        String[] appArgs = new GenericOptionsParser(confWordStatistics, args).getRemainingArgs();

        Job job = new Job(confWordStatistics, "WordStatistics");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);


        // Set the output key and value types for Map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongArrayWritable.class);

        // Set the output key and value types for Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleArrayWritable.class);

        // Set the map and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);


    }
}
