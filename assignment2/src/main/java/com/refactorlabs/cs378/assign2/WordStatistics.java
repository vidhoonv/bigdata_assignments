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

        /*

            methods needed for tests
         */
        public String toString(){
            Writable[] wVals = get();
            // String[] outArr = new String[wVals.length];
            String outText = new String("");
            for(int i=0;i<wVals.length;i++){
                outText += ((LongWritable)wVals[i]).toString();
                outText += ",";
            }

            outText.trim();
            if(outText.endsWith(",")){
                outText=outText.substring(0,outText.length()-1);
            }

            return outText;
        }

        @Override
        public boolean equals(Object obj) {
            String arg1str = toString();
            String arg2str = obj.toString();

            if(arg1str.equals(arg2str)){
                return true;
            }
            return false;
        }



        @Override
        public int hashCode() {
            return super.hashCode();
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

        public String toString(){
            Writable[] wVals = get();
           // String[] outArr = new String[wVals.length];
            String outText = new String("");
            for(int i=0;i<wVals.length;i++){
                outText += ((DoubleWritable)wVals[i]).toString();
                outText += ",";
            }

            outText.trim();
            if(outText.endsWith(",")){
                outText=outText.substring(0,outText.length()-1);
            }

            return outText;
        }

        /*
            methods required for testing
         */

        public boolean equals(Object obj) {
            String arg1str = toString();
            String arg2str = obj.toString();

            if(arg1str.equals(arg2str)){
                return true;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

    };


    /*
        Defining MapClass as an extension of Mapper
        Input: Takes LongWritable Key and Text Value
        Output: LongWritable key and {LongWritable, LongWritable} Value - LongArrayWritable

        The Mapper takes input and processes paragraph by paragraph.
        For words in each para, it accumulates the frequency of occurrence in a hashmap
        and flushes the accumulated data into LongArrayWritable (number of occurrences and
        square of number of occurrences).

     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable>{
        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
        private Text word = new Text();


        public String[] preProcessToken(String token){

            String[] resTokens;

            if(token.contains("--")){
                //special case in dataset
                resTokens = token.split("--");
            }
            else{
                resTokens = new String[1];
                resTokens[0] = token;
            }
            int i=0;
            for(String tok : resTokens){
                //special case in dataset
                if(tok.endsWith("]") && tok.contains("[")){
                    int sIndex = tok.indexOf("[");
                    tok = tok.substring(0,sIndex);
                }
                //convert to lower case
                tok=tok.toLowerCase();
                //remove all punctuations from beginning and end of text
                tok=tok.replaceAll("^\\p{Punct}+|\\p{Punct}+$", "");

                resTokens[i] = tok;
                i++;

            }

            return resTokens;

        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String linesContent = value.toString();
            //split multiple lines (paras)
            String[] lines= linesContent.split("\n");

            //process one para after another
            for(String line : lines){
                StringTokenizer tokenizer = new StringTokenizer(line);
            /*
                process the input line
             */
                Map<String,Long> wordCount = new HashMap<String,Long>();
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();

                /*
                    do all preprocessing steps on token
                 */

                    String[] tokens = preProcessToken(token);

                    for(String tok : tokens){
                        if(wordCount.containsKey(tok) == false){
                            wordCount.put(tok,1L);
                        }
                        else{
                            wordCount.put(tok,wordCount.get(tok)+1);
                        }

                        context.getCounter(MAPPER_COUNTER_GROUP, "Tokens Mapped out").increment(1L);
                    }

                }

            /*
                create mapper output for each paragraph
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
                //flush hash map after processing each paragraph of text
                wordCount.clear();

            }

        }
    }
    /*
           Defining CombineClass as an extension of Reducer
           Input: Takes Text Key and LongArrayWritable value
           Output: Text key and {LongWritable, LongWritable, LongWritable} Value - LongArrayWritable

           The purpose of combiner is to sum up counts and square of counts of same words
           occuring in different paragraphs and also to count the number of paragraphs in which
           the word has occured.



        */
    public static class CombineClass extends Reducer<Text,LongArrayWritable,Text, LongArrayWritable>{
        /**
         * Counter group for the combiner.  Individual counters are grouped for the combiner.
         */
        private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";

        @Override
        public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            long[] rArr = new long[3];
            rArr[0] = 0;
            rArr[1] = 0;
            rArr[2] = 0;

            long count = 0;
            for(LongArrayWritable lArr : values){
                long[] vArr = lArr.getValueArray();
                rArr[1] += vArr[0];
                rArr[2] += vArr[1];
                count++;
            }

            rArr[0] = count;

            LongArrayWritable cOutput = new LongArrayWritable();
            cOutput.setValueArray(rArr);
            context.write(key,cOutput);

            context.getCounter(COMBINER_COUNTER_GROUP, "Unique Words Combined").increment(1L);
        }
    }

     /*
        Defining ReduceClass as an extension of Reducer
        Input: Takes Text Key and LongArrayWritable Value
        Output: Text key and {Count, Mean, Variance} Value - DoubleArrayWritable

        The reducer sums up the number of occurrences and square of number of occurrences
        for each word and also tracks the number of paragraphs in which each word has
        occurred. This is used to calculate the required mean, variance and paragraph
        count of each word for the given data.

     */

    public static class ReduceClass extends Reducer<Text,LongArrayWritable,Text, DoubleArrayWritable> {
        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
        @Override
        public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            double[] res = new double[3]; //0- para count, 1 - mean, 2- variance
            res[0]=0.0;
            res[1]=0.0;
            res[2]=0.0;

            double count = 0;
            for(LongArrayWritable lArr : values){
                long[] vals = lArr.getValueArray();
                res[1] += vals[0]; //summing count of word occurrences
                res[2] += vals[1]; //summing count of square of word occurrences
                count++;
                //System.out.println("DEBUG: "+key+",  "+count+","+vals[0]+","+vals[1]);
            }
            res[0] = count;
            res[1] /= count; //computes mean for the word
            double var = 0.0;
            var = (res[2]/count) - (res[1]*res[1]); //computes variance for the given word
            res[2] = var;

            DoubleArrayWritable redOutput = new DoubleArrayWritable();
            redOutput.setValueArray(res);
            context.write(key, redOutput);

            context.getCounter(REDUCER_COUNTER_GROUP, "Unique Words Reduced").increment(1L);
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
        job.setCombinerClass(CombineClass.class);
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
