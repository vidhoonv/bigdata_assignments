package com.refactorlabs.cs378.assign3;

/**
 * Created by vidhoonv on 2/10/15.
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
import java.util.*;

public class InvertedIndex {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text();
        private Text docID = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            String line = value.toString();

            /*
             skipping blank lines
            */
            if(line.length()<=0)
                return;

            String[] parts = line.split(" ",2);

            String documentID = parts[0];
            String docContent = parts[1];

            String[] words = docContent.split(" ");
            for(String wrd : words){
                /*
                    do preprocessing on word here

                 */
                wrd=wrd.toLowerCase();
                wrd=wrd.replaceAll("^\\p{Punct}+|\\p{Punct}+$", "");


                word.set(wrd);
                docID.set(documentID);
                context.write(word,docID);
            }

        }

    }

/*
    public static class CombineClass extends Reducer<Text,Text,Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

                Text docsList = new Text();
                List<String> docList = new ArrayList<String>();
                for(Text val : values){
                    String docid = val.toString();
                    docList.add(docid);
                }

            docsList.set(docList.toString());
            context.write(key,docsList);
        }
    }*/


    public static class ReduceClass extends Reducer<Text,Text,Text, Text> {
        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */

        public String getDocListString(List<String> docs){
            String doclist = new String("");
            if(docs.size()<=0)
                return doclist;

            doclist += docs.get(0);

            for(int i=1;i<docs.size();i++){
                doclist += "," + docs.get(i);
            }
            return doclist;
        }
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Text docsList = new Text();
            List<String> docList = new ArrayList<String>();
            for(Text val : values){
                String docid = val.toString();
                docList.add(docid);
            }

            docsList.set(getDocListString(docList));
            context.write(key,docsList);

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration confInvertedIndex = new Configuration();
        String[] appArgs = new GenericOptionsParser(confInvertedIndex, args).getRemainingArgs();

        Job job = new Job(confInvertedIndex, "InvertedIndex");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(InvertedIndex.class);


        // Set the output key and value types for Map
      //  job.setMapOutputKeyClass(Text.class);
       // job.setMapOutputValueClass(Text.class);

        // Set the output key and value types for Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the map and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
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
