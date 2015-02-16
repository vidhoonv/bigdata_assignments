package com.refactorlabs.cs378.assign3;

/**
 * Created by vidhoonv on 2/10/15.
 */

/*
PENDING ITEMS:

1) sorted order of references - done but doubts
2) preprocessing improvement

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
        /*
            Two mutable Text objects created
            and instantiated for writing output
            of mapper
         */
        private Text word = new Text();
        private Text docID = new Text();

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /*
         For each line, the map method gets key (number of bytes)
         and value (the content of the line)
         From the content of the line, we extract {WORD, DOCID}
         for each unique word in the map method
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            String line = value.toString();
            /*
                words might be repeated in each document
                so collecting unique words
             */
            Set<String> uniqueWords = new HashSet<String>();
            /*
             skipping blank lines
            */
            if(line.length()<=0)
                return;

            String[] parts = line.split("\\s+",2);

            String documentID = parts[0];
            String docContent = parts[1];
            /*
                splitting doc by whitespace
             */
            String[] words = docContent.split("\\s+");
            for(String wrd : words){
                /*
                    do pre processing on word here

                 */
                wrd=wrd.toLowerCase();
                wrd=wrd.replaceAll("^\\p{Punct}+|\\p{Punct}+$", "");

                uniqueWords.add(wrd);

            }
            /*
                inserting unique words and doc ID as output
                of mapper by writing to context object
             */
            for(String wrd : uniqueWords){
                word.set(wrd);
                docID.set(documentID);
                context.write(word,docID);
                context.getCounter(MAPPER_COUNTER_GROUP, "Words Mapped out").increment(1L);
            }


        }

    }

    /*
        We define a combiner which gets input of the form
        Reduce method gets input of the form
        key:    word
        Iterable<Text>:  list of doc IDs

        We use this list of doc IDs to create a
        comma seperated list format entry for each word
        in the combiner.

        This involves sorting the list and then creating a
        compound string in CSV format done in reduce method.
     */
    public static class CombineClass extends Reducer<Text,Text,Text, Text>{

        /*
            a mutable text object to store
            the list of doc IDs for each word
         */
        private Text docsText = new Text();

        /**
         * Counter group for the combiner.  Individual counters are grouped for the combiner.
         */
        private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";

        /*
           A helper method to combine entries in a
           string arraylist into a String object
           in comma separated list format which
           can be directly merged by reducer
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
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            /*
            An arraylist of Strings to collect
            all doc ids from the Text Iterable
             */
            List<String> docList = new ArrayList<String>();
            for(Text val : values){                ;
                docList.add(val.toString());
            }
            //sort document ids in list in combiner
            Collections.sort(docList);
            /*
              calling helper method to get the doc IDs
               in the list sorted for each word
             */
            docsText.set(getDocListString(docList));
            context.write(key,docsText);
            context.getCounter(COMBINER_COUNTER_GROUP, "Unique words combined").increment(1L);

        }
    }

    /*
        We define a reduce class that gets input of the form:
        Reduce method gets input of the form
        Key:    Word
        Iterable<Text>: List of CSV of docsIDs

         The docs IDs in the CSV are already sorted.
         They are split into individual doc IDs and merged
         in reducer.
     */
    public static class ReduceClass extends Reducer<Text,Text,Text, Text> {

        /*
            a mutable text object to store
            the list of doc IDs for each word
         */
        private Text docsText = new Text();

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        /*
           A helper method to combine entries in a
           string arraylist into a String object
           in comma separated list format
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
        /*
        A helper method to merge sorted list of docs
        Something like the merge part of merge sort.
        Instead of sorting the whole thing, this is
        computationally more efficient.
         */
        public List<String> mergeDocs(List<String> docs, String[] ndocs){
            List<String> mergedDocs = new ArrayList<String>();
            int i=0;
            while(docs.isEmpty()==false && i<ndocs.length){
                if(docs.get(0).compareTo(ndocs[i])<0){
                    mergedDocs.add(docs.get(0));
                    docs.remove(0);
                }
                else{
                    mergedDocs.add(ndocs[i]);
                    i++;
                }
            }

            if(docs.isEmpty()){
                while(i<ndocs.length){
                    mergedDocs.add(ndocs[i]);
                    i++;
                }
            }
            else{
                while(docs.isEmpty()==false){
                    mergedDocs.add(docs.get(0));
                    docs.remove(0);
                }
            }
            return mergedDocs;
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            List<String> docList = new ArrayList<String>();

            for(Text val : values){
                /*
                    here we are getting list of documents from combiner
                    for each unique word key which are already sorted
                     in the sort and shuffle step. So, there is no need to
                     sort in reducer. But we need to merge these list of
                     doc IDs properly
                 */
                //String [] docs = val.toString().split(",");
                //docList = mergeDocs(docList,docs);
                docList.add(val.toString());

            }


            docsText.set(getDocListString(docList));
            context.write(key,docsText);
            context.getCounter(REDUCER_COUNTER_GROUP, "Unique Words Reduced").increment(1L);

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration confInvertedIndex = new Configuration();
        String[] appArgs = new GenericOptionsParser(confInvertedIndex, args).getRemainingArgs();

        Job job = new Job(confInvertedIndex, "InvertedIndex");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(InvertedIndex.class);

       // Set the output key and value types for Map and Reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
