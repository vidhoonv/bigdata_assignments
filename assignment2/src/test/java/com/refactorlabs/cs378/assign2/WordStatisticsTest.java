package com.refactorlabs.cs378.assign2;

/**
 * Created by vidhoonv on 2/8/15.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordStatisticsTest {
    MapDriver<LongWritable, Text, Text, WordStatistics.LongArrayWritable> mapDriver;
    ReduceDriver<Text,WordStatistics.LongArrayWritable,Text, WordStatistics.DoubleArrayWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, WordStatistics.LongArrayWritable,Text, WordStatistics.DoubleArrayWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordStatistics.MapClass mapper = new WordStatistics.MapClass();
        WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = mapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "Hello World Hello \nWorld Hello World Hello\n"));
        WordStatistics.LongArrayWritable x = new WordStatistics.LongArrayWritable();
        long[] vArr = new long[2];


        vArr[0]=2;
        vArr[1]=4;
        x.setValueArray(vArr);
        mapDriver.withOutput(new Text("hello"), x);

        vArr[0]=1;
        vArr[1]=1;
        x.setValueArray(vArr);
        mapDriver.withOutput(new Text("world"), x);

        vArr[0]=2;
        vArr[1]=4;
        x.setValueArray(vArr);
        mapDriver.withOutput(new Text("hello"), x);

        vArr[0]=2;
        vArr[1]=4;
        x.setValueArray(vArr);
        mapDriver.withOutput(new Text("world"), x);

        mapDriver.runTest();
    }
/*
    @Test
    public void testReducer() throws IOException {
        List<WordStatistics.LongArrayWritable> values = new ArrayList<WordStatistics.LongArrayWritable>();

        WordStatistics.LongArrayWritable x1 = new WordStatistics.LongArrayWritable();
        long[] vArr = new long[2];
        vArr[0]=4;
        vArr[1]=16;
        x1.setValueArray(vArr);

        WordStatistics.LongArrayWritable x2 = new WordStatistics.LongArrayWritable();
        vArr[0]=3;
        vArr[1]=9;
        x2.setValueArray(vArr);

        values.add(x1);
        values.add(x2);

        WordStatistics.DoubleArrayWritable y = new WordStatistics.DoubleArrayWritable();
        double[] dArr = new double[3];
        dArr[0]=2;
        dArr[1]=3.5;
        dArr[2]=0.25;
        y.setValueArray(dArr);


        reduceDriver.withInput(new Text("hello"), values);
        reduceDriver.withOutput(new Text("hello"), y);
        reduceDriver.runTest();
    }


    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "Hello World Hello World Hello World Hello\n"));

        mapReduceDriver.withInput(new LongWritable(), new Text(
                "Hello World Hello World Hello World Hello\n"));

        mapReduceDriver.withInput(new LongWritable(), new Text(
                "Hello Hello Hello Hello Hello Hello Hello\n"));

        WordStatistics.DoubleArrayWritable y1 = new WordStatistics.DoubleArrayWritable();
        double[] dArr = new double[3];
        dArr[0]=3;
        dArr[1]=5;
        dArr[2]=2;
        y1.setValueArray(dArr);

        WordStatistics.DoubleArrayWritable y2 = new WordStatistics.DoubleArrayWritable();

        dArr[0]=2;
        dArr[1]=3;
        dArr[2]=0;
        y2.setValueArray(dArr);


        mapReduceDriver.withOutput(new Text("hello"), y1);
        mapReduceDriver.withOutput(new Text("world"), y2);

        mapReduceDriver.runTest();
    }
    */
}