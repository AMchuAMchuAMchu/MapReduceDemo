package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Wuyq
 * @create 2023/4/14 20:57
 */

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    private IntWritable v = new IntWritable();

    private int sum;


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        sum = 0;
        for (IntWritable value : values) {

            sum += value.get();

        }
        v.set(sum);
        context.write(key,v);

    }
}


