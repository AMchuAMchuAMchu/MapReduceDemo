package com.atguigu.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Wuyq
 * @create 2023/4/14 20:37
 */

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text k = new Text();

    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
           k.set(word);
           context.write(k,v);
        }

    }
}
