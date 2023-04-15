package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Wuyq
 * @create 2023/4/15 15:37
 */

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

//  输出的k:手机号码
    private Text outK = new Text();
//  输出的v:FlowBean
    private FlowBean outV = new FlowBean();


//  map进行并行计算
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
//      转为字符串
        String line = value.toString();
//      根据tab键切割
        String[] words = line.split("\t");
//      手机号
        String phone = words[1];
//      上行流量
        String upFlow = words[words.length-3];
//      下行流量
        String downFlow = words[words.length-2];
//      设置电话号码
        outK.set(phone);
//      设置上行流量
        outV.setUpFlow(Long.parseLong(upFlow));
//      设置下行流量
        outV.setDownFlow(Long.parseLong(downFlow));
//      设置总流量
        outV.setSumFlow();
//      写出
        context.write(outK,outV);
    }
}
