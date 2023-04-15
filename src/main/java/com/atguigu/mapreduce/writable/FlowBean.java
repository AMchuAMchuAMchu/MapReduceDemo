package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Wuyq
 * @create 2023/4/15 15:21
 */

public class FlowBean implements Writable {

//  上行流量
    private long upFlow;
//  下行流量
    private long downFlow;
//  总流量
    private long sumFlow;

    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //  总流量的计算
    public void setSumFlow() {
        this.sumFlow = upFlow + downFlow;
    }

//  重写输出方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

//  重写读的方法,但是的话需要注意的是:写入和输出的顺序需要保持一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

//  打印信息,需要按照要求的格式进行打印
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
