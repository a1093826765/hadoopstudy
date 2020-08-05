package com.bigdata.mr.flowsum;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long dFlow;

    //反序列化时，需要反射调用一个空参构造函数
    public FlowBean() {
    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
    }

    public void setAll(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dFlow);
    }

    /**
     *反序列化方法
     * 获取的顺序根据write的顺序而定
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        dFlow=dataInput.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + dFlow+"\t"+(upFlow+dFlow);
    }

    /**
     * 倒序排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        long sum=upFlow+dFlow;
        return sum>o.getdFlow()+o.getUpFlow()?-1:1;
    }
}
