package com.bigdata.mrprocess;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 利用reduce端的GroupingComparator来实现将一组bean看成相同的key
 * reduce数据分组功能
 * 先会注册bean然后指定对应的key，reduce才会获取对应的key
 */
public class PWritableComparator extends WritableComparator {

    //传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    //第十七步，mapper执行完，且空参构造后，进入自定义分组key,注册bean
    protected PWritableComparator() {
        //注册bean
        super(PBeanSort.class, true);
        System.out.println("===>>PWritableComparator -- PWritableComparator");
    }

    //用于写设置相同key的逻辑
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        System.out.println("===>>PWritableComparator -- compare");
        return super.compare(a, b);
    }
}
