package com.bigdata.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * map执行后就根据分区放到对应的位置
 * 用于分区，设置分区数量等于reduce task数量一致
 */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
		//相同id的订单bean，会发往相同的partition
		//而且，产生的分区数，是会跟用户设置的reduce task数保持一致
		return (bean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		
	}

}
