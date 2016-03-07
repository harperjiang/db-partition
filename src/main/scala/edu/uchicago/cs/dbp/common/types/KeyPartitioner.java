package edu.uchicago.cs.dbp.common.types;

import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends
		Partitioner<StringArrayWritable, StringArrayWritable> {
	@Override
	public int getPartition(StringArrayWritable key, StringArrayWritable value,
			int numPartitions) {
		if (numPartitions == 0)
			return 0;
		return Math.abs(key.get()[0].toString().hashCode() % numPartitions);
	}
}
