package edu.uchicago.cs.dbp.hadoop.common.types;

import java.util.Arrays;

import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends
		Partitioner<StringArrayWritable, StringArrayWritable> {

	private int keySize;

	public KeyPartitioner() {
		this(1);
	}

	public KeyPartitioner(int keysize) {
		this.keySize = keysize;
	}

	@Override
	public int getPartition(StringArrayWritable key, StringArrayWritable value,
			int numPartitions) {
		if (numPartitions == 0)
			return 0;
		return Math.abs(Arrays.hashCode(Arrays.copyOf(key.get(), keySize))
				% numPartitions);
	}

	public static class Key2Partitioner extends KeyPartitioner {
		public Key2Partitioner() {
			super(2);
		}
	}
}
