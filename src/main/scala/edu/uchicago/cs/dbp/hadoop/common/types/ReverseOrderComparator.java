package edu.uchicago.cs.dbp.hadoop.common.types;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseOrderComparator<T extends WritableComparable<T>> extends
		WritableComparator {

	public ReverseOrderComparator(Class<T> keyClass) {
		super(keyClass, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
}
