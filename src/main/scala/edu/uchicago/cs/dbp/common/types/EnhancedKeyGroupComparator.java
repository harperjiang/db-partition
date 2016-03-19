package edu.uchicago.cs.dbp.common.types;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class EnhancedKeyGroupComparator extends WritableComparator {

	private Boolean[] orders;

	public EnhancedKeyGroupComparator(Boolean[] orders) {
		super(StringArrayWritable.class, true);
		this.orders = orders;
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringArrayWritable sa = (StringArrayWritable) a;
		StringArrayWritable sb = (StringArrayWritable) b;

		Writable[] partsa = sa.get();
		Writable[] partsb = sb.get();

		if (orders.length != partsa.length || orders.length != partsb.length) {
			throw new IllegalArgumentException();
		}

		for (int i = 0; i < orders.length; i++) {
			if (null != orders[i]) {
				int result = ((WritableComparable) partsa[i])
						.compareTo(partsb[i]);
				if (result != 0) {
					return orders[i] ? result : -result;
				}
			}
		}
		return 0;

	}
}
