package edu.uchicago.cs.dbp.common.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupComparator extends WritableComparator {

	public KeyGroupComparator() {
		super(StringArrayWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringArrayWritable sa = (StringArrayWritable)a;
		StringArrayWritable sb = (StringArrayWritable)b;
		return ((Text)sa.get()[0]).compareTo((Text)sb.get()[0]);
	}
}
