package edu.uchicago.cs.dbp.common.types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntArrayWritable extends ArrayWritable implements
		WritableComparable<IntArrayWritable> {

	public IntArrayWritable() {
		super(IntWritable.class);
	}

	public IntArrayWritable(String[] data) {
		this();
		Writable[] ints = new Writable[data.length];
		for (int i = 0; i < ints.length; i++)
			ints[i] = new IntWritable(Integer.valueOf(data[i]));
		set(ints);
	}

	@Override
	public int compareTo(IntArrayWritable o) {
		Writable[] mine = get();
		Writable[] yours = o.get();
		for (int i = 0; i < mine.length; i++) {
			IntWritable m = (IntWritable) mine[i];
			IntWritable y = (IntWritable) yours[i];
			int res = m.compareTo(y);
			if (res != 0)
				return res;
		}
		return 0;
	}
}
