package edu.uchicago.cs.dbp.hadoop.common.types;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupComparator extends WritableComparator {

	private int keySize;

	public KeyGroupComparator() {
		this(1);
	}

	public KeyGroupComparator(int keysize) {
		super(StringArrayWritable.class, true);
		this.keySize = keysize;
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringArrayWritable sa = (StringArrayWritable) a;
		StringArrayWritable sb = (StringArrayWritable) b;
		Writable[] aa = Arrays.copyOf(sa.get(), keySize);
		Writable[] ba = Arrays.copyOf(sb.get(), keySize);
		for (int i = 0; i < keySize; i++) {
			int val = ((Text) aa[i]).compareTo((Text) ba[i]);
			if (val != 0)
				return val;
		}
		return 0;
	}

	public static final class Key2GroupComparator extends KeyGroupComparator {
		public Key2GroupComparator() {
			super(2);
		}
	}
}