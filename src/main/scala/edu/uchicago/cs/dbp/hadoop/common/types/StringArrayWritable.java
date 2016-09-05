package edu.uchicago.cs.dbp.hadoop.common.types;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StringArrayWritable extends ArrayWritable implements
		WritableComparable<StringArrayWritable> {

	public StringArrayWritable() {
		super(Text.class);
	}

	public StringArrayWritable(String[] data) {
		this();
		Writable[] strs = new Writable[data.length];
		for (int i = 0; i < strs.length; i++)
			strs[i] = new Text(data[i]);
		set(strs);
	}

	public int size() {
		return super.get().length;
	}

	@Override
	public int compareTo(StringArrayWritable o) {
		Writable[] mine = get();
		Writable[] yours = o.get();
		for (int i = 0; i < mine.length; i++) {
			Text m = (Text) mine[i];
			Text y = (Text) yours[i];
			int res = m.compareTo(y);
			if (res != 0)
				return res;
		}
		return 0;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (Writable data : get()) {
			builder.append(data.toString());
			builder.append("\t");
		}
		return builder.toString().trim();
	}

}
