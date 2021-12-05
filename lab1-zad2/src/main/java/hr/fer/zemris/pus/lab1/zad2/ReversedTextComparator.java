package hr.fer.zemris.pus.lab1.zad2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReversedTextComparator extends WritableComparator {
	
	protected ReversedTextComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return -1 * super.compare(a, b);
	}
}
