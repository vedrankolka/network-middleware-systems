package hr.fer.zemris.pus.lab1.zad3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomTextComparator extends WritableComparator {
	
	protected CustomTextComparator() {
		super(Text.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		String[] aParts = a.toString().split("\\s+");
		String[] bParts = b.toString().split("\\s+");
		
		int aCount = Integer.parseInt(aParts[1]);
		int bCount = Integer.parseInt(bParts[1]);
		if (aCount != bCount)
			return Double.compare(bCount, aCount);
		else
			return aParts[0].compareTo(bParts[0]);
	}
}
