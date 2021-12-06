package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DebugIdentityReducer extends MapReduceBase implements Reducer<IntWritable, MatrixElementWritable, IntWritable, MatrixElementWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<MatrixElementWritable> values,
			OutputCollector<IntWritable, MatrixElementWritable> output, Reporter reporter) throws IOException {
		Iterable<MatrixElementWritable> it = () -> values;
		for (MatrixElementWritable value : it) {
			output.collect(key, value);
		}
	}
}
