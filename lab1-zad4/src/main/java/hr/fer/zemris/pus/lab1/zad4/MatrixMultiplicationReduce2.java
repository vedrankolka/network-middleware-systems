package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationReduce2 extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		double value = streamterator(values).mapToDouble(v -> v.get()).sum();
		output.collect(key, new DoubleWritable(value));
	}
	
	private static <T> Stream<T> streamterator(Iterator<T> it) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
	}
}
