package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationIdentityMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		String[] parts = value.toString().split("\\s+");
		String outputKey = parts[0] + " " + parts[1];
		double outputValue = Double.parseDouble(parts[2]);
		output.collect(new Text(outputKey), new DoubleWritable(outputValue));
	}
}
