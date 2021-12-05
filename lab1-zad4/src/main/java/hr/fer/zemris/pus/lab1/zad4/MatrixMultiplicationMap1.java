package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationMap1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

	// Map1 m_ij -> emit(j, (M, i, m_ij))
	//      n_jk -> emit(j, (N, k, n_jk))
	@Override
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String[] parts = value.toString().split(" ");
		String matrixName = parts[0];
		double v = Double.parseDouble(parts[3]);
		int j;
		StringBuilder sb = new StringBuilder(matrixName).append(' ');
		if ("a".equals(matrixName)) { // MatrixMultiplication.first
			int i = Integer.parseInt(parts[1]);
			j = Integer.parseInt(parts[2]);
			sb.append(i);
		} else {
			j = Integer.parseInt(parts[1]);
			int k = Integer.parseInt(parts[2]);
			sb.append(k);
		}
		String newValue = sb.append(' ').append(v).toString();
		output.collect(new IntWritable(j), new Text(newValue));
	}

}
