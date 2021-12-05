package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationReduce1 extends MapReduceBase implements Reducer<IntWritable, Text, Text, DoubleWritable> {

	// Reduce1 prodem kroz sve i sta?
	// 	j, [(M, i, m_ij), (N, k, n_jk)] -> emit((i,k), (m_ij*n_kj*))
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		List<String> firstMatrix = new ArrayList<String>();
		List<String> secondMatrix = new ArrayList<String>();
		Iterable<Text> it = () -> values;
		for (Text value : it) {
			String[] parts = value.toString().split(" ", 2);
			String matrixName = parts[0];
			if ("a".equals(matrixName)) { // MatrixMultiplication.first
				firstMatrix.add(parts[1]);
			} else {
				secondMatrix.add(parts[1]);
			}
		}
		for (String aPart : firstMatrix) {
			for (String bPart : secondMatrix) {
				String[] aParts = aPart.split(" ");
				String[] bParts = bPart.split(" ");
				String i = aParts[0];
				String k = bParts[0];
				double a_ij = Double.parseDouble(aParts[1]);
				double b_jk = Double.parseDouble(bParts[1]);
				double v =  a_ij * b_jk;
				String outputKey = i + " " + k;
				output.collect(new Text(outputKey), new DoubleWritable(v));
			}
		}
		
	}
}
