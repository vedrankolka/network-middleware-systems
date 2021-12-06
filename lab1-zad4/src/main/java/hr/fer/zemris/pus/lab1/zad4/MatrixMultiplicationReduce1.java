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

public class MatrixMultiplicationReduce1 extends MapReduceBase implements Reducer<IntWritable, MatrixElementWritable, Text, DoubleWritable> {

	// Reduce1 prodem kroz sve i sta?
	// 	j, [(M, i, m_ij), (N, k, n_jk)] -> emit((i,k), (m_ij*n_kj*))
	
	@Override
	public void reduce(IntWritable key, Iterator<MatrixElementWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		List<MatrixElementWritable> firstMatrix = new ArrayList<>();
		List<MatrixElementWritable> secondMatrix = new ArrayList<>();
		Iterable<MatrixElementWritable> it = () -> values;
		for (MatrixElementWritable element : it) {
//			System.out.println("element: " + element);
			if ("a".equals(element.matrixName.toString())) { // TODO MatrixMultiplication.first
				firstMatrix.add(element.copy());		
			} else {
				secondMatrix.add(element.copy());
			} 
		}
		
//		System.out.println("firstMatrix:");
//		firstMatrix.forEach(System.out::println);
//		System.out.println();
//		
//		System.out.println("secondMatrix:");
//		secondMatrix.forEach(System.out::println);
//		System.out.println();
		
		for (MatrixElementWritable aElement : firstMatrix) {
			String i = aElement.index.toString();
			double a_ij = aElement.value.get();
			for (MatrixElementWritable bElement : secondMatrix) {
				String k = bElement.index.toString();
				double b_jk = bElement.value.get();
				// ( (i, k), a_ij*b_jk )
				Text outputKey = new Text(i + " " + k);
				DoubleWritable outputValue = new DoubleWritable(a_ij * b_jk);
//				System.out.println("i + k = " + outputKey + "\ta_ij * b_jk = " + outputValue);
				output.collect(outputKey, outputValue);
			}
		}
		
	}
}
