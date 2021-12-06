package hr.fer.zemris.pus.lab1.zad4;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MatrixMultiplication {
	
	public static String first;
	public static String second;

	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.err.println("Usage: <input path> <output path> <first_name> <second_name>");
			System.exit(-1);
		}
		first = args[2];
		second = args[3];
		// firstly map to all values to sum
		JobConf job1 = new JobConf(MatrixMultiplication.class);
		job1.setJobName("Matrix multiplication 1/2");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp/"));
		job1.setMapperClass(MatrixMultiplicationMap1.class);
		job1.setReducerClass(MatrixMultiplicationReduce1.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(MatrixElementWritable.class);
		JobClient.runJob(job1);
		// secondly sum all the mapped values
		JobConf job2 = new JobConf(MatrixMultiplication.class);
		job2.setJobName("Matrix Multiplication 2/2");
		FileInputFormat.addInputPath(job2, new Path("temp/part-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setMapperClass(MatrixMultiplicationIdentityMap.class);
		job2.setReducerClass(MatrixMultiplicationReduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		JobClient.runJob(job2);
	}
	
	// P = A x B
	// Map1 m_ij -> emit(j, (M, i, m_ij))
	//      n_jk -> emit(j, (N, k, n_jk))
	// Reduce1 prodem kroz sve i sta?
	// 	j, [(M, i, m_ij), (N, k, n_jk)] -> emit((i,k), (m_ij*n_kj*))
}
