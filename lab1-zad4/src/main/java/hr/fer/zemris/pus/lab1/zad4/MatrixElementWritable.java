package hr.fer.zemris.pus.lab1.zad4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/**
 * Class for intermediate result of the MapReduce.
 * @author gudi
 *
 */
public class MatrixElementWritable implements Writable {

	public Text matrixName;
	public IntWritable index;
	public DoubleWritable value;
	
	public MatrixElementWritable() {
		this.matrixName = new Text();
		this.index = new IntWritable();
		this.value = new DoubleWritable();
	}
	
	public MatrixElementWritable(String matrixName, int index, double value) {
		this.matrixName = new Text(matrixName);
		this.index = new IntWritable(index);
		this.value = new DoubleWritable(value);
	}
	
	public MatrixElementWritable copy() {
		return new MatrixElementWritable(matrixName.toString(), index.get(), value.get());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		matrixName.write(out);
		index.write(out);
		value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		matrixName.readFields(in);
		index.readFields(in);
		value.readFields(in);
	}

	public Text getMatrixName() {
		return matrixName;
	}

	public void setMatrixName(Text matrixName) {
		this.matrixName = matrixName;
	}

	public IntWritable getIndex() {
		return index;
	}

	public void setIndex(IntWritable index) {
		this.index = index;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return matrixName + " " + index + " " + value;
	}
}
