package datacloud.hadoop.lastfm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

// source : https://stackoverflow.com/questions/49651593/why-tuplewritable-become-empty-when-passed-to-recuder
public class PairIntWritable implements WritableComparable<PairIntWritable> {
    private IntWritable value1;
    private IntWritable value2;

    public PairIntWritable() {
        this.value1 = new IntWritable();
        this.value2 = new IntWritable();
    }

    public PairIntWritable(int value1, int value2) {
        this.value1 = new IntWritable(value1);
        this.value2 = new IntWritable(value2);
    }

    public int getInt1() {
        return this.value1.get();
    }

    public int getInt2() {
        return this.value2.get();
    }
    
    public void setInt1(int value1) {
    	this.value1.set(value1);
    }
    
    public void setInt2(int value2) {
    	this.value2.set(value2);
    }



    @Override
    public void readFields(DataInput in) throws IOException {
    	this.value1.readFields(in);
    	this.value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	this.value1.write(out);
    	this.value2.write(out);
    }
    //source : http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
	@Override
	public int compareTo(PairIntWritable o) {
		if (this.value1.compareTo(o.value1)==0){
	       return (this.value2.compareTo(o.value2));
	    }
	    else return (this.value1.compareTo(o.value1));
	}

}
