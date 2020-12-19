package datacloud.hadoop.lastfm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TripleIntWritable implements WritableComparable<TripleIntWritable> {
    private IntWritable value1;
    private IntWritable value2;
    private IntWritable value3;

    public TripleIntWritable() {
        this.value1 = new IntWritable(0);
        this.value2 = new IntWritable(0);
        this.value3 = new IntWritable(0);
    }

    public TripleIntWritable(int value1, int value2, int value3) {
        this.value1 = new IntWritable(value1);
        this.value2 = new IntWritable(value2);
        this.value3 = new IntWritable(value3);
    }

    public int getInt1() {
        return this.value1.get();
    }

    public int getInt2() {
        return this.value2.get();
    }
    
    public int getInt3() {
        return this.value3.get();
    }
    
    public void setInt1(int value1) {
    	this.value1.set(value1);
    }
    
    public void setInt2(int value2) {
    	this.value2.set(value2);
    }
    
    public void setInt3(int value3) {
    	this.value3.set(value3);
    }
    
    @Override
    public String toString() {
        return this.value1.toString()+" "+this.value2.toString() + " " + this.value3.toString();
    }


    @Override
    public void readFields(DataInput in) throws IOException {
    	this.value1.readFields(in);
    	this.value2.readFields(in);
    	this.value3.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	this.value1.write(out);
    	this.value2.write(out);
    	this.value3.write(out);
    }
    //source : http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
	@Override
	public int compareTo(TripleIntWritable o) {
		if (this.value1.compareTo(o.value1)==0){
			if(this.value2.compareTo(o.value2)==0) {
				return (this.value3.compareTo(o.value3));
			}else {
				return this.value2.compareTo(o.value2);
			}
	    }
	    else return (this.value1.compareTo(o.value1));
	}


}
