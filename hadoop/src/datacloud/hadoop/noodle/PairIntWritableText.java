package datacloud.hadoop.noodle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PairIntWritableText implements WritableComparable<PairIntWritableText> {
    private IntWritable value1;
    private Text value2;

    public PairIntWritableText() {
        this.value1 = new IntWritable();
        this.value2 = new Text();
    }

    public PairIntWritableText(int value1, String value2) {
        this.value1 = new IntWritable(value1);
        this.value2 = new Text(value2);
    }

    public int getValue1() {
        return this.value1.get();
    }

    public String getValue2() {
        return this.value2.toString();
    }
    
    public void setValue1(int value1) {
    	this.value1.set(value1);
    }
    
    public void setValue2(String value2) {
    	this.value2.set(value2);
    }

    @Override
    public String toString() {
        return value1.toString()+" "+value2.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1.readFields(in);
        value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value1.write(out);
        value2.write(out);
    }

	@Override
	public int compareTo(PairIntWritableText o) {
		if (this.value1.compareTo(o.value1)==0){
	       return (this.value2.compareTo(o.value2));
	    }
	    else return (this.value1.compareTo(o.value1));
	}


}
