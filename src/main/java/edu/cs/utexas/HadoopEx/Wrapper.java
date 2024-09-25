package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class Wrapper implements Writable{
    
    FloatWritable time;
    FloatWritable total;

    public Wrapper() {
        this.total = new FloatWritable();
        this.time = new FloatWritable();
    }

    public Wrapper(float time, float total) {
        this.total = new FloatWritable(total);
        this.time = new FloatWritable(time);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        total.write(dataOutput);
        time.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total.readFields(dataInput);
        time.readFields(dataInput);
    }
    public FloatWritable gettotal() {
        return total;
    }
    public FloatWritable gettime() {
        return time;
    }

}
