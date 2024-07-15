package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeWritable implements Writable {
    private IntWritable year;
    private IntWritable district;

    public TreeWritable() {
        this.year = new IntWritable();
        this.district = new IntWritable();
    }

    public TreeWritable(int year, int district) {
        this.year = new IntWritable(year);
        this.district = new IntWritable(district);
    }

    public void set(int year, int district) {
        this.year.set(year);
        this.district.set(district);
    }

    public IntWritable getYear() {
        return year;
    }

    public IntWritable getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        year.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year.readFields(in);
        district.readFields(in);
    }

    @Override
    public String toString() {
        return year + "\t" + district;
    }
}
