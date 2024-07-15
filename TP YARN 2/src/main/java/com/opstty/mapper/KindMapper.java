package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text kind = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header
        if (line.startsWith("ID;")) {
            return;
        }
        String[] fields = line.split(";");
        if (fields.length > 3) { // Assuming kind is in the 4th column
            kind.set(fields[3]);
            context.write(kind, one);
        }
    }
}
