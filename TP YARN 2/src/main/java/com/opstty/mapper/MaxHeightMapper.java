package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text kind = new Text();
    private FloatWritable height = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header
        if (line.startsWith("ID;")) {
            return;
        }
        String[] fields = line.split(";");
        if (fields.length > 6) { // Assuming kind is in the 4th column and height is in the 7th column
            kind.set(fields[3]);
            try {
                height.set(Float.parseFloat(fields[6]));
                context.write(kind, height);
            } catch (NumberFormatException e) {
                // Ignore malformed lines
            }
        }
    }
}
