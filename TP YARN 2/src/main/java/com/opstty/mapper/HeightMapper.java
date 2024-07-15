package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HeightMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private FloatWritable height = new FloatWritable();
    private Text tree = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header
        if (line.startsWith("ID;")) {
            return;
        }
        String[] fields = line.split(";");
        if (fields.length > 6) { // Assuming height is in the 7th column
            try {
                height.set(Float.parseFloat(fields[6]));
                tree.set(fields[3]); // Set the tree kind as value (optional)
                context.write(height, tree);
            } catch (NumberFormatException e) {
                // Ignore malformed lines
            }
        }
    }
}
