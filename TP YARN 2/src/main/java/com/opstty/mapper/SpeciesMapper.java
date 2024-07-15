package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header
        if (line.startsWith("ID;")) {
            return;
        }
        String[] fields = line.split(";");
        if (fields.length > 3) { // Assuming species is in the 4th column
            species.set(fields[3]);
            context.write(species, NullWritable.get());
        }
    }
}
