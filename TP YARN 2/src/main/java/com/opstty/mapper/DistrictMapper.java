package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    private final IntWritable district = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header
        if (line.startsWith("ID;")) {
            return;
        }
        String[] fields = line.split(";");
        // Ajoutez une vérification pour s'assurer que fields a au moins 2 éléments
        if (fields.length > 1) {
            try {
                int districtNumber = Integer.parseInt(fields[1]); // Assuming district number is in the second column
                district.set(districtNumber);
                context.write(district, NullWritable.get());
            } catch (NumberFormatException e) {
                // Log and ignore lines where the district number is not a valid integer
                System.err.println("Invalid district number: " + fields[1]);
            }
        }
    }
}
