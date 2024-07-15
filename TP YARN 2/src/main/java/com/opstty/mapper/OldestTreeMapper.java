package com.opstty.mapper;

import com.opstty.writable.TreeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, IntWritable, TreeWritable> {
    private IntWritable district = new IntWritable();
    private TreeWritable treeWritable = new TreeWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(";");

        // Check if the line has the correct number of tokens and if the year field is not empty
        if (tokens.length > 5 && !tokens[5].trim().isEmpty()) {
            try {
                district.set(Integer.parseInt(tokens[1]));
                int year = Integer.parseInt(tokens[5].trim());

                treeWritable.set(year, district.get());
                context.write(district, treeWritable);
            } catch (NumberFormatException e) {
                // Log and skip the line if there is a number format exception
                System.err.println("NumberFormatException: " + e.getMessage() + " for line: " + value.toString());
            }
        } else {
            // Log and skip the line if it does not have the correct number of tokens or year is missing
            System.err.println("Incorrect number of tokens or missing year: " + value.toString());
        }
    }
}
