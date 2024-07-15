package com.opstty.reducer;

import com.opstty.writable.TreeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, TreeWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<TreeWritable> values, Context context) throws IOException, InterruptedException {
        int oldestYear = Integer.MAX_VALUE;
        int oldestDistrict = 0;

        for (TreeWritable value : values) {
            if (value.getYear().get() < oldestYear) {
                oldestYear = value.getYear().get();
                oldestDistrict = value.getDistrict().get();
            }
        }

        context.write(new IntWritable(oldestDistrict), new IntWritable(oldestYear));
    }
}
