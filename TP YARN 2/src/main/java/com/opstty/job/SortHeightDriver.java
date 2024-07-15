package com.opstty.job;

import com.opstty.mapper.HeightMapper;
import com.opstty.reducer.IdentityReducer; // Importez votre IdentityReducer
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortHeightDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "sort heights");
        job.setJarByClass(SortHeightDriver.class);
        job.setMapperClass(HeightMapper.class);
        job.setReducerClass(IdentityReducer.class); // Utilisez le bon réducteur
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(FloatWritable.Comparator.class); // Sort by key (height)

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SortHeightDriver(), args);
        System.exit(res);
    }
}
