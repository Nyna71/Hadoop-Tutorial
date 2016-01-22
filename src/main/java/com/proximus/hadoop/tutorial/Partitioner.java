package com.proximus.hadoop.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.proximus.hadoop.tutorial.PartitionerMRJob.MonthPartitioner;
import com.proximus.hadoop.tutorial.PartitionerMRJob.PartitionerMapper;
import com.proximus.hadoop.tutorial.PartitionerMRJob.PartitionerReducer;

public class Partitioner implements Tool
{
	private static Configuration conf;
	
    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        ToolRunner.run(new Partitioner(), args);
    }

    public int run(String[] allArgs) throws Exception {
        Job job = Job.getInstance(conf);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(MonthPartitioner.class);
        job.setMapperClass(PartitionerMapper.class);
        job.setReducerClass(PartitionerReducer.class);
        job.setNumReduceTasks(12);

        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }
    
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		
	}
}
