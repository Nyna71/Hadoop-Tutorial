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

import com.proximus.hadoop.tutorial.AggregatorMRJob.AggregatorMapper;
import com.proximus.hadoop.tutorial.AggregatorMRJob.AggregatorReducer;

/**
 * MapReduce program that reads Airline statistics and aggregates flight delays per month.
 * Input and Output Paths are read from command line.
 * @author Jonathan Puvilland
 *
 */
public class Aggregator implements Tool {
	private Configuration conf;

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Aggregator(), args);
		System.exit(status);
	}

	@Override
	public int run(String[] allArgs) throws Exception {
		conf = new Configuration();
		Job job = Job.getInstance(conf);
		
	    job.setJarByClass(AggregatorMRJob.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(AggregatorMapper.class);
	    job.setReducerClass(AggregatorReducer.class);
	    
	    job.setNumReduceTasks(1);
	    
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.out.println("Input Path: " + args[0]);
	    System.out.println("Output Path: " + args[1]);
	    
	    boolean status = job.waitForCompletion(true);
	    
	    return status ? 0: 1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}