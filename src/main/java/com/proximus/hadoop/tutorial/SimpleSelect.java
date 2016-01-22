package com.proximus.hadoop.tutorial;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * MapReduce program that reads Airline statistics and simply copy back all records to the output,
 * removing the header record.
 * Input and Output Paths are read from command line
 * As no aggregations is performed, no Reducer is defined.
 * @author Jonathan Puvilland
 *
 */
public class SimpleSelect implements Tool {
	private Configuration conf;
	
	public SimpleSelect() {
		conf = new Configuration();
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int run(String[] allArgs) throws Exception {
		Job job = Job.getInstance(conf);
		
//		printConfiguration();
		
		job.setJarByClass(SimpleSelect.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(SimpleSelectMapper.class);
	    job.setNumReduceTasks(0);
	    
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    boolean status = job.waitForCompletion(true);
	    
	    return status ? 0 : 1; 
	}
	
	/**
	 * Prints the Hadoop configuration parameters to standard output.
	 */
	@SuppressWarnings("unused")
	private void printConfiguration()
	{
		Iterator<Entry<String, String>> configIter = conf.iterator();
		Entry<String, String> entry;
		
		while(configIter.hasNext()) {
			entry = configIter.next();
			System.out.println("{" + entry.getKey() + ": " + entry.getValue() + "}");
		}
	}
}
