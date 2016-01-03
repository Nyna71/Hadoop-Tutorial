package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apress.prohadoop.utils.AirlineDataUtils;

public class WhereClauseMRJob extends Configured implements Tool {

	public static class WhereClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text>
	{
		private int delayInMinutes = 0;
		
	    @Override
	    public void setup(Context context)
	    {
	         delayInMinutes = context.getConfiguration().getInt("map.where.delay",1);
	         System.out.println("Delay in minutes: " + delayInMinutes);
	    }
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			String[] flightDetail = AirlineDataUtils.getSelectResultsPerRow(value);
			
			if(!AirlineDataUtils.isHeader(value))
			{
				int iDepDel = getDepDelay(flightDetail);
				int iArrDel = getArrDelay(flightDetail);
				String delReason = "";
				
				StringBuilder out = AirlineDataUtils.mergeStringArray(flightDetail, ",");
				
				if(iDepDel >= delayInMinutes && iArrDel >= delayInMinutes)
					delReason = "B";
				else if (iDepDel >= delayInMinutes)
					delReason = "O";
				else if (iArrDel >= delayInMinutes)
					delReason = "D";
				
				if(! delReason.isEmpty())
				{
					out.append(",").append(delReason);
				    context.write(NullWritable.get(), new Text(out.toString()));
				}
			}
		}
		
		private int getDepDelay(String[] flightDetail) {
			return AirlineDataUtils.parseMinutes(flightDetail[8], 0);
		}
		
		private int getArrDelay(String[] flightDetail) {
			return AirlineDataUtils.parseMinutes(flightDetail[9], 0);
		}
	}
	
	@Override
	public int run(String[] allArgs) throws Exception {
		Job job = Job.getInstance(getConf());
	    job.setJarByClass(WhereClauseMRJob.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(WhereClauseMapper.class);
	    job.setNumReduceTasks(0);
	    
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    boolean status = job.waitForCompletion(true);
	    
	    if(status){
	    	return 0; }
	    else{
	    	return 1;
	    }
	}

}
