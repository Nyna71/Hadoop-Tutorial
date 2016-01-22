package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apress.prohadoop.utils.*;

public class PartitionerMRJob {

	public static class PartitionerMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map (LongWritable byteOffset, Text airlineRecord, Context context) throws IOException, InterruptedException
		{
			if(!AirlineDataUtils.isHeader(airlineRecord))
			{
				int month = Integer.parseInt(AirlineDataUtils.getMonth(airlineRecord.toString().split(",")));
				context.write(new IntWritable(month), airlineRecord);
			}
		}
	}
	
	public static class MonthPartitioner extends Partitioner<IntWritable, Text> {

		@Override
		public int getPartition(IntWritable month, Text airlineRecord, int nbrReducers) {
			return month.get() - 1;
		}
	}

	public static class PartitionerReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		public void reduce (IntWritable month, Iterable<Text> airlineRecords, Context context) throws IOException, InterruptedException
		{
			for(Text record : airlineRecords)
				context.write(NullWritable.get(), record);
		}
	}
}
