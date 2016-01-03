package com.proximus.hadoop.tutorial;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AggregatorWithCombinerMRJob
{
	public static class AggregatorMapper extends Mapper<LongWritable, Text, Text, MapWritable>
	{
		public void map (LongWritable key, Text value, Context context)
		{
			
		}
	}
}
