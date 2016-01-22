package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apress.prohadoop.utils.AirlineDataUtils;

/**
 * Reads a csv text file with Airline statistics and simply copy back all records to the output,
 * removing the header record.
 * <b>- Key: Null 
 * - Value: Ariline dataset record</b>
 * @author Jonathan Puvilland
 *
 */
public class SimpleSelectMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		if(!AirlineDataUtils.isHeader(value))
		{
			StringBuilder output = AirlineDataUtils.mergeStringArray(AirlineDataUtils.getSelectResultsPerRow(value), ",");
			context.write(NullWritable.get(),new Text(output.toString()));
		}
	}
}