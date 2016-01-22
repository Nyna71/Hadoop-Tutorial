package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apress.prohadoop.utils.AirlineDataUtils;

/**
 * Reads a csv text file with Airline statistics and filters records based on flight delay.
 * <b>- Key: Null 
 * - Value: Ariline dataset record</b>
 * @author Jonathan Puvilland
 *
 */
public class WhereClauseMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	private int delayInMinutes = 0;
	
    @Override
    public void setup(Context context)
    {
         delayInMinutes = context.getConfiguration().getInt("map.where.delay",1);
         System.out.println("Filtering on delays higher than: " + delayInMinutes + " minutes");
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