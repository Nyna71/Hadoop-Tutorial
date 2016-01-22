package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apress.prohadoop.utils.AirlineDataUtils;

/**
 * A Class that groups the Mapper and Reducer Classes used to aggregate Airline statistics.
 * In this set-up, Mapper and Reducer classes have to be declared as static.
 *  * @author Jonathan Puvilland
 *
 */
public class AggregatorMRJob {
	public static final IntWritable RECORD = new IntWritable(0);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_ON_TIME = new IntWritable(2);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(3);
	public static final IntWritable DEPARTURE_ON_TIME = new IntWritable(4);
	public static final IntWritable IS_CANCELLED = new IntWritable(5);
	public static final IntWritable IS_DIVERTED = new IntWritable(6);
	
	public static class AggregatorMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
		   if(!AirlineDataUtils.isHeader(value))
		   {
			   String[] contents = value.toString().split(",");
			   String month = AirlineDataUtils.getMonth(contents);
			   int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents),0);
			   int departureDelay =AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents),0);
			   boolean isCancelled =  AirlineDataUtils.parseBoolean(AirlineDataUtils.getCancelled(contents),false);
			   boolean isDiverted = AirlineDataUtils.parseBoolean(AirlineDataUtils.getDiverted(contents),false);
			   
			   context.write(new Text(month), RECORD);
			   
			   if(arrivalDelay > 0)
				   context.write(new Text(month), DEPARTURE_DELAY);
			   else
				   context.write(new Text(month), DEPARTURE_ON_TIME);
			   
			   if(departureDelay > 0)
				   context.write(new Text(month), ARRIVAL_DELAY);
			   else
				   context.write(new Text(month), ARRIVAL_ON_TIME);
			   
			   if(isCancelled)
				   context.write(new Text(month), IS_CANCELLED);
			   
			   if(isDiverted)
				   context.write(new Text(month), IS_DIVERTED);   
		   }
		}
	}
	
	public static class AggregatorReducer extends Reducer<Text, IntWritable, NullWritable, Text>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int totalRecords = 0;
			int arrivalOnTime = 0;
			int arrivalDelays = 0;
			int departureOnTime = 0;
			int departureDelays = 0;
			int cancellations = 0;
			int diversions = 0;

			for(IntWritable value : values)
			{
				if(value.equals(RECORD)) totalRecords++;
				if(value.equals(ARRIVAL_ON_TIME)) arrivalOnTime++;
				if(value.equals(ARRIVAL_DELAY)) arrivalDelays++;
				if(value.equals(DEPARTURE_ON_TIME)) departureOnTime++;
				if(value.equals(DEPARTURE_DELAY)) departureDelays++;
				if(value.equals(IS_CANCELLED)) cancellations++;
				if(value.equals(IS_DIVERTED)) diversions++;
			}
			
			//Prepare and produce output

			StringBuilder output = new StringBuilder(key.toString());
			output.append(";").append(totalRecords);
			output.append(";").append(arrivalOnTime);
			output.append(";").append(arrivalDelays);
			output.append(";").append(departureOnTime);
			output.append(";").append(departureDelays);
			output.append(";").append(cancellations);
			output.append(";").append(diversions);
			
			context.write(NullWritable.get(), new Text(output.toString()));
		}
	}
}
