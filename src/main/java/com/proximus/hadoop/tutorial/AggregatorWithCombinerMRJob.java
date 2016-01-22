package com.proximus.hadoop.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apress.prohadoop.utils.AirlineDataUtils;

/**
 * A Class that groups the Mapper and Reducer Classes used to aggregate Airline statistics.
 * In this set-up, Mapper, Combiner and Reducer classes have to be declared as static.
 *  * @author Jonathan Puvilland
 *
 */
public class AggregatorWithCombinerMRJob
{
	public static final IntWritable RECORD = new IntWritable(0);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_ON_TIME = new IntWritable(2);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(3);
	public static final IntWritable DEPARTURE_ON_TIME = new IntWritable(4);
	public static final IntWritable IS_CANCELLED = new IntWritable(5);
	public static final IntWritable IS_DIVERTED = new IntWritable(6);
	
	/**
	 * Mapper that read lines of text from Airline dataset and extracts some statistics (Arrival OnTime, Departure OnTime, etc)
	 * Makes use of a Combiner for summing the statistics. Generates KVP for the combiner in the form
	 * - Key: Month
	 * - Value: Map<IntWritable statistic, IntWritable 1>
	 * @author Jonathan Puvilland
	 *
	 */
	public static class AggregatorMapper extends Mapper<LongWritable, Text, Text, MapWritable>
	{
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			if(!AirlineDataUtils.isHeader(value))
			   {
				   String[] contents = value.toString().split(",");
				   String month = AirlineDataUtils.getMonth(contents);
				   int arrivalDelay = AirlineDataUtils.parseMinutes(AirlineDataUtils.getArrivalDelay(contents),0);
				   int departureDelay =AirlineDataUtils.parseMinutes(AirlineDataUtils.getDepartureDelay(contents),0);
				   boolean isCancelled =  AirlineDataUtils.parseBoolean(AirlineDataUtils.getCancelled(contents),false);
				   boolean isDiverted = AirlineDataUtils.parseBoolean(AirlineDataUtils.getDiverted(contents),false);
				   
				   context.write(new Text(month), getMapWritable(RECORD, new IntWritable(1)));     
				   
				   if(arrivalDelay > 0)
					   context.write(new Text(month), getMapWritable(DEPARTURE_DELAY, new IntWritable(1)));
				   else
					   context.write(new Text(month), getMapWritable(DEPARTURE_ON_TIME, new IntWritable(1)));
				   
				   if(departureDelay > 0)
					   context.write(new Text(month), getMapWritable(ARRIVAL_DELAY, new IntWritable(1)));
				   else
					   context.write(new Text(month), getMapWritable(ARRIVAL_ON_TIME, new IntWritable(1)));
				   
				   if(isCancelled)
					   context.write(new Text(month), getMapWritable(IS_CANCELLED, new IntWritable(1)));
				   
				   if(isDiverted)
					   context.write(new Text(month), getMapWritable(IS_DIVERTED, new IntWritable(1)));
			   }
		}
	}
	
	private static MapWritable getMapWritable(IntWritable key, IntWritable value)
	{
		MapWritable map = new MapWritable();
		map.put(key, value);
		return map;
	}
	
	public static class AggregatorCombiner extends Reducer<Text, MapWritable, Text, MapWritable>
	{
		public void reduce (Text month, MapWritable combinerMap, Context context) throws IOException, InterruptedException
		{
			int totalRecords = 0;
			int arrivalOnTime = 0;
			int arrivalDelays = 0;
			int departureOnTime = 0;
			int departureDelays = 0;
			int cancellations = 0;
			int diversions = 0;
			
			if(combinerMap.containsKey(RECORD))
				totalRecords += ((IntWritable) combinerMap.get(RECORD)).get();
			if(combinerMap.containsKey(ARRIVAL_DELAY))
				totalRecords += ((IntWritable) combinerMap.get(ARRIVAL_DELAY)).get();
			if(combinerMap.containsKey(ARRIVAL_ON_TIME))
				totalRecords += ((IntWritable) combinerMap.get(ARRIVAL_ON_TIME)).get();
			if(combinerMap.containsKey(DEPARTURE_DELAY))
				totalRecords += ((IntWritable) combinerMap.get(DEPARTURE_DELAY)).get();
			if(combinerMap.containsKey(DEPARTURE_ON_TIME))
				totalRecords += ((IntWritable) combinerMap.get(DEPARTURE_ON_TIME)).get();
			if(combinerMap.containsKey(IS_CANCELLED))
				totalRecords += ((IntWritable) combinerMap.get(IS_CANCELLED)).get();
			if(combinerMap.containsKey(IS_DIVERTED))
				totalRecords += ((IntWritable) combinerMap.get(IS_DIVERTED)).get();
			
			context.write(month, getMapWritable(RECORD, new IntWritable(totalRecords)));
			context.write(month, getMapWritable(ARRIVAL_ON_TIME, new IntWritable(arrivalOnTime)));
			context.write(month, getMapWritable(ARRIVAL_DELAY, new IntWritable(arrivalDelays)));
			context.write(month, getMapWritable(DEPARTURE_ON_TIME, new IntWritable(departureOnTime)));
			context.write(month, getMapWritable(DEPARTURE_DELAY, new IntWritable(departureDelays)));
			context.write(month, getMapWritable(IS_CANCELLED, new IntWritable(cancellations)));
			context.write(month, getMapWritable(IS_DIVERTED, new IntWritable(diversions)));
		}
	}
	
	public static class AggregatorReducer extends Reducer<Text, MapWritable, NullWritable, Text>
	{
		public void reduce(Text month, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
		{
			int totalRecords = 0;
			int arrivalOnTime = 0;
			int arrivalDelays = 0;
			int departureOnTime = 0;
			int departureDelays = 0;
			int cancellations = 0;
			int diversions = 0;

			for(MapWritable value : values)
			{
				if(value.containsKey(RECORD))
					totalRecords += ((IntWritable) value.get(RECORD)).get();
				if(value.containsKey(ARRIVAL_DELAY))
					totalRecords += ((IntWritable) value.get(ARRIVAL_DELAY)).get();
				if(value.containsKey(ARRIVAL_ON_TIME))
					totalRecords += ((IntWritable) value.get(ARRIVAL_ON_TIME)).get();
				if(value.containsKey(DEPARTURE_DELAY))
					totalRecords += ((IntWritable) value.get(DEPARTURE_DELAY)).get();
				if(value.containsKey(DEPARTURE_ON_TIME))
					totalRecords += ((IntWritable) value.get(DEPARTURE_ON_TIME)).get();
				if(value.containsKey(IS_CANCELLED))
					totalRecords += ((IntWritable) value.get(IS_CANCELLED)).get();
				if(value.containsKey(IS_DIVERTED))
					totalRecords += ((IntWritable) value.get(IS_DIVERTED)).get();
			}
			
			//Prepare and produce output

			StringBuilder output = new StringBuilder(month.toString());
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
