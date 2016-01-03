package com.proximus.hadoop.tutorial;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apress.prohadoop.utils.AirlineDataUtils;

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
			double totalRecords = 0;
			double arrivalOnTime = 0;
			double arrivalDelays = 0;
			double departureOnTime = 0;
			double departureDelays = 0;
			double cancellations = 0;
			double diversions = 0;

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
			DecimalFormat df = new DecimalFormat( "0.0000" );

			StringBuilder output = new StringBuilder(key.toString());
			output.append(";").append(totalRecords);
			output.append(";").append(df.format(arrivalOnTime/totalRecords));
			output.append(";").append(df.format(arrivalDelays/totalRecords));
			output.append(";").append(df.format(departureOnTime/totalRecords));
			output.append(";").append(df.format(departureDelays/totalRecords));
			output.append(";").append(df.format(cancellations/totalRecords));
			output.append(";").append(df.format(diversions/totalRecords));
			
			context.write(NullWritable.get(), new Text(output.toString()));
		}
	}
}
