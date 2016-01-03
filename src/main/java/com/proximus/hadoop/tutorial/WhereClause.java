package com.proximus.hadoop.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class WhereClause {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ToolRunner.run(new WhereClauseMRJob(), args);
		
		System.out.println("System property: " + System.getProperty("map.where.delay"));
	}

}
