package com.proximus.hadoop.tutorial;

import org.apache.hadoop.util.ToolRunner;

/**
 * Main Class for executing various MapReduce jobs.
 * MapReduce jobs are better started using the ToolRunner Class. It provides an easy way to Configure an Hadoop MapReduce
 * session and pass command line arguments using the GenricOptionParser class.
 * The run method of ToolRunner invokes a Class that must implement the Tool interface.
 * @author Jonathan Puvilland
 *
 */
public class HadoopTutorialMain {

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new WhereClause(), args);

		System.exit(status);
	}

}
