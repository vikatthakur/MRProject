package com.vt.empctc;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeeCTCMapper extends Mapper<Object, Text, Text, Text>{
	 
//	sample datasets
//	101	Alice	23	female	IT	45
//	102	Bob	34	male	Finance	89
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\t");
		
		if (tokens.length == 6) {
			String dept = tokens[4];	
			context.write(new Text(dept), new Text(value));
		}
		else {
			System.out.println("Bad Record Log: " + value);
		}
		

	}

}
