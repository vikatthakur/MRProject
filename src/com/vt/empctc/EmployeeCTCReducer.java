package com.vt.empctc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeeCTCReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int mxSal = Integer.MIN_VALUE;
		String res = "";
		
		System.out.println("Reducer key : "+ key);
	
		for(Text val : values) {
			String[] tokens = val.toString().split("\\t");
			int salary = Integer.parseInt(tokens[5]);
			
			if(salary > mxSal ) {
				res = val.toString();
				mxSal = salary; 
			}
		}
		context.write(NullWritable.get(), new Text(res));
	}

}
