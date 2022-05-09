package com.vt.adv.fj;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileJoinAdvReducer extends Reducer<Text, JoinWritable, NullWritable, Text> {
	
	
	public void reduce(Text key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException {
		
		String dept = null;
		String name = null;
		
		for(JoinWritable val : values) {
			System.out.println("Reducer Log : " + val.getFileName().toString());
			if(val.getFileName().toString().equals("employee_name.txt")) {
				name = val.getMovalue().toString();
			}
			else if(val.getFileName().toString().equals("employee_dept.txt")){
				dept = val.getMovalue().toString();
			}
		}
		//for orderd output format 
		StringBuffer result = new StringBuffer(key.toString()).append(",");
		result.append(name).append(",").append(dept);
		
		context.write(NullWritable.get(), new Text(result.toString()));
	}

}
