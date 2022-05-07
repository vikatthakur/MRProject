package com.vt.fj;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileJoinMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	private Text mokey = new Text();
	private LongWritable movalue = new LongWritable();
	
	public void map(LongWritable key, Text Value, Context context) throws IOException, InterruptedException {
		
		String[] flights = Value.toString().split(",");
		
		String destination = flights[0];
		String origin = flights[1];
		Long count = 0L;
		try {
			count = Long.parseLong(flights[2].trim());
		}
		catch(NumberFormatException e) {
			e.printStackTrace();
		}
		
		if(destination.equals("United States")){
			mokey.set(destination+"\t"+origin);
			movalue.set(count);
			context.write(mokey, movalue);
		}
			

	}
	
	

}
