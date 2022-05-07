package com.vt.sla;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SlaMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	private Text moKey = new Text();
	private FloatWritable moValue = new FloatWritable();
	
	public void map(LongWritable key, Text value, Context context) {
		//log line
		//hostname 060522,18:36 Average:        all      0.95      0.00      0.92      0.21      0.00     97.91
		
		String[] logs = value.toString().split("\\s+");
		String hostname = logs[0];
		String Date = logs[1].split(",")[0];
		float cpuPercentUsed = 100.0f - Float.parseFloat(logs[logs.length - 1]);
		
		moKey.set(hostname+"\t"+Date);
		moValue.set(cpuPercentUsed);
		
		try {
			context.write(moKey, moValue);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
