package com.vt.fj;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileJoinReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	private LongWritable result = new LongWritable();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		Long sum = 0L;
		
		for(LongWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		
		context.write(key, result);
	}

}
