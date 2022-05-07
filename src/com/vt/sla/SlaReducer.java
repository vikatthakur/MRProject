package com.vt.sla;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SlaReducer extends Reducer<Text, FloatWritable, Text, Text>{
	
	private Text avg = new Text();
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) {
	
		int count = 0;
		float sum = 0.0f;
		
		for(FloatWritable val : values) {
			sum += val.get();
			count++;
		}
		
		avg.set((sum / count) + "%");
		
		try {
			context.write(key, avg);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
			
	}

}
