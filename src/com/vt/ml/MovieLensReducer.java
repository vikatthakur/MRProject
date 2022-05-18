package com.vt.ml;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int maxRating = 5;
		int totViews = 0;
		String title = null;
		for(Text val : values) {
			if(val.toString().equalsIgnoreCase("1")) {
				totViews++;
			}
			else {
				title = val.toString();
			}
		}
		context.write(NullWritable.get(), new Text(title+","+totViews));
	}
}
