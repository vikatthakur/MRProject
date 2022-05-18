package com.vt.ml;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieLensRatingsMapper extends Mapper<LongWritable, Text, Text , Text>{
	private static Long rowNum = 0L;
	private final String views = "1";

	//ratings.csv
	//userId,movieId,rating,timestamp
	//3,2028,5,945141611
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String movieID = tokens[1];
		
		if(rowNum > 0) {
			context.write(new Text(movieID), new Text(views));
		}
		++rowNum;
	}

}
