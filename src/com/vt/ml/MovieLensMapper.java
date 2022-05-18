package com.vt.ml;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieLensMapper extends Mapper<LongWritable, Text, Text, Text>{
	
//	private String inputFileName;
	private Long rowNum = 0L;
//	protected void setup(Mapper<LongWritable, Text, Text, Text>. Context context) throws IOException, InterruptedException {
//		FileSplit filesplit = (FileSplit)context.getInputSplit();
//		inputFileName = filesplit.getPath().getName();
//	}
	
	//movies.csv
	//movieId,title,genres
	//1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String movieID = tokens[0];
		String title = tokens[1];
		if(rowNum > 0) {
			context.write(new Text(movieID), new Text(title));
		}
		++rowNum;
	}
}
