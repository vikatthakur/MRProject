package com.vt.adv.fj;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileJoinAdvMapper extends Mapper<LongWritable, Text, Text, JoinWritable>{
	
	String inputFileName;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, JoinWritable>.Context context) throws IOException, InterruptedException {
		FileSplit filesplit = (FileSplit)context.getInputSplit();
		inputFileName = filesplit.getPath().getName();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String [] tokens = value.toString().split(",");
		if(tokens.length == 2) {
			System.out.println("file name is : " + inputFileName);
			context.write(new Text(tokens[0]), new JoinWritable(new Text(tokens[1]), new Text(inputFileName)));
		}
		

		
			

	}
	
	

}
