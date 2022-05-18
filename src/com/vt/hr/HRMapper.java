package com.vt.hr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HRMapper extends Mapper<Object, Text, Text, Text>{
	private int recordNum = 0;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//satisfaction_level	last_evaluation	number_project	average_montly_hours	time_spend_company	Work_accident	left	promotion_last_5years	sales	salary
		//0.38	0.53	2	157	3	0	1	0	sales	low
		
		if(recordNum > 0) {
		
			String[] tokens = value.toString().split(",");
			
			if(tokens.length == 10) {
				System.out.println("Mapper Log :"  + value.toString());
				context.write(new Text(tokens[8]), value);
			}
		
		}
		++recordNum;
		
	}

}
