package com.vt.empctc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmployeeCTCPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
	
		
		String[] tokens = value.toString().split("\\t");
		int partitionsNo = 0;
		String gender = tokens[3];
	
		if (numReduceTasks != 0) {
			
			if (gender.equals("female")) {
				partitionsNo = 0;
			}
			else if (gender.equals("male")) {
				partitionsNo = 1;
			}
		}
		
		return partitionsNo;
	}
	

}
