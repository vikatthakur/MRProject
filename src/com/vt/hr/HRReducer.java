package com.vt.hr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HRReducer extends Reducer<Text, Text, Text, Text>{
	

	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuffer buffer = new StringBuffer();
		int counter = 0;
		double tempSatisfactionLevel = 0.0;
		int tempNoProjects = 0;
		int tempMonthlyHours = 0;
		int tempLeftEmployees = 0;
		Set<String> salDistribution = new HashSet<String>();
		int tempPromotedEmployees = 0;
		
		
		for(Text val : values) {
			
			String[] tokens = val.toString().trim().split(",");
			
			if(tokens.length == 10) {
				
				//KPI-1 
				//1. Average satisfaction_level for individual Department.
				Double satisfactionLevel = Double.parseDouble(tokens[0]);
				tempSatisfactionLevel += satisfactionLevel;
				
				
				//KPI-2
				//2. How many employees are left in each individual Department?
				int leftEmployees = Integer.parseInt(tokens[6]);
				tempLeftEmployees += leftEmployees;
				
				
				//KPI-3
				//3. Department wise average monthly working hour.
				int monthlyHours = Integer.parseInt(tokens[3]);
				tempMonthlyHours += monthlyHours;
				
				
				//KPI-4
				//4. No of Project done by individual Department.
				int noProjects = Integer.parseInt(tokens[2]);
				tempNoProjects += noProjects;
				
				
				//KPI-5
				//5. Department wise salary Distribution.
				salDistribution.add(tokens[9]);
				
				
				//KPI-6
				//6. In individual Department How many Employees promoted in last 5 years but still left the company.
				int promotedEmployees = Integer.parseInt(tokens[7]);
				tempPromotedEmployees += promotedEmployees;
				
				++counter;
			}
		}
		
		buffer.append(tempSatisfactionLevel/counter).append(",");
		buffer.append(tempLeftEmployees).append(",");
		buffer.append(tempMonthlyHours/counter).append(",");
		buffer.append(tempNoProjects).append(",");
		buffer.append(salDistribution).append(",");
		buffer.append(tempPromotedEmployees);
		
		context.write(key, new Text(buffer.toString()));
	}

}
