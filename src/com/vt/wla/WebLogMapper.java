package com.vt.wla;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class WebLogMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	//21.125.155.111 - - [01/Jan/2012:12:07:48 +0530] "GET /digital-cameras/digital-camera/sony-qx-dsc-qx100-point-shoot-digital-camera-black.html HTTP/1.1" 200 1470 "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.17) Gecko/20110420 Firefox/3.6.17" "-"
	private static String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"([^\"]*)\" (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\"";
	private static int NUM_FIELDS = 9;
	private Pattern pattern = null;
	MultipleOutputs <NullWritable, Text> mos= null;
	
	protected void setup(Context context) {
		mos = new MultipleOutputs <NullWritable, Text> (context);
		pattern = Pattern.compile(LOG_PATTERN);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String inProcessedRecord = value.toString().replaceAll(ParserUtil.DELIMITER_TAB, "").trim();
		Matcher matcher = pattern.matcher(inProcessedRecord);
		
		if(matcher.matches() && NUM_FIELDS == matcher.groupCount()) {
			String requestString = matcher.group(5);
			String seperatedReqCatagory = getSeperateReqCatagories(requestString);
			
			StringBuffer outString = new StringBuffer();
			outString.append(matcher.group(1)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(2)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(3)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(4)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(5)).append(ParserUtil.DELIMITER_TAB);
			
			outString.append(seperatedReqCatagory).append(ParserUtil.DELIMITER_TAB);
			
			outString.append(matcher.group(6)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(7)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(8)).append(ParserUtil.DELIMITER_TAB);
			outString.append(matcher.group(9)).append(ParserUtil.DELIMITER_TAB);
			
			mos.write("ParsedRecords", NullWritable.get(), new Text(outString.toString()));
		}
		else {
			String badRecord = inProcessedRecord;
			mos.write("BadRecords", NullWritable.get(), new Text(badRecord));
		}
	}
	
	public String getSeperateReqCatagories(String requestString) {
		String[] splittedRequestString = requestString.split(" ");
		String seperatedReqCatagory = null;
		if(splittedRequestString.length == 3){
			seperatedReqCatagory = getProcessedRequest(splittedRequestString[1]);
		}
		else {
			seperatedReqCatagory = getProcessedDefaultRequest(splittedRequestString[1]);

		}
		
		return seperatedReqCatagory;
	
}

	private String getProcessedDefaultRequest(String string) {
		StringBuffer seperateRequestCatagories = new StringBuffer();
		
		seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
		seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
		seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
		seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
		seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH);
		
		return seperateRequestCatagories.toString();
	}

	public String getProcessedRequest(String requestCatagoryString) {
		StringBuffer seperateRequestCatagories = new StringBuffer();
		String[] processRequestCatagory  = requestCatagoryString.split("\\?");
		String paramString = "-";
		boolean paramFlag = false;
		if(processRequestCatagory.length == 2) {
			paramFlag = true;
			paramString = processRequestCatagory[1];
		}
		else if(processRequestCatagory.length > 2) {
			paramFlag = true;
			StringBuffer paramBuffer = new StringBuffer();
			for(int i = 1; i < processRequestCatagory.length; ++i) {
				paramBuffer.append(processRequestCatagory[i]);
				if(i < processRequestCatagory.length-1) {
					paramBuffer.append(ParserUtil.DELIMITER_QUESTIONMARK);
				}
			}
			paramString = paramBuffer.toString();
		}
		 //processing /a/b/c
		String catagoriesTokens[] = null;
		if(paramFlag) {
			catagoriesTokens = processRequestCatagory[0].split("/");
		}
		else {
			catagoriesTokens = requestCatagoryString.split("/");
		}
		
		int catagoriesLen = catagoriesTokens.length;
	
		//handling edge cases
		//case "/"
		if(catagoriesLen == 0) {
			seperateRequestCatagories.append("/").append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH);
		}
		//case "a/"
		else if(catagoriesLen == 1) {
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH);
		}
		//case "/abc.html"
		else if(catagoriesLen == 2) {
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[1]);
		}
		
		//case "/a/abc.html"
		else if(catagoriesLen == 3) {
			seperateRequestCatagories.append(catagoriesTokens[1]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[2]);
		}
		
		//case "/a/b/abc.html"
		else if(catagoriesLen == 4) {
			seperateRequestCatagories.append(catagoriesTokens[1]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[2]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[3]);
		}
		
		//case "/a/b/c/abc.html"
		else if(catagoriesLen == 5) {
			seperateRequestCatagories.append(catagoriesTokens[1]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[2]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[3]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(ParserUtil.DEFAULT_VALUE_DASH).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[4]);
		}
		
		//case "/a/b/c/d/abc.html"
		else if(catagoriesLen == 6) {
			seperateRequestCatagories.append(catagoriesTokens[1]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[2]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[3]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[4]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[5]);
		}
		
		//case "/a/b/c/d/,,/abc.html"
		else if(catagoriesLen > 6) {
			seperateRequestCatagories.append(catagoriesTokens[1]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[2]).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[3]).append(ParserUtil.DELIMITER_TAB);
			//appending everything to catagory 4 field
			StringBuffer catagoryBuffer = new StringBuffer();
			for(int i =4; i < catagoriesLen; ++i) {
				catagoryBuffer.append(catagoriesTokens[i]).append("/");
			}
			
			seperateRequestCatagories.append(catagoryBuffer).append(ParserUtil.DELIMITER_TAB);
			seperateRequestCatagories.append(catagoriesTokens[5]).append(ParserUtil.DELIMITER_TAB);
		}
		
		if(paramFlag) {
			seperateRequestCatagories.append(ParserUtil.DELIMITER_TAB).append(paramString);
		}
		else {
			seperateRequestCatagories.append(ParserUtil.DELIMITER_TAB).append(ParserUtil.DEFAULT_VALUE_DASH);
		}
		

		return seperateRequestCatagories.toString();
	}
}	
