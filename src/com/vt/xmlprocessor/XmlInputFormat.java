package com.vt.xmlprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XmlInputFormat extends TextInputFormat{
	
	private static final Log LOG = LogFactory.getLog(
            XmlInputFormat.class);
	
	public static final String XML_START_TAG = "xml.start.tag";
	public static final String XML_END_TAG = "xml.end.tag";
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit ins, TaskAttemptContext tac) {
		
		return new XmlRecordReader();
	}
	
	public static class XmlRecordReader extends RecordReader<LongWritable, Text>{
		
		private byte[] startTag;
		private byte[] endTag;
		private Long start;
		private Long end;
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		
		
		@Override
		public void close() throws IOException {
			fsin.close();
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
		
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
	
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void initialize(InputSplit ins, TaskAttemptContext tac) throws IOException, InterruptedException {
			FileSplit fsplit = (FileSplit) ins;
			startTag = tac.getConfiguration().get("xml.start.tag").getBytes("utf-8");
			endTag = tac.getConfiguration().get("xml.end.tag").getBytes("utf-8");
			
			start = fsplit.getStart();
			end = start + fsplit.getLength();
			Path file = fsplit.getPath();
			
			LOG.info("start of fsplit " + start);
			LOG.info("end of fsplit " + end);
			
			FileSystem fs = file.getFileSystem(tac.getConfiguration());
			fsin = fs.open(file);
			fsin.seek(start);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				
				if (matchUntil(startTag, false)) {
					
					try {
						
						buffer.write(startTag);
						
						if (matchUntil(endTag, true)) {
							
							value.set(buffer.getData(), 0, buffer.getLength());
							key.set(fsin.getPos());
							return true;
						}
					}
					finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		public boolean matchUntil(byte[] tagOffsets, boolean withinBlock) throws IOException {
			int i = 0;
			
			while(true) {
				
				int b = fsin.read();
				
				if (b == -1) {
					return false;
				}
				
				if (withinBlock) {
					buffer.write(b);
				}
				
				if(b == tagOffsets[i]) {
					i++;
					if (i >= tagOffsets.length) {
						return true;
					}
				}
				else {
					i = 0;
				}
				if(!withinBlock && i == 0 && fsin.getPos() >= end) {
					return false;
				}
			}

		}
	
	}

}