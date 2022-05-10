package com.vt.xmlprocessor;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

public class XmlProcessorMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String xmlString = value.toString();
		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		
		try {
			Document doc = builder.build(in);
		
			Element root = doc.getRootElement();
		
			String nameTag = root.getChild("name").getTextTrim();
			String valueTag = root.getChild("value").getTextTrim();
			
			context.write(NullWritable.get(), new Text(nameTag +"," + valueTag));
		}
		catch (JDOMException | IOException e) {
			e.printStackTrace();
		}
	}
}
