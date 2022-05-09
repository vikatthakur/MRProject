package com.vt.adv.fj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinWritable implements Writable{
	
	private Text movalue = null;
	private Text fileName = null;
	
	public JoinWritable() {
		set(new Text(), new Text());
	}
	
	public JoinWritable(String movalue, String fileName) {
		set(new Text(movalue), new Text(fileName));
	}

	public JoinWritable(Text movalue, Text fileName) {
		set(movalue, fileName);
	}
	
	public void set(Text movalue, Text fileName) {
		this.movalue = movalue;
		this.fileName = fileName;		
	}
	
	public Text getMovalue() {
		return movalue;
	}

	public Text getFileName() {
		return fileName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movalue.readFields(in);
		fileName.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		movalue.write(out);
		fileName.write(out);
	}
	
	public int hashCode() {
		return movalue.hashCode() * 163 + fileName.hashCode();
	}
	

}
