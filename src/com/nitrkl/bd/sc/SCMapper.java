package com.nitrkl.bd.sc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SCMapper extends Mapper<LongWritable, Text, Text, Text> {
	int i = 0;

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] lineTokens=ivalue.toString().split("\t");
		context.write(new Text(lineTokens[1]), new Text(lineTokens[2]));
	}

}
