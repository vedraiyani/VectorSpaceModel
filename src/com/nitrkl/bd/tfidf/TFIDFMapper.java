package com.nitrkl.bd.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TFIDFMapper extends Mapper<LongWritable, Text, Text, DocumentFrequencyValue> {
	int i = 0;

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] lineTokens=ivalue.toString().split("\t");
		context.write(new Text(lineTokens[0]), new DocumentFrequencyValue(lineTokens[1],lineTokens[2]));
	}

}
