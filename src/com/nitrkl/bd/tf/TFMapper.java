package com.nitrkl.bd.tf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFMapper extends Mapper<Object, Text, TermDocumentWritable, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(ivalue.toString());

		FileSplit split = (FileSplit) context.getInputSplit();
		String[] filenameArray = split.getPath().toString().split("/");
		String document = filenameArray[filenameArray.length - 1];

		while (itr.hasMoreTokens()) {
			context.write(new TermDocumentWritable(new Text(document),new Text(itr.nextToken())), one);
		}
	}

}
