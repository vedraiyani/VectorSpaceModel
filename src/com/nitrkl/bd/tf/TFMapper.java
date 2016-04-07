package com.nitrkl.bd.tf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.nitrkl.bd.query.Query;
import com.nitrkl.bd.tfidf.DocumentFrequencyValue;

public class TFMapper extends Mapper<Object, Text, TermDocumentKey, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	Query query;

	@Override
	protected void setup(Mapper<Object, Text, TermDocumentKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		query = new Query();
	}

	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(ivalue.toString());

		FileSplit split = (FileSplit) context.getInputSplit();
		String filename = split.getPath().toString();
		if (filename.endsWith("~")) {
			return;
		}
		String[] filenameArray = filename.split("/");
		String document = filenameArray[filenameArray.length - 1];
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken().toLowerCase();
			if (!query.containsTerm(token)) {
				continue;
			}
			context.write(new TermDocumentKey(new Text(document), new Text(token)), one);
		}
	}

}
