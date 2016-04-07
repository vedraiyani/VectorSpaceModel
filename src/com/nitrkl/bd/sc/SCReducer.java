package com.nitrkl.bd.sc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.nitrkl.bd.query.Query;

public class SCReducer extends Reducer<Text, TermTFIDFKey, Text, Text> {
	public static int TOTAL_IP_DOCS = 0;
	Query query;

	@Override
	protected void setup(Reducer<Text, TermTFIDFKey, Text, Text>.Context context) throws IOException, InterruptedException {
		query = new Query();
	}

	public void reduce(Text _key, Iterable<TermTFIDFKey> values, Context context) throws IOException, InterruptedException {
		double sc = 0;
		double dotProduct=0;
		double qMod=query.getMod();
		double dMod=0;

		for (TermTFIDFKey val : values) {
			double qtfidf=query.getTFIDFs(val.getTerm().toString());
			double tfidf=Double.parseDouble(val.getTFIDF().toString());

			dotProduct+=(qtfidf*tfidf);
			dMod+=(tfidf*tfidf);
		}
		
		sc=dotProduct/(qMod*Math.sqrt(dMod));
		context.write(_key, new Text(sc + ""));
	}

}
