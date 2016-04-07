package com.nitrkl.bd.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.nitrkl.bd.query.Query;
import com.nitrkl.bd.tf.TermDocumentKey;

public class TFIDFReducer extends Reducer<Text, DocumentFrequencyValue, Text, Text> {
	public static int TOTAL_IP_DOCS = 0;
	Query query;

	@Override
	protected void setup(Reducer<Text, DocumentFrequencyValue, Text, Text>.Context context)
			throws IOException, InterruptedException {
		query = new Query();
	}

	public void reduce(Text _key, Iterable<DocumentFrequencyValue> values, Context context)
			throws IOException, InterruptedException {

		// Set<String> documents = new HashSet<>();

		ArrayList<DocumentFrequencyValue> documentList = new ArrayList<>();

		// count document frequency
		for (DocumentFrequencyValue val : values) {
			String document = val.getDocument().toString();
			String frequency = val.getFrequency().toString();
			documentList.add(new DocumentFrequencyValue(document, frequency));
		}

		// count idf
		double idf = Math.log10((double) TOTAL_IP_DOCS / documentList.size());

		// count for query
		double qtfidf = 0;

		qtfidf = query.getTerm(_key.toString()) * idf;
		query.setTFIDF(_key.toString(), qtfidf);
//		context.write(_key, new Text("q \t" + qtfidf));
		
		// count and emit tf*idf for documents
		for (DocumentFrequencyValue val : documentList) {
			double tfidf = Double.parseDouble(val.getFrequency().toString()) * idf;
			context.write(_key, new Text(val.getDocument() + "\t" + tfidf));
		}

		// count and emit document_term(tf*idf)*query_term(tf*idf) for documents
		// for (DocumentFrequencyValue val : documentList) {
		// tfidf = Double.parseDouble(val.getFrequency().toString()) * idf;
		// context.write(_key, new Text(val.getDocument() + "\t" +
		// (tfidf*qtfidf)));
		// }

		// emit df and idf of term
		// context.write(_key, new Text(documents.size() + " " +idf));
	}

}
