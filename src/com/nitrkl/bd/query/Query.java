package com.nitrkl.bd.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Query {
	public String query = "";
	Map<String, Integer> terms = new HashMap<>();
	Map<String, Double> TFIDFs = new HashMap<>();
	double mod=0;
	
	private Path QUERYPATH;

	public Query(String query) throws IOException {
		this.query = query;

		String[] queryTermsArray = query.split(" ");

		for (String queryTerm : queryTermsArray) {
			int freq = containsTerm(queryTerm) ? (terms.get(queryTerm) + 1) : 1;
			terms.put(queryTerm, freq);
			TFIDFs.put(queryTerm, 0D);
		}
		writeMap();
	}

	public Query() throws IOException {
		readMap();
	}

	private void writeToFile(String str) throws IOException {
		Configuration conf = new Configuration(true);
		FileSystem fs = FileSystem.get(conf);
		QUERYPATH = new Path("/home/ved/query/query");
		if (fs.exists(QUERYPATH)) {
			fs.delete(QUERYPATH, true);
		}

		FSDataOutputStream out = fs.create(QUERYPATH);
		out.writeUTF(str);
		out.close();
	}

	private String readFromFile() throws IOException {
		// read query file
		Configuration conf = new Configuration(true);
		FileSystem fs = FileSystem.get(conf);
		QUERYPATH = new Path("/home/ved/query/query");
		if (!fs.exists(QUERYPATH)) {
			return null;
		}

		FSDataInputStream out = fs.open(QUERYPATH);
		String str = out.readUTF();
		out.close();
		return str;
	}

	private void writeMap() throws IOException {
		String str = "";
		for (Entry<String, Integer> term : terms.entrySet()) {
			str += term.getKey() + " " + term.getValue() + " " + TFIDFs.get(term.getKey()) + "\n";
		}
		writeToFile(str);
	}

	private void readMap() throws IOException {
		String str = readFromFile();
		this.query="";
		this.mod=0;
		String[] lines = str.split("\n");
		for (String line : lines) {
			if (line != null) {
				String[] termStr = line.split(" ");
				this.query+=termStr[0]+" ";
				int tf=Integer.parseInt(termStr[1]);
				double tfidf=Double.parseDouble(termStr[2]);
				terms.put(termStr[0], tf);
				TFIDFs.put(termStr[0], tfidf);
				this.mod+=tfidf*tfidf;
			}
		}
	}

	public Map<String, Integer> getTerms() {
		return terms;
	}

	public Integer getTerm(String key) {
		if (!containsTerm(key)) {
			return 0;
		}
		return terms.get(key);
	}

	public void setTerms(Map<String, Integer> terms) throws IOException {
		this.terms = terms;
		writeMap();
	}

	public void setTerm(String key, Integer value) throws IOException {
		terms.put(key, value);
		writeMap();
	}

	public Map<String, Double> getTFIDFs() {
		return TFIDFs;
	}

	public Double getTFIDFs(String key) {
		if (!containsTerm(key)) {
			return 0D;
		}
		return TFIDFs.get(key);
	}

	public void setTFIDFs(Map<String, Double> tFIDFs) throws IOException {
		TFIDFs = tFIDFs;
		writeMap();
	}

	public void setTFIDF(String key, Double value) throws IOException {
		TFIDFs.put(key, value);
		writeMap();
	}

	public double getMod() {
		return Math.sqrt(mod);
	}

	public void setMod(double mod) {
		this.mod = mod*mod;
	}

	public Boolean containsTerm(String key) {
		return terms.containsKey(key);
	}

	public String toString() {
		return query;
	}

	public void printMap() throws IOException {
		for (Entry<String, Integer> term : terms.entrySet()) {
			System.out.println(term.getKey() + " : " + term.getValue());
		}
		//System.out.println(readFromFile());
	}
}
