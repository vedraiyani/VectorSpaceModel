package com.nitrkl.bd.sc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TermTFIDFKey implements Writable{
	private Text term;
	private Text TFIDF;
	public TermTFIDFKey() {
		term=new Text();
		TFIDF=new Text();
	}
	public TermTFIDFKey(Text term, Text TFIDF) {
		super();
		this.term = term;
		this.TFIDF = TFIDF;
	}
	public TermTFIDFKey(String term, String TFIDF) {
		this.term = new Text(term);
		this.TFIDF = new Text(TFIDF);
	}
	public Text getTFIDF() {
		return TFIDF;
	}
	public void setTFIDF(Text TFIDF) {
		this.TFIDF = TFIDF;
	}
	public void setTFIDFAsString(String TFIDF) {
		this.TFIDF = new Text(TFIDF);
	}
	public Text getTerm() {
		return term;
	}
	public void setTerm(Text term) {
		this.term = term;
	}
	public void setTermAsString(String term) {
		this.term = new Text(term);
	}
	public void write(DataOutput out) throws IOException {
		TFIDF.write(out);
		term.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		TFIDF.readFields(in);
		term.readFields(in);
	}

	public int compareTo(TermTFIDFKey o) {
		if(TFIDF.compareTo(o.TFIDF) == 0) return term.compareTo(o.term);
		else return TFIDF.compareTo(o.TFIDF);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((term == null) ? 0 : term.hashCode());
		result = prime * result + ((TFIDF == null) ? 0 : TFIDF.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TermTFIDFKey other = (TermTFIDFKey) obj;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		if (TFIDF == null) {
			if (other.TFIDF != null)
				return false;
		} else if (!TFIDF.equals(other.TFIDF))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return TFIDF + "\t" + term;
	}

}
