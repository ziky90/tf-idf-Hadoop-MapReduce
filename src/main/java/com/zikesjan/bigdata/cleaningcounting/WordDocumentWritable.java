package com.zikesjan.bigdata.cleaningcounting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordDocumentWritable implements WritableComparable<WordDocumentWritable>{
	
	private Text word;
	private Text document;

	public WordDocumentWritable(){
		word = new Text();
		document = new Text();
	}
	
	public WordDocumentWritable(Text word, Text document) {
		super();
		this.word = word;
		this.document = document;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
		document.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		document.readFields(in);
	}

	public Text getWord() {
		return word;
	}

	public Text getDocument() {
		return document;
	}

	public void setWord(Text word) {
		this.word = word;
	}
	
	public void setWordByStrings(String value){
		this.word.set(value);
	}
	
	public void setDocumentByString(String document){
		this.document.set(document);
	}

	public void setDocument(Text document) {
		this.document = document;
	}

	@Override
	public int compareTo(WordDocumentWritable o) {
		if(word.compareTo(o.word) == 0) return document.compareTo(o.document);
		else return word.compareTo(o.word);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((document == null) ? 0 : document.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WordDocumentWritable other = (WordDocumentWritable) obj;
		if (document == null) {
			if (other.document != null)
				return false;
		} else if (!document.equals(other.document))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return word + " " + document;
	}
	
	
}
