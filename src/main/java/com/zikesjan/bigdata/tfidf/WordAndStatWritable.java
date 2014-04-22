package com.zikesjan.bigdata.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordAndStatWritable implements Writable{

	private Text document;
	private Text occurencePerDocument;
	private Text wordsInDocument;
	
	
	
	public WordAndStatWritable() {
		super();
		document = new Text();
		occurencePerDocument = new Text();
		wordsInDocument = new Text();
	}

	public WordAndStatWritable(Text document, Text occurencePerDocument,
			Text totalOccurencies) {
		super();
		this.document = document;
		this.occurencePerDocument = occurencePerDocument;
		this.wordsInDocument = totalOccurencies;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		document.write(out);
		occurencePerDocument.write(out);
		wordsInDocument.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		document.readFields(in);
		occurencePerDocument.readFields(in);
		wordsInDocument.readFields(in);
	}

	public Text getDocument() {
		return document;
	}

	public void setDocument(Text document) {
		this.document = document;
	}

	public Text getOccurencePerDocument() {
		return occurencePerDocument;
	}

	public void setOccurencePerDocument(Text occurencePerDocument) {
		this.occurencePerDocument = occurencePerDocument;
	}

	public Text getWordsInDocument() {
		return wordsInDocument;
	}

	public void setWordsInDocument(Text totalOccurencies) {
		this.wordsInDocument = totalOccurencies;
	}

	public void setDocumentByString(String document){
		this.document.set(document);
	}
	
	public void setOccurencePerDocumentByString(String value){
		this.occurencePerDocument.set(value);
	}
	
	public void setTotalOccurenciesByString(String value){
		this.wordsInDocument.set(value);
	}
}
