package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.TreeSet;

public class RemoveStopWords {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_sorted_compressed.txt")), 1024 * 1024);
		TreeSet<String> set = new TreeSet<String>();
		String line = null;
		while ((line = br.readLine()) != null) {
			set.add(line.trim().toLowerCase());
		}
		br = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\stopwords.txt")), 1024 * 1024);

		while ((line = br.readLine()) != null) {
			String word = line.trim().toLowerCase();
			// System.out.println(word);
			if (set.contains(word)) {
				System.out.println(word);
				set.remove(word);
			}
		}
		OutputStreamWriter fout = new OutputStreamWriter(new FileOutputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_without_stopwords.txt"));
		Iterator<String> iter = set.iterator();
		while (iter.hasNext()) {
			fout.write(iter.next());
			fout.write(System.getProperty("line.separator"));
		}

	}

}
