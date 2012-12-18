package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class WordsFilter {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_sorted_opt.txt")), 1024 * 1024 * 100);
		HashSet<String> set = new HashSet<String>();
		BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream("e:/dict.txt")));
		BufferedWriter fout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_sorted_compressed.txt")));
		String line = null;
		int cnt = 1;
		while ((line = fin.readLine()) != null) {
			set.add(line.toLowerCase());
		}
		fin.close();
		List<String> words = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
			line = line.toLowerCase();
			if (set.contains(line)) {
				words.add(line);
				set.remove(line);
			}
		}
		br.close();

		Collections.sort(words);
		for (String str : words) {
			fout.write(str);
			fout.write(System.getProperty("line.separator"));
		}
		fout.flush();
		fout.close();

	}

}
