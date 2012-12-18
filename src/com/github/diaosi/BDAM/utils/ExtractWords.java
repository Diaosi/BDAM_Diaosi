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

import com.google.common.base.Splitter;

public class ExtractWords {

	/**
	 * @param args
	 */

	private static HashSet<String> set = new HashSet<String>(1024 * 1024 * 40);

	public static void main(String[] args) throws Exception {

		String pat = "[^A-Za-z0-9]+";
		Splitter sp = Splitter.onPattern(pat).omitEmptyStrings();
		BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\part-00000")), 1024 * 1024 * 256);
		String line = null;
		int countOfLines = 0;
		ArrayList<String> words = new ArrayList<String>();
		long before = System.currentTimeMillis();
		while (true) {
			line = fin.readLine();
			countOfLines++;
			if (countOfLines % 10000 == 0) {
				System.out.println("Number of 10K lines: " + (countOfLines / 10000));
			}
			if (line == null) {
				break;
			}
			int pos = 0;
			while (line.charAt(pos++) != '\t') {
				;
			}
			String text = line.substring(pos);
			Iterable<String> iter = sp.split(text);
			for (String word : iter) {
				String newWord = new String(word);
				if (set.add(newWord)) {
					words.add(newWord);
					if (set.size() % 10000 == 0) {
						System.out.println("Number of 10K Words: " + set.size() / 10000);
					}
				}
			}
			iter = null;
		}

		System.out.format("File reading finished, %d words\n", set.size());
		Collections.sort(words);
		BufferedWriter fout = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_sorted_opt.txt")), 1024 * 1024 * 32);
		for (String word : words) {
			fout.write(word);
			fout.write("\n");
		}
		fout.flush();
		fout.close();
		long after = System.currentTimeMillis();
		System.out.println("Total time: " + (after - before) / 1024 / 60 + " mins");

	}
}
