package com.github.diaosi.BDAM.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.google.common.base.Splitter;

public class TestSplitFile {

	/**
	 * @param args
	 */
	private static String format = "%s|%d,";
	private static int MAX_NEIGHBORS = 30;

	private static String pat = "[^A-Za-z0-9]+";
	private static Splitter sp = Splitter.onPattern(pat).omitEmptyStrings();

	public static void main(String[] args) throws Exception {
		BufferedReader fin = new BufferedReader(new InputStreamReader(
				new FileInputStream("/hadoop/input/task3/0")));

		String str = fin.readLine().toLowerCase();
		Iterable<String> strs = sp.split(str);
		List<String> wordList = new ArrayList<String>();

		for (String s : strs)
			if (str.length() > 1 && str.length() < 50)
				wordList.add(s);

		for (int i = 0; i < wordList.size(); i++) {
			String x = wordList.get(i);
			Text k = new Text(x);
			for (int j = i + 1; j < Math
					.min(wordList.size(), i + MAX_NEIGHBORS); j++) {
				String y = wordList.get(j);
				if (x.compareTo(y) >= 0) {
					String t = x;
					x = y;
					y = t;
				}
				Text v = new Text(String.format(format, y, 1));
				System.out.println(k.toString() + " " + v.toString());
			}
		}

	}

}
