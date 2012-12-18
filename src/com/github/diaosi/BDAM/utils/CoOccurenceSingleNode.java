package com.github.diaosi.BDAM.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import com.google.common.base.Splitter;

public class CoOccurenceSingleNode {

	/**
	 * Number of unique words : 70184 Load factor: 0.75. Therefore
	 * initialCapacity = 71139/0.75 = 93579
	 */
	private static HashMap<String, Integer> map = new HashMap<String, Integer>(93579);
	private static HashMap<Integer, Integer>[] counts = new HashMap[70184];
	private static int CACHE_THRESHOLD = 10;
	private static String pathPattern = "E:\\Datasets\\Wikipedia\\Task3\\intermediate\\part-%d.dat";
	private static int fileCount = 0;
	private static BufferedOutputStream fout = null;
	private static int MAX_WORD_NEIGHBORS = 50;

	public static void main(String[] args) throws Exception {

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_without_stopwords.txt")), 1024 * 1024);
		String line = null;
		int cnt = 1;
		while ((line = br.readLine()) != null) {
			map.put(line, cnt++);
		}

		int countOfLines = 0;
		String pat = "[^A-Za-z0-9]+";
		Splitter sp = Splitter.onPattern(pat).omitEmptyStrings();
		boolean[] count = new boolean[71148];

		for (int i = 0; i < counts.length; i++)
			counts[i] = new HashMap<Integer, Integer>();

		BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\part-00000")), 1024 * 1024 * 256);
		ArrayList<Integer> ids = new ArrayList<Integer>();
		while (true) {
			line = fin.readLine();
			if (countOfLines % 10000 == 0) {
				System.out.println("Number of 10K lines: " + (countOfLines / 10000));
				if ((countOfLines / 10000) % CACHE_THRESHOLD == 0) {
					flushHashMapsOutAndClear(countOfLines);
				}
			}
			countOfLines++;
			if (line == null) {
				break;
			}
			int pos = 0;
			while (line.charAt(pos++) != '\t') {
				;
			}
			String text = line.substring(pos);
			Iterable<String> iter = sp.split(text);
			Arrays.fill(count, false);
			ids.clear();
			for (String word : iter) {
				Integer i = map.get(word.toLowerCase());
				if (i == null)
					continue;
				if (!count[i]) {
					ids.add(i);
					count[i] = true;
				}

			}

			// System.out.println(ids.size());
			for (int i = 0; i < ids.size(); i++)
				for (int j = i + 1; j < Math.min(i + MAX_WORD_NEIGHBORS, ids.size()); j++) {
					int x = ids.get(i);
					int y = ids.get(j);
					if (x > y) {
						int t = x;
						x = y;
						y = t;
					}
					Integer cc = counts[x].get(y);
					if (cc == null)
						cc = 1;
					else
						cc += 1;
					counts[x].put(y, cc);
					// fout.write(int2bytes(ids.get(i)));
					// fout.write(int2bytes(ids.get(j)));
				}
			iter = null;
		}
		flushHashMapsOutAndClear(countOfLines);
		fout.flush();
		fout.close();

	}

	private static void flushHashMapsOutAndClear(int countOfLines) throws IOException {
		if (fout != null) {
			fout.flush();
			fout.close();
		}
		fout = new BufferedOutputStream(new FileOutputStream(String.format(pathPattern, fileCount)), 1024 * 1024 * 128);
		System.out.println("Count of intermediate files: " + fileCount);
		fileCount++;
		if (countOfLines == 0)
			return;
		for (int x = 0; x < counts.length; x++) {
			HashMap<Integer, Integer> map = counts[x];
			for (Entry<Integer, Integer> e : map.entrySet()) {
				fout.write(int2bytes(x));
				fout.write(int2bytes(e.getKey()));
				fout.write(int2bytes(e.getValue()));
			}
			map.clear();
		}

	}

	private static int MASK = 0xff;

	public static byte[] int2bytes(int v) {
		byte b[] = new byte[4];
		b[3] = (byte) ((v >>> 24) & 0xFF);
		b[2] = (byte) ((v >>> 16) & 0xFF);
		b[1] = (byte) ((v >>> 8) & 0xFF);
		b[0] = (byte) ((v >>> 0) & 0xFF);
		return b;
	}

}
