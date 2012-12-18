package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class WordLengthCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
				"E:\\Datasets\\Wikipedia\\Task3\\words_sorted_opt.txt")), 1024 * 1024);
		int[] count = new int[10000000];
		String line = null;
		int cnt = 1;
		while ((line = br.readLine()) != null) {

			count[line.length()]++;
			cnt++;
		}
		for (int i = 0; i < count.length; i++)
			if (count[i] != 0) {
				System.out.format("%d\n", count[i]);
			}

	}

}
