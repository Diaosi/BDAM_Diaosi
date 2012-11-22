package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class BigFileSplit {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		File file = new File(args[0]);
		long length = file.length() / 2;
		System.out.println(length);
		// String sep = System.getProperty("line.separator");
		FileInputStream fis = new FileInputStream(args[0]);
		InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
		BufferedReader fin = new BufferedReader(isr);

		FileOutputStream fos = new FileOutputStream(args[0] + ".part0");
		OutputStreamWriter osr = new OutputStreamWriter(fos, "UTF-8");
		BufferedWriter fout = new BufferedWriter(osr);

		String line = null;
		long count = 0;
		boolean switched = false;
		while ((line = fin.readLine()) != null) {
			fout.write(line + '\n');
			count += line.length() + 1;
			if ("</page>".equals(line.trim())) {
				// System.out.format("%d, %d\n", count, length);
				if (count > length && !switched) {
					switched = true;
					fout.flush();
					fout.close();
					fos = new FileOutputStream(args[0] + ".part1");
					osr = new OutputStreamWriter(fos, "UTF-8");
					fout = new BufferedWriter(osr);
				}
			}
		}
		System.out.println(count);
		fout.flush();
		fout.close();

	}
}
