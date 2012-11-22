package com.github.diaosi.BDAM.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;

public class TestCBZip2InputStream {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		URL url = new URL(
				"https://s3.amazonaws.com/cs9223/enwiki-20121001/enwiki-20121001-pages-meta-current1.xml-p000000010p000010000.bz2");

		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		is.read();
		is.read();
		CBZip2InputStream cis = new CBZip2InputStream(is);
		InputStreamReader isr = new InputStreamReader(cis);
		BufferedReader fin = new BufferedReader(isr);
		for (int i = 0; i < 10; i++) {
			String line = fin.readLine();
			System.out.println(line);
		}
	}

}
