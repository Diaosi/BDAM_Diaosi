package com.github.diaosi.BDAM.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;

public class TestRegularExpression {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String pat = "[\\~\\`\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\_\\-\\+\\=\\|\\\\\\\\}\\]\\{\\[\\:\\;\\'\\\\\"\\?\\/\\>\\.\\<\\,\\ ]";
		String str = "Highest estimate Iris Chang, ''The Rape of Nanking (1997)'', citing James Yin & Shi Young: 400,000 |The  pillaged and burned  for six weeks while, at the same time, systematically raping, murdering, enslaving, and torturing  and civilians.Justin Harmon [http://www.princeton.edu/pr/news/97/q4/1112-nanking.html";
		String[] s = str.split(pat);
		for (String st : s) {
			if (!"".equals(st)) {
				System.out.println(st);
			}
		}
	}

}
