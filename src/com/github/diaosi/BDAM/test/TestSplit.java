package com.github.diaosi.BDAM.test;

import java.awt.BorderLayout;
import java.nio.charset.Charset;

import javax.swing.JFrame;

import com.csvreader.CsvReader;

public class TestSplit {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String s = "";
		String pat = "[|]";
		System.out.println(s.split(pat).length);
	}

}
