package com.github.diaosi.BDAM.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestInfoboxAnalyzer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "Kazakhstan at the 2004 Summer Olympics,\"{{Infobox Olympics Kazakhstan";
		String mid = "{{Infobox ";
		String line = str;
		int pos = line.indexOf(mid);
		if (pos != -1) {
			int before = line.substring(0, pos).lastIndexOf(',');
			String id = line.substring(0, before);
			if (id.charAt(0) == '"')
				id = id.substring(1);
			if (id.endsWith("\""))
				id = id.substring(0, id.length() - 1);
			pos = pos + mid.length();
			String infobox = line.substring(pos).trim();
			if (infobox.endsWith("\"")) {
				infobox = infobox.substring(0, infobox.length() - 1);
			}
			System.out.println(infobox);
			System.out.println(id);
		}

	}
}
