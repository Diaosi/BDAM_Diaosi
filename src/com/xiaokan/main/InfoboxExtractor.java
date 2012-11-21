package com.xiaokan.main;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tools.bzip2.CBZip2InputStream;

import com.csvreader.CsvWriter;
import com.xiaokan.config.DataConstants;

public class InfoboxExtractor {

	/**
	 * @param args
	 * 
	 */

	private static BufferedReader fin;
	private static CsvWriter fout;
	private static StringBuilder sb;
	private static boolean debug = false;
	private static String pre = "<title>", suf = "</title>";
	private static String sep = System.getProperty("line.separator");
	private static String foutPath = "infobox.csv";

	public static void main(String[] args) throws Exception {

		if (args.length > 0) {
			for (String str : args) {
				if ("--debug".equals(str)) {
					debug = true;
				} else if (str.startsWith("-Path=")) {
					foutPath = str.substring(str.indexOf("-Path=") + 6);
				}
			}
		}
		// CSV Fileout
		fout = new CsvWriter(foutPath, ',', Charset.forName("UTF-8"));
//		for (String s3Path : DataConstants.filePaths) {
//			String localPath = download(s3Path);
//			processDataFile(localPath);
//		}
		processDataFile("e:/Datasets/Wikipedia/enwiki-20121001-pages-meta-current1.xml-p000000010p000010000.bz2");
		fout.flush();
		fout.close();

	}

	private static void processDataFile(String localPath) throws IOException {
		FileInputStream fis = new FileInputStream(localPath);
		fis.read();
		fis.read();
		fin = new BufferedReader(new InputStreamReader(new CBZip2InputStream(fis), "UTF-8"));
		// System.out.println(fin.readLine());
		String currentTitle = "";
		int cnt = 0;
		String line = null;
		while ((line = fin.readLine()) != null) {
			if ("<page>".equals(line.trim())) {
				String secondLine = fin.readLine();
				currentTitle = secondLine.substring(secondLine.indexOf(pre) + pre.length(), secondLine.indexOf(suf));
				if (debug) {
					System.out.println(currentTitle);
				}
			}
			if (line.trim().startsWith("{{Infobox")) {
				sb = new StringBuilder();
				sb.append(line);
				sb.append(sep);
				while (true) {
					line = fin.readLine().trim();
					sb.append(line);
					sb.append(sep);
					if ("}}".equals(line)) {
						sb.append(line);
						sb.append(sep);
						break;
					}
				}
				fout.writeRecord(new String[] { currentTitle, sb.toString() });
				sb = null;
			}

			line = null;
		}
		fout.flush();
		fin.close();
		System.out.println(localPath + " processed");

	}

	private static String download(String path) {
		String localPath = null;
		byte[] buf = new byte[10 * 1024 * 1024];
		try {
			// create url to file in s3
			URL url = new URL(path);
			localPath = DataConstants.localDataFromS3Path + url.getFile();

			// check if the file is already existed
			File f = new File(localPath);
			if (f.exists())
				return localPath;
			// download file
			InputStream is = url.openStream();
			BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(localPath));
			int len = 0;
			while (true) {
				len = is.read(buf, 0, buf.length);
				if (len == -1)
					break;
				fos.write(buf, 0, len);
			}
			fos.flush();
			is.close();
			fos.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("File: " + localPath + " downloaded");
		return localPath;

	}

}
