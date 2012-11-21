package com.xiaokan.test;

import java.io.FileInputStream;
import java.nio.charset.Charset;

import org.apache.tools.bzip2.CBZip2InputStream;

public class TestZipInputStream {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String path = "e:/Datasets/Wikipedia/enwiki-20121001-pages-meta-current1.xml-p000000010p000010000.bz2";
		FileInputStream fis = new FileInputStream(path);
		fis.read();
		fis.read();
		CBZip2InputStream fin = new CBZip2InputStream(fis);
		byte[] buf = new byte[1024];
		int len = fin.read(buf);
		System.out.println(new String(buf, 0, len, Charset.forName("UTF-8")));

	}

}
