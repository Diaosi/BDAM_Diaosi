package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class Baoku {

	/**
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String[] args) throws Exception {
		String username = "骗子死全家";
		String pwd = "操你妈死骗子";

		String url = "http://83379.qsdef.us/2608efgtv/lin.asp?username=%s&pwd=%s";
		URL u = new URL(String.format(url, username, pwd));
		URLConnection conn = null;
		for (int i = 0; i < 10000; i++) {
			conn = u.openConnection();
			conn.connect();
			System.out.println(i);
		}
		BufferedReader fout = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		System.out.println(fout.readLine());

	}

}
