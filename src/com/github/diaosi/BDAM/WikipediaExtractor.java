package com.github.diaosi.BDAM;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.tools.bzip2.CBZip2InputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class WikipediaExtractor {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		byte[] buf = new byte[1024 * 1024 * 42];

		for (String s3Path : DataConstants.filePaths) {
			ExtractorThread t = new ExtractorThread(s3Path);
			t.start();

		}

	}

	private static class ExtractorThread extends Thread {

		private String s3Path;
		private byte[] buf = new byte[1024 * 1024];

		public ExtractorThread(String s3Path) {
			super();
			this.s3Path = s3Path;
		}

		@Override
		public void run() {
			String fileName = null;
			try {
				// Init Amazon S3
				AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials(
						"AKIAI42VQL6SUU5S27SA",
						"/P3g0/U/hnKgWKRMsRXtG1w+gM+H0dwtLPnuvOBG"));
				String bucketName = "diaosi-mapreduce";

				// Download and Extract
				fileName = s3Path.substring(s3Path.indexOf("enwiki-20121001-"));
				fileName = fileName.substring(0, fileName.indexOf("bz2"))
						+ ".xml";
				FileOutputStream fout = new FileOutputStream(fileName);

				URL url = new URL(s3Path);
				URLConnection conn = url.openConnection();
				conn.setConnectTimeout(20000);
				conn.setReadTimeout(20000);
				InputStream is = conn.getInputStream();
				is.read();
				is.read();
				CBZip2InputStream fin = new CBZip2InputStream(is);
				while (true) {
					int len = fin.read(buf, 0, buf.length);
					System.out.println(fileName + ": " + len);
					if (len == -1)
						break;
					fout.write(buf, 0, len);
				}
				fout.flush();
				fout.close();
				fin.close();

				// Upload to S3
				File file = new File(fileName);
				String key = fileName;
				s3.putObject(new PutObjectRequest(bucketName, key, file));

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

}
