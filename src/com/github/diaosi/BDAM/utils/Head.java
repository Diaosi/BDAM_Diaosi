package com.github.diaosi.BDAM.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

public class Head {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String str = "https://s3.amazonaws.com/diaosi-mapreduce/output-cooc-output/part-00001?AWSAccessKeyId=ASIAIF7RB7PI2BJ6P3OA&Expires=1355262023&Signature=krjQ3C/r2Glldw3aw0oesyIKuGU%3D&x-amz-security-token=AQoDYXdzEGcakAKcbSO66cDYIGHM51hQRGSshMtgmAL4/b/fYmLEZdDdhAOti1yu4uztY7pqXSG8NtH4HFmwQc520cphaZ0QAs5PyW7tDlhzsB8/gpRwvpuDBIqBQZY2KuB4XQ45BQ1E9t6jNg6dskKoNL%2BMBeg9tOxUS33Dqz5a0rsFTPSEFAM0z16fyoHgqLa9fdt3wP9vifIl1dg%2By1RUKnXsDC28bGu49vL6FY5AobQwk7tIddSPNe/WbQ4g%2BkoAQNm0UZ1zAS2bl/G6aq67CMIuJe3mAhdc1AmCBYUHztNe0glMq7lYfj57LL6SLbwY1w5YqlH0y75VE9v0enmZfc2uX8NiNrZd/kj5MtANy0Qww14owJjwMSD3y56GBQ%3D%3D";
		URL url = new URL(str);
		InputStream is = url.openStream();
		BufferedReader fin = new BufferedReader(new InputStreamReader(is));
		for (int i = 0; i < 50; i++) {
			String line = fin.readLine();
			System.out.println(line);
		}
	}

}
