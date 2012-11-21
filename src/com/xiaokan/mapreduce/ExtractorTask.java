package com.xiaokan.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.csvreader.CsvWriter;
import com.xiaokan.hadoop.inputformat.XmlInputFormat;
import com.xiaokan.hadoop.outputformat.CsvOutputFormat;

public class ExtractorTask {

	/**
	 * @param args
	 */

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static String pre = "<title>", suf = "</title>";
		private static String sep = "\n";
		private static StringBuilder sb;

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> oc, Reporter reporter)
				throws IOException {
			BufferedReader fin = null;
			InputStream is = null;
			try {
				String s3Path = value.toString();
				URL url = new URL(s3Path);
				URLConnection conn = url.openConnection();
				conn.setConnectTimeout(20000);
				conn.setReadTimeout(20000);

				is = conn.getInputStream();
				fin = new BufferedReader(new InputStreamReader(
						new CBZip2InputStream(is), "UTF-8"));
				String currentTitle = "";
				// int cnt = 0;
				String line = null;
				StringWriter merged = null;
				CsvWriter writer;

				while ((line = fin.readLine()) != null) {
					if ("<page>".equals(line.trim())) {
						String secondLine = fin.readLine();
						currentTitle = new String(secondLine.substring(
								secondLine.indexOf(pre) + pre.length(),
								secondLine.indexOf(suf)));
						secondLine = null;
					}
					if (line.trim().startsWith("{{Infobox")) {
						sb = new StringBuilder();
						merged = new StringWriter();
						writer = new CsvWriter(merged, ',');
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
							reporter.progress();
						}
						writer.writeRecord(new String[] { currentTitle,
								sb.toString() });
						writer.flush();
						oc.collect(new Text(""), new Text(merged.toString()));
						reporter.progress();
						reporter.setStatus(value.toString() + " processed");
						sb = null;
						merged = null;
						writer = null;
					}

					line = null;
				}
			} catch (IOException ioe) {
				reporter.setStatus("This task didn't get fully passed");
			} finally {
				try {
					fin.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ExtractorTask.class);
		conf.setJobName("Wikipedia Extrator");
		conf.setMapperClass(MyMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(CsvOutputFormat.class);
		conf.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(conf, args[1]);
		CsvOutputFormat.setOutputPath(conf, new Path(args[2]));
		JobClient.runJob(conf);
	}

}
