package com.github.diaosi.BDAM.outputformat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * An {@link OutputFormat} that writes plain text files.
 */
public class CsvOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\r\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		protected DataOutputStream out;
		private final byte[] keyValueSeparator;

		public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
			this.out = out;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		public LineRecordWriter(DataOutputStream out) {
			this(out, "\t");
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value) throws IOException {

			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			// if (!nullKey) {
			// writeObject(key);
			// }
			// if (!(nullKey || nullValue)) {
			// out.write(keyValueSeparator);
			// }
			if (!nullValue) {
				writeObject(value);
			}
			out.write(newline);
		}

		public synchronized void close(Reporter reporter) throws IOException {
			out.close();
		}
	}

	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = job.get("mapred.textoutputformat.separator", "\t");
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
		} else {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			// create the named codec
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
			// build the filename including the extension
			Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)),
					keyValueSeparator);
		}
	}
}
