using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
	/// that writes plain text files.
	/// </summary>
	public class TextOutputFormat<K, V> : FileOutputFormat<K, V>
	{
		public static string Seperator = "mapreduce.output.textoutputformat.separator";

		protected internal class LineRecordWriter<K, V> : RecordWriter<K, V>
		{
			private const string utf8 = "UTF-8";

			private static readonly byte[] newline;

			static LineRecordWriter()
			{
				try
				{
					newline = Sharpen.Runtime.GetBytesForString("\n", utf8);
				}
				catch (UnsupportedEncodingException)
				{
					throw new ArgumentException("can't find " + utf8 + " encoding");
				}
			}

			protected internal DataOutputStream @out;

			private readonly byte[] keyValueSeparator;

			public LineRecordWriter(DataOutputStream @out, string keyValueSeparator)
			{
				this.@out = @out;
				try
				{
					this.keyValueSeparator = Sharpen.Runtime.GetBytesForString(keyValueSeparator, utf8
						);
				}
				catch (UnsupportedEncodingException)
				{
					throw new ArgumentException("can't find " + utf8 + " encoding");
				}
			}

			public LineRecordWriter(DataOutputStream @out)
				: this(@out, "\t")
			{
			}

			/// <summary>
			/// Write the object to the byte stream, handling Text as a special
			/// case.
			/// </summary>
			/// <param name="o">the object to print</param>
			/// <exception cref="System.IO.IOException">if the write throws, we pass it on</exception>
			private void WriteObject(object o)
			{
				if (o is Text)
				{
					Text to = (Text)o;
					@out.Write(to.GetBytes(), 0, to.GetLength());
				}
				else
				{
					@out.Write(Sharpen.Runtime.GetBytesForString(o.ToString(), utf8));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(K key, V value)
			{
				lock (this)
				{
					bool nullKey = key == null || key is NullWritable;
					bool nullValue = value == null || value is NullWritable;
					if (nullKey && nullValue)
					{
						return;
					}
					if (!nullKey)
					{
						WriteObject(key);
					}
					if (!(nullKey || nullValue))
					{
						@out.Write(keyValueSeparator);
					}
					if (!nullValue)
					{
						WriteObject(value);
					}
					@out.Write(newline);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close(TaskAttemptContext context)
			{
				lock (this)
				{
					@out.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext job)
		{
			Configuration conf = job.GetConfiguration();
			bool isCompressed = GetCompressOutput(job);
			string keyValueSeparator = conf.Get(Seperator, "\t");
			CompressionCodec codec = null;
			string extension = string.Empty;
			if (isCompressed)
			{
				Type codecClass = GetOutputCompressorClass(job, typeof(GzipCodec));
				codec = (CompressionCodec)ReflectionUtils.NewInstance(codecClass, conf);
				extension = codec.GetDefaultExtension();
			}
			Path file = GetDefaultWorkFile(job, extension);
			FileSystem fs = file.GetFileSystem(conf);
			if (!isCompressed)
			{
				FSDataOutputStream fileOut = fs.Create(file, false);
				return new TextOutputFormat.LineRecordWriter<K, V>(fileOut, keyValueSeparator);
			}
			else
			{
				FSDataOutputStream fileOut = fs.Create(file, false);
				return new TextOutputFormat.LineRecordWriter<K, V>(new DataOutputStream(codec.CreateOutputStream
					(fileOut)), keyValueSeparator);
			}
		}
	}
}
