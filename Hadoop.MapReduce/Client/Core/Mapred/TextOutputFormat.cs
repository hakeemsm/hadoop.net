using System;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="OutputFormat{K, V}"/>
	/// that writes plain text files.
	/// </summary>
	public class TextOutputFormat<K, V> : FileOutputFormat<K, V>
	{
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
			public virtual void Write(K key, V value)
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
			public virtual void Close(Reporter reporter)
			{
				lock (this)
				{
					@out.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
			, string name, Progressable progress)
		{
			bool isCompressed = GetCompressOutput(job);
			string keyValueSeparator = job.Get("mapreduce.output.textoutputformat.separator", 
				"\t");
			if (!isCompressed)
			{
				Path file = FileOutputFormat.GetTaskOutputPath(job, name);
				FileSystem fs = file.GetFileSystem(job);
				FSDataOutputStream fileOut = fs.Create(file, progress);
				return new TextOutputFormat.LineRecordWriter<K, V>(fileOut, keyValueSeparator);
			}
			else
			{
				Type codecClass = GetOutputCompressorClass(job, typeof(GzipCodec));
				// create the named codec
				CompressionCodec codec = ReflectionUtils.NewInstance(codecClass, job);
				// build the filename including the extension
				Path file = FileOutputFormat.GetTaskOutputPath(job, name + codec.GetDefaultExtension
					());
				FileSystem fs = file.GetFileSystem(job);
				FSDataOutputStream fileOut = fs.Create(file, progress);
				return new TextOutputFormat.LineRecordWriter<K, V>(new DataOutputStream(codec.CreateOutputStream
					(fileOut)), keyValueSeparator);
			}
		}
	}
}
