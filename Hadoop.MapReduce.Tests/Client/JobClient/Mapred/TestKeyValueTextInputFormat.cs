using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestKeyValueTextInputFormat : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestKeyValueTextInputFormat
			).FullName);

		private static int MaxLength = 10000;

		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestKeyValueTextInputFormat()
		{
			try
			{
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "."), "data"), "TestKeyValueTextInputFormat");

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf();
			Path file = new Path(workDir, "test.txt");
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			int seed = new Random().Next();
			Log.Info("seed = " + seed);
			Random random = new Random(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 10) + 
				1)
			{
				Log.Debug("creating; entries = " + length);
				// create a file with length entries
				TextWriter writer = new OutputStreamWriter(localFs.Create(file));
				try
				{
					for (int i = 0; i < length; i++)
					{
						writer.Write(Sharpen.Extensions.ToString(i * 2));
						writer.Write("\t");
						writer.Write(Sharpen.Extensions.ToString(i));
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
				// try splitting the file in a variety of sizes
				KeyValueTextInputFormat format = new KeyValueTextInputFormat();
				format.Configure(job);
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / 20) + 1;
					Log.Debug("splitting: requesting = " + numSplits);
					InputSplit[] splits = format.GetSplits(job, numSplits);
					Log.Debug("splitting: got =        " + splits.Length);
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Length; j++)
					{
						Log.Debug("split[" + j + "]= " + splits[j]);
						RecordReader<Text, Text> reader = format.GetRecordReader(splits[j], job, reporter
							);
						Type readerClass = reader.GetType();
						NUnit.Framework.Assert.AreEqual("reader class is KeyValueLineRecordReader.", typeof(
							KeyValueLineRecordReader), readerClass);
						Text key = reader.CreateKey();
						Type keyClass = key.GetType();
						Text value = reader.CreateValue();
						Type valueClass = value.GetType();
						NUnit.Framework.Assert.AreEqual("Key class is Text.", typeof(Text), keyClass);
						NUnit.Framework.Assert.AreEqual("Value class is Text.", typeof(Text), valueClass);
						try
						{
							int count = 0;
							while (reader.Next(key, value))
							{
								int v = System.Convert.ToInt32(value.ToString());
								Log.Debug("read " + v);
								if (bits.Get(v))
								{
									Log.Warn("conflict with " + v + " in split " + j + " at position " + reader.GetPos
										());
								}
								NUnit.Framework.Assert.IsFalse("Key in multiple partitions.", bits.Get(v));
								bits.Set(v);
								count++;
							}
							Log.Debug("splits[" + j + "]=" + splits[j] + " count=" + count);
						}
						finally
						{
							reader.Close();
						}
					}
					NUnit.Framework.Assert.AreEqual("Some keys in no partition.", length, bits.Cardinality
						());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private LineReader MakeStream(string str)
		{
			return new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(
				str, "UTF-8")), defaultConf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUTF8()
		{
			LineReader @in = null;
			try
			{
				@in = MakeStream("abcd\u20acbdcd\u20ac");
				Text line = new Text();
				@in.ReadLine(line);
				NUnit.Framework.Assert.AreEqual("readLine changed utf8 characters", "abcd\u20acbdcd\u20ac"
					, line.ToString());
				@in = MakeStream("abc\u200axyz");
				@in.ReadLine(line);
				NUnit.Framework.Assert.AreEqual("split on fake newline", "abc\u200axyz", line.ToString
					());
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNewLines()
		{
			LineReader @in = null;
			try
			{
				@in = MakeStream("a\nbb\n\nccc\rdddd\r\neeeee");
				Text @out = new Text();
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line1 length", 1, @out.GetLength());
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line2 length", 2, @out.GetLength());
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line3 length", 0, @out.GetLength());
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line4 length", 3, @out.GetLength());
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line5 length", 4, @out.GetLength());
				@in.ReadLine(@out);
				NUnit.Framework.Assert.AreEqual("line5 length", 5, @out.GetLength());
				NUnit.Framework.Assert.AreEqual("end of file", 0, @in.ReadLine(@out));
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fs, Path name, CompressionCodec codec, string
			 contents)
		{
			OutputStream stm;
			if (codec == null)
			{
				stm = fs.Create(name);
			}
			else
			{
				stm = codec.CreateOutputStream(fs.Create(name));
			}
			stm.Write(Sharpen.Runtime.GetBytesForString(contents));
			stm.Close();
		}

		private static readonly Reporter voidReporter = Reporter.Null;

		/// <exception cref="System.IO.IOException"/>
		private static IList<Text> ReadSplit(KeyValueTextInputFormat format, InputSplit split
			, JobConf job)
		{
			IList<Text> result = new AList<Text>();
			RecordReader<Text, Text> reader = null;
			try
			{
				reader = format.GetRecordReader(split, job, voidReporter);
				Text key = reader.CreateKey();
				Text value = reader.CreateValue();
				while (reader.Next(key, value))
				{
					result.AddItem(value);
					value = (Text)reader.CreateValue();
				}
			}
			finally
			{
				if (reader != null)
				{
					reader.Close();
				}
			}
			return result;
		}

		/// <summary>Test using the gzip codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void TestGzip()
		{
			JobConf job = new JobConf();
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, job);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "line-1\tthe quick\nline-2\tbrown\nline-3\tfox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n"
				);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "line-1\tthis is a test\nline-1\tof gzip\n"
				);
			FileInputFormat.SetInputPaths(job, workDir);
			KeyValueTextInputFormat format = new KeyValueTextInputFormat();
			format.Configure(job);
			InputSplit[] splits = format.GetSplits(job, 100);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Length);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("part2.txt.gz"))
			{
				splits[0] = splits[1];
				splits[1] = tmp;
			}
			IList<Text> results = ReadSplit(format, splits[0], job);
			NUnit.Framework.Assert.AreEqual("splits[0] length", 6, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[0][5]", " dog", results[5].ToString());
			results = ReadSplit(format, splits[1], job);
			NUnit.Framework.Assert.AreEqual("splits[1] length", 2, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[1][0]", "this is a test", results[0].ToString
				());
			NUnit.Framework.Assert.AreEqual("splits[1][1]", "of gzip", results[1].ToString());
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestKeyValueTextInputFormat().TestFormat();
		}
	}
}
