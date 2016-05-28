using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTextInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestTextInputFormat).FullName
			);

		private static int MaxLength = 10000;

		private static JobConf defaultConf = new JobConf();

		private static FileSystem localFs = null;

		static TestTextInputFormat()
		{
			try
			{
				defaultConf.Set("fs.defaultFS", "file:///");
				localFs = FileSystem.GetLocal(defaultConf);
			}
			catch (IOException e)
			{
				throw new RuntimeException("init failure", e);
			}
		}

		private static Path workDir = new Path(new Path(Runtime.GetProperty("test.build.data"
			, "/tmp")), "TestTextInputFormat").MakeQualified(localFs);

		/// <exception cref="System.Exception"/>
		public virtual void TestFormat()
		{
			JobConf job = new JobConf(defaultConf);
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
						writer.Write(Sharpen.Extensions.ToString(i));
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
				// try splitting the file in a variety of sizes
				TextInputFormat format = new TextInputFormat();
				format.Configure(job);
				LongWritable key = new LongWritable();
				Text value = new Text();
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / 20) + 1;
					Log.Debug("splitting: requesting = " + numSplits);
					InputSplit[] splits = format.GetSplits(job, numSplits);
					Log.Debug("splitting: got =        " + splits.Length);
					if (length == 0)
					{
						NUnit.Framework.Assert.AreEqual("Files of length 0 are not returned from FileInputFormat.getSplits()."
							, 1, splits.Length);
						NUnit.Framework.Assert.AreEqual("Empty file length == 0", 0, splits[0].GetLength(
							));
					}
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Length; j++)
					{
						Log.Debug("split[" + j + "]= " + splits[j]);
						RecordReader<LongWritable, Text> reader = format.GetRecordReader(splits[j], job, 
							reporter);
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
		public virtual void TestSplitableCodecs()
		{
			JobConf conf = new JobConf(defaultConf);
			int seed = new Random().Next();
			// Create the codec
			CompressionCodec codec = null;
			try
			{
				codec = (CompressionCodec)ReflectionUtils.NewInstance(conf.GetClassByName("org.apache.hadoop.io.compress.BZip2Codec"
					), conf);
			}
			catch (TypeLoadException)
			{
				throw new IOException("Illegal codec!");
			}
			Path file = new Path(workDir, "test" + codec.GetDefaultExtension());
			// A reporter that does nothing
			Reporter reporter = Reporter.Null;
			Log.Info("seed = " + seed);
			Random random = new Random(seed);
			FileSystem localFs = FileSystem.GetLocal(conf);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(conf, workDir);
			int MaxLength = 500000;
			// for a variety of lengths
			for (int length = MaxLength / 2; length < MaxLength; length += random.Next(MaxLength
				 / 4) + 1)
			{
				Log.Info("creating; entries = " + length);
				// create a file with length entries
				TextWriter writer = new OutputStreamWriter(codec.CreateOutputStream(localFs.Create
					(file)));
				try
				{
					for (int i = 0; i < length; i++)
					{
						writer.Write(Sharpen.Extensions.ToString(i));
						writer.Write("\n");
					}
				}
				finally
				{
					writer.Close();
				}
				// try splitting the file in a variety of sizes
				TextInputFormat format = new TextInputFormat();
				format.Configure(conf);
				LongWritable key = new LongWritable();
				Text value = new Text();
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / 2000) + 1;
					Log.Info("splitting: requesting = " + numSplits);
					InputSplit[] splits = format.GetSplits(conf, numSplits);
					Log.Info("splitting: got =        " + splits.Length);
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Length; j++)
					{
						Log.Debug("split[" + j + "]= " + splits[j]);
						RecordReader<LongWritable, Text> reader = format.GetRecordReader(splits[j], conf, 
							reporter);
						try
						{
							int counter = 0;
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
								counter++;
							}
							if (counter > 0)
							{
								Log.Info("splits[" + j + "]=" + splits[j] + " count=" + counter);
							}
							else
							{
								Log.Debug("splits[" + j + "]=" + splits[j] + " count=" + counter);
							}
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
		private static LineReader MakeStream(string str)
		{
			return new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(
				str, "UTF-8")), defaultConf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static LineReader MakeStream(string str, int bufsz)
		{
			return new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(
				str, "UTF-8")), bufsz);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUTF8()
		{
			LineReader @in = MakeStream("abcd\u20acbdcd\u20ac");
			Text line = new Text();
			@in.ReadLine(line);
			NUnit.Framework.Assert.AreEqual("readLine changed utf8 characters", "abcd\u20acbdcd\u20ac"
				, line.ToString());
			@in = MakeStream("abc\u200axyz");
			@in.ReadLine(line);
			NUnit.Framework.Assert.AreEqual("split on fake newline", "abc\u200axyz", line.ToString
				());
		}

		/// <summary>Test readLine for various kinds of line termination sequneces.</summary>
		/// <remarks>
		/// Test readLine for various kinds of line termination sequneces.
		/// Varies buffer size to stress test.  Also check that returned
		/// value matches the string length.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestNewLines()
		{
			string Str = "a\nbb\n\nccc\rdddd\r\r\r\n\r\neeeee";
			int Strlenbytes = Sharpen.Runtime.GetBytesForString(Str).Length;
			Text @out = new Text();
			for (int bufsz = 1; bufsz < Strlenbytes + 1; ++bufsz)
			{
				LineReader @in = MakeStream(Str, bufsz);
				int c = 0;
				c += @in.ReadLine(@out);
				//"a"\n
				NUnit.Framework.Assert.AreEqual("line1 length, bufsz:" + bufsz, 1, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//"bb"\n
				NUnit.Framework.Assert.AreEqual("line2 length, bufsz:" + bufsz, 2, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//""\n
				NUnit.Framework.Assert.AreEqual("line3 length, bufsz:" + bufsz, 0, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//"ccc"\r
				NUnit.Framework.Assert.AreEqual("line4 length, bufsz:" + bufsz, 3, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//dddd\r
				NUnit.Framework.Assert.AreEqual("line5 length, bufsz:" + bufsz, 4, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//""\r
				NUnit.Framework.Assert.AreEqual("line6 length, bufsz:" + bufsz, 0, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//""\r\n
				NUnit.Framework.Assert.AreEqual("line7 length, bufsz:" + bufsz, 0, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//""\r\n
				NUnit.Framework.Assert.AreEqual("line8 length, bufsz:" + bufsz, 0, @out.GetLength
					());
				c += @in.ReadLine(@out);
				//"eeeee"EOF
				NUnit.Framework.Assert.AreEqual("line9 length, bufsz:" + bufsz, 5, @out.GetLength
					());
				NUnit.Framework.Assert.AreEqual("end of file, bufsz: " + bufsz, 0, @in.ReadLine(@out
					));
				NUnit.Framework.Assert.AreEqual("total bytes, bufsz: " + bufsz, c, Strlenbytes);
			}
		}

		/// <summary>
		/// Test readLine for correct interpretation of maxLineLength
		/// (returned string should be clipped at maxLineLength, and the
		/// remaining bytes on the same line should be thrown out).
		/// </summary>
		/// <remarks>
		/// Test readLine for correct interpretation of maxLineLength
		/// (returned string should be clipped at maxLineLength, and the
		/// remaining bytes on the same line should be thrown out).
		/// Also check that returned value matches the string length.
		/// Varies buffer size to stress test.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestMaxLineLength()
		{
			string Str = "a\nbb\n\nccc\rdddd\r\neeeee";
			int Strlenbytes = Sharpen.Runtime.GetBytesForString(Str).Length;
			Text @out = new Text();
			for (int bufsz = 1; bufsz < Strlenbytes + 1; ++bufsz)
			{
				LineReader @in = MakeStream(Str, bufsz);
				int c = 0;
				c += @in.ReadLine(@out, 1);
				NUnit.Framework.Assert.AreEqual("line1 length, bufsz: " + bufsz, 1, @out.GetLength
					());
				c += @in.ReadLine(@out, 1);
				NUnit.Framework.Assert.AreEqual("line2 length, bufsz: " + bufsz, 1, @out.GetLength
					());
				c += @in.ReadLine(@out, 1);
				NUnit.Framework.Assert.AreEqual("line3 length, bufsz: " + bufsz, 0, @out.GetLength
					());
				c += @in.ReadLine(@out, 3);
				NUnit.Framework.Assert.AreEqual("line4 length, bufsz: " + bufsz, 3, @out.GetLength
					());
				c += @in.ReadLine(@out, 10);
				NUnit.Framework.Assert.AreEqual("line5 length, bufsz: " + bufsz, 4, @out.GetLength
					());
				c += @in.ReadLine(@out, 8);
				NUnit.Framework.Assert.AreEqual("line5 length, bufsz: " + bufsz, 5, @out.GetLength
					());
				NUnit.Framework.Assert.AreEqual("end of file, bufsz: " + bufsz, 0, @in.ReadLine(@out
					));
				NUnit.Framework.Assert.AreEqual("total bytes, bufsz: " + bufsz, c, Strlenbytes);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMRMaxLine()
		{
			int Maxpos = 1024 * 1024;
			int Maxline = 10 * 1024;
			int Buf = 64 * 1024;
			InputStream infNull = new _InputStream_343(Buf);
			// max LRR pos + LineReader buf
			LongWritable key = new LongWritable();
			Text val = new Text();
			Log.Info("Reading a line from /dev/null");
			Configuration conf = new Configuration(false);
			conf.SetInt(LineRecordReader.MaxLineLength, Maxline);
			conf.SetInt("io.file.buffer.size", Buf);
			// used by LRR
			// test another constructor 
			LineRecordReader lrr = new LineRecordReader(infNull, 0, Maxpos, conf);
			NUnit.Framework.Assert.IsFalse("Read a line from null", lrr.Next(key, val));
			infNull.Reset();
			lrr = new LineRecordReader(infNull, 0L, Maxline, Maxpos);
			NUnit.Framework.Assert.IsFalse("Read a line from null", lrr.Next(key, val));
		}

		private sealed class _InputStream_343 : InputStream
		{
			public _InputStream_343(int Buf)
			{
				this.Buf = Buf;
				this.position = 0;
				this.Maxposbuf = 1024 * 1024 + Buf;
			}

			internal int position;

			internal readonly int Maxposbuf;

			public override int Read()
			{
				++this.position;
				return 0;
			}

			public override int Read(byte[] b)
			{
				NUnit.Framework.Assert.IsTrue("Read too many bytes from the stream", this.position
					 < this.Maxposbuf);
				Arrays.Fill(b, unchecked((byte)0));
				this.position += b.Length;
				return b.Length;
			}

			public override void Reset()
			{
				this.position = 0;
			}

			private readonly int Buf;
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
		private static IList<Text> ReadSplit(TextInputFormat format, InputSplit split, JobConf
			 job)
		{
			IList<Text> result = new AList<Text>();
			RecordReader<LongWritable, Text> reader = format.GetRecordReader(split, job, voidReporter
				);
			LongWritable key = reader.CreateKey();
			Text value = reader.CreateValue();
			while (reader.Next(key, value))
			{
				result.AddItem(value);
				value = reader.CreateValue();
			}
			reader.Close();
			return result;
		}

		/// <summary>Test using the gzip codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGzip()
		{
			JobConf job = new JobConf(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, job);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n"
				);
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "this is a test\nof gzip\n"
				);
			FileInputFormat.SetInputPaths(job, workDir);
			TextInputFormat format = new TextInputFormat();
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

		/// <summary>Test using the gzip codec and an empty input file</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGzipEmpty()
		{
			JobConf job = new JobConf(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, job);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "empty.gz"), gzip, string.Empty);
			FileInputFormat.SetInputPaths(job, workDir);
			TextInputFormat format = new TextInputFormat();
			format.Configure(job);
			InputSplit[] splits = format.GetSplits(job, 100);
			NUnit.Framework.Assert.AreEqual("Compressed files of length 0 are not returned from FileInputFormat.getSplits()."
				, 1, splits.Length);
			IList<Text> results = ReadSplit(format, splits[0], job);
			NUnit.Framework.Assert.AreEqual("Compressed empty file length == 0", 0, results.Count
				);
		}

		private static string Unquote(string @in)
		{
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < @in.Length; ++i)
			{
				char ch = @in[i];
				if (ch == '\\')
				{
					ch = @in[++i];
					switch (ch)
					{
						case 'n':
						{
							result.Append('\n');
							break;
						}

						case 'r':
						{
							result.Append('\r');
							break;
						}

						default:
						{
							result.Append(ch);
							break;
						}
					}
				}
				else
				{
					result.Append(ch);
				}
			}
			return result.ToString();
		}

		/// <summary>Parse the command line arguments into lines and display the result.</summary>
		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			foreach (string arg in args)
			{
				System.Console.Out.WriteLine("Working on " + arg);
				LineReader reader = MakeStream(Unquote(arg));
				Org.Apache.Hadoop.IO.Text line = new Org.Apache.Hadoop.IO.Text();
				int size = reader.ReadLine(line);
				while (size > 0)
				{
					System.Console.Out.WriteLine("Got: " + line.ToString());
					size = reader.ReadLine(line);
				}
				reader.Close();
			}
		}
	}
}
