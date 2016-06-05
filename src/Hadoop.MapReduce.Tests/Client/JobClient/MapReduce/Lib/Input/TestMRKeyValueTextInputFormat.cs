using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Input
{
	public class TestMRKeyValueTextInputFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRKeyValueTextInputFormat
			).FullName);

		private static Configuration defaultConf = new Configuration();

		private static FileSystem localFs = null;

		static TestMRKeyValueTextInputFormat()
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
			, "."), "data"), "TestKeyValueTextInputFormat");

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormat()
		{
			Job job = Job.GetInstance(new Configuration(defaultConf));
			Path file = new Path(workDir, "test.txt");
			int seed = new Random().Next();
			Log.Info("seed = " + seed);
			Random random = new Random(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int MaxLength = 10000;
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
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / 20) + 1;
					Log.Debug("splitting: requesting = " + numSplits);
					IList<InputSplit> splits = format.GetSplits(job);
					Log.Debug("splitting: got =        " + splits.Count);
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Count; j++)
					{
						Log.Debug("split[" + j + "]= " + splits[j]);
						TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
							.GetConfiguration());
						RecordReader<Text, Text> reader = format.CreateRecordReader(splits[j], context);
						Type clazz = reader.GetType();
						NUnit.Framework.Assert.AreEqual("reader class is KeyValueLineRecordReader.", typeof(
							KeyValueLineRecordReader), clazz);
						MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text
							, Text>(job.GetConfiguration(), context.GetTaskAttemptID(), reader, null, null, 
							MapReduceTestUtil.CreateDummyReporter(), splits[j]);
						reader.Initialize(splits[j], mcontext);
						Text key = null;
						Text value = null;
						try
						{
							int count = 0;
							while (reader.NextKeyValue())
							{
								key = reader.GetCurrentKey();
								clazz = key.GetType();
								NUnit.Framework.Assert.AreEqual("Key class is Text.", typeof(Text), clazz);
								value = reader.GetCurrentValue();
								clazz = value.GetType();
								NUnit.Framework.Assert.AreEqual("Value class is Text.", typeof(Text), clazz);
								int k = System.Convert.ToInt32(key.ToString());
								int v = System.Convert.ToInt32(value.ToString());
								NUnit.Framework.Assert.AreEqual("Bad key", 0, k % 2);
								NUnit.Framework.Assert.AreEqual("Mismatched key/value", k / 2, v);
								Log.Debug("read " + v);
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitableCodecs()
		{
			Job job = Job.GetInstance(defaultConf);
			Configuration conf = job.GetConfiguration();
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
			int seed = new Random().Next();
			Log.Info("seed = " + seed);
			Random random = new Random(seed);
			localFs.Delete(workDir, true);
			FileInputFormat.SetInputPaths(job, workDir);
			int MaxLength = 500000;
			FileInputFormat.SetMaxInputSplitSize(job, MaxLength / 20);
			// for a variety of lengths
			for (int length = 0; length < MaxLength; length += random.Next(MaxLength / 4) + 1)
			{
				Log.Info("creating; entries = " + length);
				// create a file with length entries
				TextWriter writer = new OutputStreamWriter(codec.CreateOutputStream(localFs.Create
					(file)));
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
				NUnit.Framework.Assert.IsTrue("KVTIF claims not splittable", format.IsSplitable(job
					, file));
				for (int i_1 = 0; i_1 < 3; i_1++)
				{
					int numSplits = random.Next(MaxLength / 2000) + 1;
					Log.Info("splitting: requesting = " + numSplits);
					IList<InputSplit> splits = format.GetSplits(job);
					Log.Info("splitting: got =        " + splits.Count);
					// check each split
					BitSet bits = new BitSet(length);
					for (int j = 0; j < splits.Count; j++)
					{
						Log.Debug("split[" + j + "]= " + splits[j]);
						TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
							.GetConfiguration());
						RecordReader<Text, Text> reader = format.CreateRecordReader(splits[j], context);
						Type clazz = reader.GetType();
						MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text
							, Text>(job.GetConfiguration(), context.GetTaskAttemptID(), reader, null, null, 
							MapReduceTestUtil.CreateDummyReporter(), splits[j]);
						reader.Initialize(splits[j], mcontext);
						Text key = null;
						Text value = null;
						try
						{
							int count = 0;
							while (reader.NextKeyValue())
							{
								key = reader.GetCurrentKey();
								value = reader.GetCurrentValue();
								int k = System.Convert.ToInt32(key.ToString());
								int v = System.Convert.ToInt32(value.ToString());
								NUnit.Framework.Assert.AreEqual("Bad key", 0, k % 2);
								NUnit.Framework.Assert.AreEqual("Mismatched key/value", k / 2, v);
								Log.Debug("read " + k + "," + v);
								NUnit.Framework.Assert.IsFalse(k + "," + v + " in multiple partitions.", bits.Get
									(v));
								bits.Set(v);
								count++;
							}
							if (count > 0)
							{
								Log.Info("splits[" + j + "]=" + splits[j] + " count=" + count);
							}
							else
							{
								Log.Debug("splits[" + j + "]=" + splits[j] + " count=" + count);
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
		private LineReader MakeStream(string str)
		{
			return new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(
				str, "UTF-8")), defaultConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewLines()
		{
			LineReader @in = MakeStream("a\nbb\n\nccc\rdddd\r\neeeee");
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

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static IList<Text> ReadSplit(KeyValueTextInputFormat format, InputSplit split
			, Job job)
		{
			IList<Text> result = new AList<Text>();
			Configuration conf = job.GetConfiguration();
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(conf
				);
			RecordReader<Text, Text> reader = format.CreateRecordReader(split, MapReduceTestUtil
				.CreateDummyMapTaskAttemptContext(conf));
			MapContext<Text, Text, Text, Text> mcontext = new MapContextImpl<Text, Text, Text
				, Text>(conf, context.GetTaskAttemptID(), reader, null, null, MapReduceTestUtil.
				CreateDummyReporter(), split);
			reader.Initialize(split, mcontext);
			while (reader.NextKeyValue())
			{
				result.AddItem(new Text(reader.GetCurrentValue()));
			}
			reader.Close();
			return result;
		}

		/// <summary>Test using the gzip codec for reading</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGzip()
		{
			Configuration conf = new Configuration(defaultConf);
			CompressionCodec gzip = new GzipCodec();
			ReflectionUtils.SetConf(gzip, conf);
			localFs.Delete(workDir, true);
			WriteFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, "line-1\tthe quick\nline-2\tbrown\nline-3\t"
				 + "fox jumped\nline-4\tover\nline-5\t the lazy\nline-6\t dog\n");
			WriteFile(localFs, new Path(workDir, "part2.txt.gz"), gzip, "line-1\tthis is a test\nline-1\tof gzip\n"
				);
			Job job = Job.GetInstance(conf);
			FileInputFormat.SetInputPaths(job, workDir);
			KeyValueTextInputFormat format = new KeyValueTextInputFormat();
			IList<InputSplit> splits = format.GetSplits(job);
			NUnit.Framework.Assert.AreEqual("compressed splits == 2", 2, splits.Count);
			FileSplit tmp = (FileSplit)splits[0];
			if (tmp.GetPath().GetName().Equals("part2.txt.gz"))
			{
				splits.Set(0, splits[1]);
				splits.Set(1, tmp);
			}
			IList<Text> results = ReadSplit(format, splits[0], job);
			NUnit.Framework.Assert.AreEqual("splits[0] length", 6, results.Count);
			NUnit.Framework.Assert.AreEqual("splits[0][0]", "the quick", results[0].ToString(
				));
			NUnit.Framework.Assert.AreEqual("splits[0][1]", "brown", results[1].ToString());
			NUnit.Framework.Assert.AreEqual("splits[0][2]", "fox jumped", results[2].ToString
				());
			NUnit.Framework.Assert.AreEqual("splits[0][3]", "over", results[3].ToString());
			NUnit.Framework.Assert.AreEqual("splits[0][4]", " the lazy", results[4].ToString(
				));
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
			new TestMRKeyValueTextInputFormat().TestFormat();
		}
	}
}
