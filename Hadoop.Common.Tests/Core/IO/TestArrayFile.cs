using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestArrayFile : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.TestArrayFile
			));

		private static readonly Path TestDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestMapFile).Name);

		private static string TestFile = new Path(TestDir, "test.array").ToString();

		public TestArrayFile(string name)
			: base(name)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestArrayFile()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			RandomDatum[] data = Generate(10000);
			WriteTest(fs, data, TestFile);
			ReadTest(fs, data, TestFile, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyFile()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			WriteTest(fs, new RandomDatum[0], TestFile);
			ArrayFile.Reader reader = new ArrayFile.Reader(fs, TestFile, conf);
			NUnit.Framework.Assert.IsNull(reader.Get(0, new RandomDatum()));
			reader.Close();
		}

		private static RandomDatum[] Generate(int count)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("generating " + count + " records in debug");
			}
			RandomDatum[] data = new RandomDatum[count];
			RandomDatum.Generator generator = new RandomDatum.Generator();
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				data[i] = generator.GetValue();
			}
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteTest(FileSystem fs, RandomDatum[] data, string file)
		{
			Configuration conf = new Configuration();
			MapFile.Delete(fs, file);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("creating with " + data.Length + " debug");
			}
			ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, file, typeof(RandomDatum
				));
			writer.SetIndexInterval(100);
			for (int i = 0; i < data.Length; i++)
			{
				writer.Append(data[i]);
			}
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ReadTest(FileSystem fs, RandomDatum[] data, string file, Configuration
			 conf)
		{
			RandomDatum v = new RandomDatum();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("reading " + data.Length + " debug");
			}
			ArrayFile.Reader reader = new ArrayFile.Reader(fs, file, conf);
			try
			{
				for (int i = 0; i < data.Length; i++)
				{
					// try forwards
					reader.Get(i, v);
					if (!v.Equals(data[i]))
					{
						throw new RuntimeException("wrong value at " + i);
					}
				}
				for (int i_1 = data.Length - 1; i_1 >= 0; i_1--)
				{
					// then backwards
					reader.Get(i_1, v);
					if (!v.Equals(data[i_1]))
					{
						throw new RuntimeException("wrong value at " + i_1);
					}
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("done reading " + data.Length + " debug");
				}
			}
			finally
			{
				reader.Close();
			}
		}

		/// <summary>
		/// test on
		/// <see cref="Reader"/>
		/// iteration methods
		/// <pre>
		/// <c>next(), seek()</c>
		/// in and out of range.
		/// </pre>
		/// </summary>
		public virtual void TestArrayFileIteration()
		{
			int Size = 10;
			Configuration conf = new Configuration();
			try
			{
				FileSystem fs = FileSystem.Get(conf);
				ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, TestFile, typeof(LongWritable
					), SequenceFile.CompressionType.Record, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull("testArrayFileIteration error !!!", writer);
				for (int i = 0; i < Size; i++)
				{
					writer.Append(new LongWritable(i));
				}
				writer.Close();
				ArrayFile.Reader reader = new ArrayFile.Reader(fs, TestFile, conf);
				LongWritable nextWritable = new LongWritable(0);
				for (int i_1 = 0; i_1 < Size; i_1++)
				{
					nextWritable = (LongWritable)reader.Next(nextWritable);
					NUnit.Framework.Assert.AreEqual(nextWritable.Get(), i_1);
				}
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration seek error !!!", reader.Seek
					(new LongWritable(6)));
				nextWritable = (LongWritable)reader.Next(nextWritable);
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration error !!!", reader.Key() ==
					 7);
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration error !!!", nextWritable.Equals
					(new LongWritable(7)));
				NUnit.Framework.Assert.IsFalse("testArrayFileIteration error !!!", reader.Seek(new 
					LongWritable(Size + 5)));
				reader.Close();
			}
			catch (Exception)
			{
				Fail("testArrayFileWriterConstruction error !!!");
			}
		}

		/// <summary>For debugging and testing.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int count = 1024 * 1024;
			bool create = true;
			bool check = true;
			string file = TestFile;
			string usage = "Usage: TestArrayFile [-count N] [-nocreate] [-nocheck] file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			Configuration conf = new Configuration();
			int i = 0;
			Path fpath = null;
			FileSystem fs = null;
			try
			{
				for (; i < args.Length; i++)
				{
					// parse command line
					if (args[i] == null)
					{
						continue;
					}
					else
					{
						if (args[i].Equals("-count"))
						{
							count = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-nocreate"))
							{
								create = false;
							}
							else
							{
								if (args[i].Equals("-nocheck"))
								{
									check = false;
								}
								else
								{
									// file is required parameter
									file = args[i];
									fpath = new Path(file);
								}
							}
						}
					}
				}
				fs = fpath.GetFileSystem(conf);
				Log.Info("count = " + count);
				Log.Info("create = " + create);
				Log.Info("check = " + check);
				Log.Info("file = " + file);
				RandomDatum[] data = Generate(count);
				if (create)
				{
					WriteTest(fs, data, file);
				}
				if (check)
				{
					ReadTest(fs, data, file, conf);
				}
			}
			finally
			{
				fs.Close();
			}
		}

		private sealed class _Progressable_213 : Progressable
		{
			public _Progressable_213()
			{
			}

			public void Progress()
			{
			}
		}

		private static readonly Progressable defaultProgressable = new _Progressable_213(
			);
	}
}
