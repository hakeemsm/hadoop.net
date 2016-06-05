using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestSetFile : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.TestSetFile
			));

		private static string File = Runtime.GetProperty("test.build.data", ".") + "/test.set";

		private static Configuration conf = new Configuration();

		public TestSetFile(string name)
			: base(name)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetFile()
		{
			FileSystem fs = FileSystem.GetLocal(conf);
			try
			{
				RandomDatum[] data = Generate(10000);
				WriteTest(fs, data, File, SequenceFile.CompressionType.None);
				ReadTest(fs, data, File);
				WriteTest(fs, data, File, SequenceFile.CompressionType.Block);
				ReadTest(fs, data, File);
			}
			finally
			{
				fs.Close();
			}
		}

		/// <summary>
		/// test
		/// <c>SetFile.Reader</c>
		/// methods
		/// next(), get() in combination
		/// </summary>
		public virtual void TestSetFileAccessMethods()
		{
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				int size = 10;
				WriteData(fs, size);
				SetFile.Reader reader = CreateReader(fs);
				Assert.True("testSetFileWithConstruction1 error !!!", reader.Next
					(new IntWritable(0)));
				// don't know why reader.get(i) return i+1
				Assert.Equal("testSetFileWithConstruction2 error !!!", new IntWritable
					(size / 2 + 1), reader.Get(new IntWritable(size / 2)));
				NUnit.Framework.Assert.IsNull("testSetFileWithConstruction3 error !!!", reader.Get
					(new IntWritable(size * 2)));
			}
			catch (Exception)
			{
				Fail("testSetFileWithConstruction error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private SetFile.Reader CreateReader(FileSystem fs)
		{
			return new SetFile.Reader(fs, File, WritableComparator.Get(typeof(IntWritable)), 
				conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteData(FileSystem fs, int elementSize)
		{
			MapFile.Delete(fs, File);
			SetFile.Writer writer = new SetFile.Writer(fs, File, typeof(IntWritable));
			for (int i = 0; i < elementSize; i++)
			{
				writer.Append(new IntWritable(i));
			}
			writer.Close();
		}

		private static RandomDatum[] Generate(int count)
		{
			Log.Info("generating " + count + " records in memory");
			RandomDatum[] data = new RandomDatum[count];
			RandomDatum.Generator generator = new RandomDatum.Generator();
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				data[i] = generator.GetValue();
			}
			Log.Info("sorting " + count + " records");
			Arrays.Sort(data);
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteTest(FileSystem fs, RandomDatum[] data, string file, SequenceFile.CompressionType
			 compress)
		{
			MapFile.Delete(fs, file);
			Log.Info("creating with " + data.Length + " records");
			SetFile.Writer writer = new SetFile.Writer(conf, fs, file, WritableComparator.Get
				(typeof(RandomDatum)), compress);
			for (int i = 0; i < data.Length; i++)
			{
				writer.Append(data[i]);
			}
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ReadTest(FileSystem fs, RandomDatum[] data, string file)
		{
			RandomDatum v = new RandomDatum();
			int sample = (int)Math.Sqrt(data.Length);
			Random random = new Random();
			Log.Info("reading " + sample + " records");
			SetFile.Reader reader = new SetFile.Reader(fs, file, conf);
			for (int i = 0; i < sample; i++)
			{
				if (!reader.Seek(data[random.Next(data.Length)]))
				{
					throw new RuntimeException("wrong value at " + i);
				}
			}
			reader.Close();
			Log.Info("done reading " + data.Length);
		}

		/// <summary>For debugging and testing.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int count = 1024 * 1024;
			bool create = true;
			bool check = true;
			string file = File;
			string compress = "NONE";
			string usage = "Usage: TestSetFile [-count N] [-nocreate] [-nocheck] [-compress type] file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
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
									if (args[i].Equals("-compress"))
									{
										compress = args[++i];
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
				}
				fs = fpath.GetFileSystem(conf);
				Log.Info("count = " + count);
				Log.Info("create = " + create);
				Log.Info("check = " + check);
				Log.Info("compress = " + compress);
				Log.Info("file = " + file);
				RandomDatum[] data = Generate(count);
				if (create)
				{
					WriteTest(fs, data, file, SequenceFile.CompressionType.ValueOf(compress));
				}
				if (check)
				{
					ReadTest(fs, data, file);
				}
			}
			finally
			{
				fs.Close();
			}
		}
	}
}
