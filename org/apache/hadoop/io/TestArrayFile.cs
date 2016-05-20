using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestArrayFile : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestArrayFile
			)));

		private static readonly org.apache.hadoop.fs.Path TEST_DIR = new org.apache.hadoop.fs.Path
			(Sharpen.Runtime.getProperty("test.build.data", "/tmp"), Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.io.TestMapFile)).getSimpleName());

		private static string TEST_FILE = new org.apache.hadoop.fs.Path(TEST_DIR, "test.array"
			).ToString();

		public TestArrayFile(string name)
			: base(name)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void testArrayFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.io.RandomDatum[] data = generate(10000);
			writeTest(fs, data, TEST_FILE);
			readTest(fs, data, TEST_FILE, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testEmptyFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			writeTest(fs, new org.apache.hadoop.io.RandomDatum[0], TEST_FILE);
			org.apache.hadoop.io.ArrayFile.Reader reader = new org.apache.hadoop.io.ArrayFile.Reader
				(fs, TEST_FILE, conf);
			NUnit.Framework.Assert.IsNull(reader.get(0, new org.apache.hadoop.io.RandomDatum(
				)));
			reader.close();
		}

		private static org.apache.hadoop.io.RandomDatum[] generate(int count)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("generating " + count + " records in debug");
			}
			org.apache.hadoop.io.RandomDatum[] data = new org.apache.hadoop.io.RandomDatum[count
				];
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				();
			for (int i = 0; i < count; i++)
			{
				generator.next();
				data[i] = generator.getValue();
			}
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeTest(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RandomDatum
			[] data, string file)
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.MapFile.delete(fs, file);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("creating with " + data.Length + " debug");
			}
			org.apache.hadoop.io.ArrayFile.Writer writer = new org.apache.hadoop.io.ArrayFile.Writer
				(conf, fs, file, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum
				)));
			writer.setIndexInterval(100);
			for (int i = 0; i < data.Length; i++)
			{
				writer.append(data[i]);
			}
			writer.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void readTest(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RandomDatum
			[] data, string file, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.io.RandomDatum v = new org.apache.hadoop.io.RandomDatum();
			if (LOG.isDebugEnabled())
			{
				LOG.debug("reading " + data.Length + " debug");
			}
			org.apache.hadoop.io.ArrayFile.Reader reader = new org.apache.hadoop.io.ArrayFile.Reader
				(fs, file, conf);
			try
			{
				for (int i = 0; i < data.Length; i++)
				{
					// try forwards
					reader.get(i, v);
					if (!v.Equals(data[i]))
					{
						throw new System.Exception("wrong value at " + i);
					}
				}
				for (int i_1 = data.Length - 1; i_1 >= 0; i_1--)
				{
					// then backwards
					reader.get(i_1, v);
					if (!v.Equals(data[i_1]))
					{
						throw new System.Exception("wrong value at " + i_1);
					}
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug("done reading " + data.Length + " debug");
				}
			}
			finally
			{
				reader.close();
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
		public virtual void testArrayFileIteration()
		{
			int SIZE = 10;
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
				org.apache.hadoop.io.ArrayFile.Writer writer = new org.apache.hadoop.io.ArrayFile.Writer
					(conf, fs, TEST_FILE, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
					)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, defaultProgressable
					);
				NUnit.Framework.Assert.IsNotNull("testArrayFileIteration error !!!", writer);
				for (int i = 0; i < SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.LongWritable(i));
				}
				writer.close();
				org.apache.hadoop.io.ArrayFile.Reader reader = new org.apache.hadoop.io.ArrayFile.Reader
					(fs, TEST_FILE, conf);
				org.apache.hadoop.io.LongWritable nextWritable = new org.apache.hadoop.io.LongWritable
					(0);
				for (int i_1 = 0; i_1 < SIZE; i_1++)
				{
					nextWritable = (org.apache.hadoop.io.LongWritable)reader.next(nextWritable);
					NUnit.Framework.Assert.AreEqual(nextWritable.get(), i_1);
				}
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration seek error !!!", reader.seek
					(new org.apache.hadoop.io.LongWritable(6)));
				nextWritable = (org.apache.hadoop.io.LongWritable)reader.next(nextWritable);
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration error !!!", reader.key() ==
					 7);
				NUnit.Framework.Assert.IsTrue("testArrayFileIteration error !!!", nextWritable.Equals
					(new org.apache.hadoop.io.LongWritable(7)));
				NUnit.Framework.Assert.IsFalse("testArrayFileIteration error !!!", reader.seek(new 
					org.apache.hadoop.io.LongWritable(SIZE + 5)));
				reader.close();
			}
			catch (System.Exception)
			{
				fail("testArrayFileWriterConstruction error !!!");
			}
		}

		/// <summary>For debugging and testing.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int count = 1024 * 1024;
			bool create = true;
			bool check = true;
			string file = TEST_FILE;
			string usage = "Usage: TestArrayFile [-count N] [-nocreate] [-nocheck] file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			int i = 0;
			org.apache.hadoop.fs.Path fpath = null;
			org.apache.hadoop.fs.FileSystem fs = null;
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
									fpath = new org.apache.hadoop.fs.Path(file);
								}
							}
						}
					}
				}
				fs = fpath.getFileSystem(conf);
				LOG.info("count = " + count);
				LOG.info("create = " + create);
				LOG.info("check = " + check);
				LOG.info("file = " + file);
				org.apache.hadoop.io.RandomDatum[] data = generate(count);
				if (create)
				{
					writeTest(fs, data, file);
				}
				if (check)
				{
					readTest(fs, data, file, conf);
				}
			}
			finally
			{
				fs.close();
			}
		}

		private sealed class _Progressable_213 : org.apache.hadoop.util.Progressable
		{
			public _Progressable_213()
			{
			}

			public void progress()
			{
			}
		}

		private static readonly org.apache.hadoop.util.Progressable defaultProgressable = 
			new _Progressable_213();
	}
}
