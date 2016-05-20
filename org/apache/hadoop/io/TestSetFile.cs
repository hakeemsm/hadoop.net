using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestSetFile : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestSetFile)
			));

		private static string FILE = Sharpen.Runtime.getProperty("test.build.data", ".") 
			+ "/test.set";

		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		public TestSetFile(string name)
			: base(name)
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void testSetFile()
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			try
			{
				org.apache.hadoop.io.RandomDatum[] data = generate(10000);
				writeTest(fs, data, FILE, org.apache.hadoop.io.SequenceFile.CompressionType.NONE);
				readTest(fs, data, FILE);
				writeTest(fs, data, FILE, org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK
					);
				readTest(fs, data, FILE);
			}
			finally
			{
				fs.close();
			}
		}

		/// <summary>
		/// test
		/// <c>SetFile.Reader</c>
		/// methods
		/// next(), get() in combination
		/// </summary>
		public virtual void testSetFileAccessMethods()
		{
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				int size = 10;
				writeData(fs, size);
				org.apache.hadoop.io.SetFile.Reader reader = createReader(fs);
				NUnit.Framework.Assert.IsTrue("testSetFileWithConstruction1 error !!!", reader.next
					(new org.apache.hadoop.io.IntWritable(0)));
				// don't know why reader.get(i) return i+1
				NUnit.Framework.Assert.AreEqual("testSetFileWithConstruction2 error !!!", new org.apache.hadoop.io.IntWritable
					(size / 2 + 1), reader.get(new org.apache.hadoop.io.IntWritable(size / 2)));
				NUnit.Framework.Assert.IsNull("testSetFileWithConstruction3 error !!!", reader.get
					(new org.apache.hadoop.io.IntWritable(size * 2)));
			}
			catch (System.Exception)
			{
				fail("testSetFileWithConstruction error !!!");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.io.SetFile.Reader createReader(org.apache.hadoop.fs.FileSystem
			 fs)
		{
			return new org.apache.hadoop.io.SetFile.Reader(fs, FILE, org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable))), 
				conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeData(org.apache.hadoop.fs.FileSystem fs, int elementSize)
		{
			org.apache.hadoop.io.MapFile.delete(fs, FILE);
			org.apache.hadoop.io.SetFile.Writer writer = new org.apache.hadoop.io.SetFile.Writer
				(fs, FILE, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
				)));
			for (int i = 0; i < elementSize; i++)
			{
				writer.append(new org.apache.hadoop.io.IntWritable(i));
			}
			writer.close();
		}

		private static org.apache.hadoop.io.RandomDatum[] generate(int count)
		{
			LOG.info("generating " + count + " records in memory");
			org.apache.hadoop.io.RandomDatum[] data = new org.apache.hadoop.io.RandomDatum[count
				];
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				();
			for (int i = 0; i < count; i++)
			{
				generator.next();
				data[i] = generator.getValue();
			}
			LOG.info("sorting " + count + " records");
			java.util.Arrays.sort(data);
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeTest(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RandomDatum
			[] data, string file, org.apache.hadoop.io.SequenceFile.CompressionType compress
			)
		{
			org.apache.hadoop.io.MapFile.delete(fs, file);
			LOG.info("creating with " + data.Length + " records");
			org.apache.hadoop.io.SetFile.Writer writer = new org.apache.hadoop.io.SetFile.Writer
				(conf, fs, file, org.apache.hadoop.io.WritableComparator.get(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.RandomDatum))), compress);
			for (int i = 0; i < data.Length; i++)
			{
				writer.append(data[i]);
			}
			writer.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void readTest(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RandomDatum
			[] data, string file)
		{
			org.apache.hadoop.io.RandomDatum v = new org.apache.hadoop.io.RandomDatum();
			int sample = (int)System.Math.sqrt(data.Length);
			java.util.Random random = new java.util.Random();
			LOG.info("reading " + sample + " records");
			org.apache.hadoop.io.SetFile.Reader reader = new org.apache.hadoop.io.SetFile.Reader
				(fs, file, conf);
			for (int i = 0; i < sample; i++)
			{
				if (!reader.seek(data[random.nextInt(data.Length)]))
				{
					throw new System.Exception("wrong value at " + i);
				}
			}
			reader.close();
			LOG.info("done reading " + data.Length);
		}

		/// <summary>For debugging and testing.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int count = 1024 * 1024;
			bool create = true;
			bool check = true;
			string file = FILE;
			string compress = "NONE";
			string usage = "Usage: TestSetFile [-count N] [-nocreate] [-nocheck] [-compress type] file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
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
									if (args[i].Equals("-compress"))
									{
										compress = args[++i];
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
				}
				fs = fpath.getFileSystem(conf);
				LOG.info("count = " + count);
				LOG.info("create = " + create);
				LOG.info("check = " + check);
				LOG.info("compress = " + compress);
				LOG.info("file = " + file);
				org.apache.hadoop.io.RandomDatum[] data = generate(count);
				if (create)
				{
					writeTest(fs, data, file, org.apache.hadoop.io.SequenceFile.CompressionType.valueOf
						(compress));
				}
				if (check)
				{
					readTest(fs, data, file);
				}
			}
			finally
			{
				fs.close();
			}
		}
	}
}
