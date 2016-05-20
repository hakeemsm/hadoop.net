using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestBloomMapFile : NUnit.Framework.TestCase
	{
		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		private static readonly org.apache.hadoop.fs.Path TEST_ROOT = new org.apache.hadoop.fs.Path
			(Sharpen.Runtime.getProperty("test.build.data", "/tmp"), Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.io.TestMapFile)).getSimpleName());

		private static readonly org.apache.hadoop.fs.Path TEST_DIR = new org.apache.hadoop.fs.Path
			(TEST_ROOT, "testfile");

		private static readonly org.apache.hadoop.fs.Path TEST_FILE = new org.apache.hadoop.fs.Path
			(TEST_ROOT, "testfile");

		/// <exception cref="System.Exception"/>
		protected override void setUp()
		{
			org.apache.hadoop.fs.LocalFileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			if (fs.exists(TEST_ROOT) && !fs.delete(TEST_ROOT, true))
			{
				NUnit.Framework.Assert.Fail("Can't clean up test root dir");
			}
			fs.mkdirs(TEST_ROOT);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMembershipTest()
		{
			// write the file
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path qualifiedDirName = fs.makeQualified(TEST_DIR);
			conf.setInt("io.mapfile.bloom.size", 2048);
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			org.apache.hadoop.io.BloomMapFile.Reader reader = null;
			try
			{
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, fs, qualifiedDirName.
					ToString(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				org.apache.hadoop.io.IntWritable key = new org.apache.hadoop.io.IntWritable();
				org.apache.hadoop.io.Text value = new org.apache.hadoop.io.Text();
				for (int i = 0; i < 2000; i += 2)
				{
					key.set(i);
					value.set("00" + i);
					writer.append(key, value);
				}
				writer.close();
				reader = new org.apache.hadoop.io.BloomMapFile.Reader(fs, qualifiedDirName.ToString
					(), conf);
				// check false positives rate
				int falsePos = 0;
				int falseNeg = 0;
				for (int i_1 = 0; i_1 < 2000; i_1++)
				{
					key.set(i_1);
					bool exists = reader.probablyHasKey(key);
					if (i_1 % 2 == 0)
					{
						if (!exists)
						{
							falseNeg++;
						}
					}
					else
					{
						if (exists)
						{
							falsePos++;
						}
					}
				}
				reader.close();
				fs.delete(qualifiedDirName, true);
				System.Console.Out.WriteLine("False negatives: " + falseNeg);
				NUnit.Framework.Assert.AreEqual(0, falseNeg);
				System.Console.Out.WriteLine("False positives: " + falsePos);
				NUnit.Framework.Assert.IsTrue(falsePos < 2);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		private void checkMembershipVaryingSizedKeys(string name, System.Collections.Generic.IList
			<org.apache.hadoop.io.Text> keys)
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path qualifiedDirName = fs.makeQualified(TEST_DIR);
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			org.apache.hadoop.io.BloomMapFile.Reader reader = null;
			try
			{
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, fs, qualifiedDirName.
					ToString(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)), 
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable)));
				foreach (org.apache.hadoop.io.Text key in keys)
				{
					writer.append(key, org.apache.hadoop.io.NullWritable.get());
				}
				writer.close();
				// will check for membership in opposite order of how keys were inserted
				reader = new org.apache.hadoop.io.BloomMapFile.Reader(fs, qualifiedDirName.ToString
					(), conf);
				java.util.Collections.reverse(keys);
				foreach (org.apache.hadoop.io.Text key_1 in keys)
				{
					NUnit.Framework.Assert.IsTrue("False negative for existing key " + key_1, reader.
						probablyHasKey(key_1));
				}
				reader.close();
				fs.delete(qualifiedDirName, true);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMembershipVaryingSizedKeysTest1()
		{
			System.Collections.Generic.List<org.apache.hadoop.io.Text> list = new System.Collections.Generic.List
				<org.apache.hadoop.io.Text>();
			list.add(new org.apache.hadoop.io.Text("A"));
			list.add(new org.apache.hadoop.io.Text("BB"));
			checkMembershipVaryingSizedKeys(getName(), list);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testMembershipVaryingSizedKeysTest2()
		{
			System.Collections.Generic.List<org.apache.hadoop.io.Text> list = new System.Collections.Generic.List
				<org.apache.hadoop.io.Text>();
			list.add(new org.apache.hadoop.io.Text("AA"));
			list.add(new org.apache.hadoop.io.Text("B"));
			checkMembershipVaryingSizedKeys(getName(), list);
		}

		/// <summary>
		/// test
		/// <c>BloomMapFile.delete()</c>
		/// method
		/// </summary>
		public virtual void testDeleteFile()
		{
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, TEST_FILE, org.apache.hadoop.io.MapFile.Writer
					.keyClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					))), org.apache.hadoop.io.MapFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text))));
				NUnit.Framework.Assert.IsNotNull("testDeleteFile error !!!", writer);
				writer.close();
				org.apache.hadoop.io.BloomMapFile.delete(fs, TEST_FILE.ToString());
			}
			catch (System.Exception)
			{
				fail("unexpect ex in testDeleteFile !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <see cref="Reader"/>
		/// constructor with
		/// IOException
		/// </summary>
		public virtual void testIOExceptionInWriterConstructor()
		{
			org.apache.hadoop.fs.Path dirNameSpy = org.mockito.Mockito.spy(TEST_FILE);
			org.apache.hadoop.io.BloomMapFile.Reader reader = null;
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			try
			{
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, TEST_FILE, org.apache.hadoop.io.MapFile.Writer
					.keyClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					))), org.apache.hadoop.io.MapFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text))));
				writer.append(new org.apache.hadoop.io.IntWritable(1), new org.apache.hadoop.io.Text
					("123124142"));
				writer.close();
				org.mockito.Mockito.when(dirNameSpy.getFileSystem(conf)).thenThrow(new System.IO.IOException
					());
				reader = new org.apache.hadoop.io.BloomMapFile.Reader(dirNameSpy, conf, org.apache.hadoop.io.MapFile.Reader
					.comparator(new org.apache.hadoop.io.WritableComparator(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)))));
				NUnit.Framework.Assert.IsNull("testIOExceptionInWriterConstructor error !!!", reader
					.getBloomFilter());
			}
			catch (System.Exception)
			{
				fail("unexpect ex in testIOExceptionInWriterConstructor !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <see>BloomMapFile.Reader.get()</see>
		/// method
		/// </summary>
		public virtual void testGetBloomMapFile()
		{
			int SIZE = 10;
			org.apache.hadoop.io.BloomMapFile.Reader reader = null;
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			try
			{
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, TEST_FILE, org.apache.hadoop.io.MapFile.Writer
					.keyClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					))), org.apache.hadoop.io.MapFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text))));
				for (int i = 0; i < SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(i), new org.apache.hadoop.io.Text
						());
				}
				writer.close();
				reader = new org.apache.hadoop.io.BloomMapFile.Reader(TEST_FILE, conf, org.apache.hadoop.io.MapFile.Reader
					.comparator(new org.apache.hadoop.io.WritableComparator(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)))));
				for (int i_1 = 0; i_1 < SIZE; i_1++)
				{
					NUnit.Framework.Assert.IsNotNull("testGetBloomMapFile error !!!", reader.get(new 
						org.apache.hadoop.io.IntWritable(i_1), new org.apache.hadoop.io.Text()));
				}
				NUnit.Framework.Assert.IsNull("testGetBloomMapFile error !!!", reader.get(new org.apache.hadoop.io.IntWritable
					(SIZE + 5), new org.apache.hadoop.io.Text()));
			}
			catch (System.Exception)
			{
				fail("unexpect ex in testGetBloomMapFile !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>BloomMapFile.Writer</c>
		/// constructors
		/// </summary>
		public virtual void testBloomMapFileConstructors()
		{
			org.apache.hadoop.io.BloomMapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem ts = org.apache.hadoop.fs.FileSystem.get(conf);
				string testFileName = TEST_FILE.ToString();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK
					, defaultCodec, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK
					, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK
					);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					, defaultCodec, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
				writer = new org.apache.hadoop.io.BloomMapFile.Writer(conf, ts, testFileName, org.apache.hadoop.io.WritableComparator
					.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)));
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.close();
			}
			catch (System.Exception)
			{
				fail("testBloomMapFileConstructors error !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		private sealed class _Progressable_282 : org.apache.hadoop.util.Progressable
		{
			public _Progressable_282()
			{
			}

			public void progress()
			{
			}
		}

		internal static readonly org.apache.hadoop.util.Progressable defaultProgress = new 
			_Progressable_282();

		private sealed class _CompressionCodec_288 : org.apache.hadoop.io.compress.CompressionCodec
		{
			public _CompressionCodec_288()
			{
			}

			public override string getDefaultExtension()
			{
				return null;
			}

			public override java.lang.Class getDecompressorType()
			{
				return null;
			}

			public override java.lang.Class getCompressorType()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
			{
				return org.mockito.Mockito.mock<org.apache.hadoop.io.compress.CompressionOutputStream
					>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out)
			{
				return org.mockito.Mockito.mock<org.apache.hadoop.io.compress.CompressionOutputStream
					>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in)
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Decompressor createDecompressor()
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Compressor createCompressor()
			{
				return null;
			}
		}

		internal static readonly org.apache.hadoop.io.compress.CompressionCodec defaultCodec
			 = new _CompressionCodec_288();
	}
}
