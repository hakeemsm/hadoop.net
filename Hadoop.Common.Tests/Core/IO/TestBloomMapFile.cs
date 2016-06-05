using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	public class TestBloomMapFile : TestCase
	{
		private static Configuration conf = new Configuration();

		private static readonly Path TestRoot = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestMapFile).Name);

		private static readonly Path TestDir = new Path(TestRoot, "testfile");

		private static readonly Path TestFile = new Path(TestRoot, "testfile");

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			LocalFileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(TestRoot) && !fs.Delete(TestRoot, true))
			{
				NUnit.Framework.Assert.Fail("Can't clean up test root dir");
			}
			fs.Mkdirs(TestRoot);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMembershipTest()
		{
			// write the file
			FileSystem fs = FileSystem.GetLocal(conf);
			Path qualifiedDirName = fs.MakeQualified(TestDir);
			conf.SetInt("io.mapfile.bloom.size", 2048);
			BloomMapFile.Writer writer = null;
			BloomMapFile.Reader reader = null;
			try
			{
				writer = new BloomMapFile.Writer(conf, fs, qualifiedDirName.ToString(), typeof(IntWritable
					), typeof(Text));
				IntWritable key = new IntWritable();
				Text value = new Text();
				for (int i = 0; i < 2000; i += 2)
				{
					key.Set(i);
					value.Set("00" + i);
					writer.Append(key, value);
				}
				writer.Close();
				reader = new BloomMapFile.Reader(fs, qualifiedDirName.ToString(), conf);
				// check false positives rate
				int falsePos = 0;
				int falseNeg = 0;
				for (int i_1 = 0; i_1 < 2000; i_1++)
				{
					key.Set(i_1);
					bool exists = reader.ProbablyHasKey(key);
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
				reader.Close();
				fs.Delete(qualifiedDirName, true);
				System.Console.Out.WriteLine("False negatives: " + falseNeg);
				Assert.Equal(0, falseNeg);
				System.Console.Out.WriteLine("False positives: " + falsePos);
				Assert.True(falsePos < 2);
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		private void CheckMembershipVaryingSizedKeys(string name, IList<Text> keys)
		{
			FileSystem fs = FileSystem.GetLocal(conf);
			Path qualifiedDirName = fs.MakeQualified(TestDir);
			BloomMapFile.Writer writer = null;
			BloomMapFile.Reader reader = null;
			try
			{
				writer = new BloomMapFile.Writer(conf, fs, qualifiedDirName.ToString(), typeof(Text
					), typeof(NullWritable));
				foreach (Text key in keys)
				{
					writer.Append(key, NullWritable.Get());
				}
				writer.Close();
				// will check for membership in opposite order of how keys were inserted
				reader = new BloomMapFile.Reader(fs, qualifiedDirName.ToString(), conf);
				Sharpen.Collections.Reverse(keys);
				foreach (Text key_1 in keys)
				{
					Assert.True("False negative for existing key " + key_1, reader.
						ProbablyHasKey(key_1));
				}
				reader.Close();
				fs.Delete(qualifiedDirName, true);
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMembershipVaryingSizedKeysTest1()
		{
			AList<Text> list = new AList<Text>();
			list.AddItem(new Text("A"));
			list.AddItem(new Text("BB"));
			CheckMembershipVaryingSizedKeys(GetName(), list);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMembershipVaryingSizedKeysTest2()
		{
			AList<Text> list = new AList<Text>();
			list.AddItem(new Text("AA"));
			list.AddItem(new Text("B"));
			CheckMembershipVaryingSizedKeys(GetName(), list);
		}

		/// <summary>
		/// test
		/// <c>BloomMapFile.delete()</c>
		/// method
		/// </summary>
		public virtual void TestDeleteFile()
		{
			BloomMapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				writer = new BloomMapFile.Writer(conf, TestFile, MapFile.Writer.KeyClass(typeof(IntWritable
					)), MapFile.Writer.ValueClass(typeof(Text)));
				NUnit.Framework.Assert.IsNotNull("testDeleteFile error !!!", writer);
				writer.Close();
				BloomMapFile.Delete(fs, TestFile.ToString());
			}
			catch (Exception)
			{
				Fail("unexpect ex in testDeleteFile !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <see cref="Reader"/>
		/// constructor with
		/// IOException
		/// </summary>
		public virtual void TestIOExceptionInWriterConstructor()
		{
			Path dirNameSpy = Org.Mockito.Mockito.Spy(TestFile);
			BloomMapFile.Reader reader = null;
			BloomMapFile.Writer writer = null;
			try
			{
				writer = new BloomMapFile.Writer(conf, TestFile, MapFile.Writer.KeyClass(typeof(IntWritable
					)), MapFile.Writer.ValueClass(typeof(Text)));
				writer.Append(new IntWritable(1), new Text("123124142"));
				writer.Close();
				Org.Mockito.Mockito.When(dirNameSpy.GetFileSystem(conf)).ThenThrow(new IOException
					());
				reader = new BloomMapFile.Reader(dirNameSpy, conf, MapFile.Reader.Comparator(new 
					WritableComparator(typeof(IntWritable))));
				NUnit.Framework.Assert.IsNull("testIOExceptionInWriterConstructor error !!!", reader
					.GetBloomFilter());
			}
			catch (Exception)
			{
				Fail("unexpect ex in testIOExceptionInWriterConstructor !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <see>BloomMapFile.Reader.get()</see>
		/// method
		/// </summary>
		public virtual void TestGetBloomMapFile()
		{
			int Size = 10;
			BloomMapFile.Reader reader = null;
			BloomMapFile.Writer writer = null;
			try
			{
				writer = new BloomMapFile.Writer(conf, TestFile, MapFile.Writer.KeyClass(typeof(IntWritable
					)), MapFile.Writer.ValueClass(typeof(Text)));
				for (int i = 0; i < Size; i++)
				{
					writer.Append(new IntWritable(i), new Text());
				}
				writer.Close();
				reader = new BloomMapFile.Reader(TestFile, conf, MapFile.Reader.Comparator(new WritableComparator
					(typeof(IntWritable))));
				for (int i_1 = 0; i_1 < Size; i_1++)
				{
					NUnit.Framework.Assert.IsNotNull("testGetBloomMapFile error !!!", reader.Get(new 
						IntWritable(i_1), new Text()));
				}
				NUnit.Framework.Assert.IsNull("testGetBloomMapFile error !!!", reader.Get(new IntWritable
					(Size + 5), new Text()));
			}
			catch (Exception)
			{
				Fail("unexpect ex in testGetBloomMapFile !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>BloomMapFile.Writer</c>
		/// constructors
		/// </summary>
		public virtual void TestBloomMapFileConstructors()
		{
			BloomMapFile.Writer writer = null;
			try
			{
				FileSystem ts = FileSystem.Get(conf);
				string testFileName = TestFile.ToString();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Block, defaultCodec, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Block, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Block);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Record, defaultCodec, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Record, defaultProgress);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, typeof(IntWritable), typeof(
					Text), SequenceFile.CompressionType.Record);
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
				writer = new BloomMapFile.Writer(conf, ts, testFileName, WritableComparator.Get(typeof(
					Text)), typeof(Text));
				NUnit.Framework.Assert.IsNotNull("testBloomMapFileConstructors error !!!", writer
					);
				writer.Close();
			}
			catch (Exception)
			{
				Fail("testBloomMapFileConstructors error !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		private sealed class _Progressable_282 : Progressable
		{
			public _Progressable_282()
			{
			}

			public void Progress()
			{
			}
		}

		internal static readonly Progressable defaultProgress = new _Progressable_282();

		private sealed class _CompressionCodec_288 : CompressionCodec
		{
			public _CompressionCodec_288()
			{
			}

			public override string GetDefaultExtension()
			{
				return null;
			}

			public override Type GetDecompressorType()
			{
				return null;
			}

			public override Type GetCompressorType()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
				 compressor)
			{
				return Org.Mockito.Mockito.Mock<CompressionOutputStream>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out)
			{
				return Org.Mockito.Mockito.Mock<CompressionOutputStream>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in, Decompressor
				 decompressor)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in)
			{
				return null;
			}

			public override Decompressor CreateDecompressor()
			{
				return null;
			}

			public override Compressor CreateCompressor()
			{
				return null;
			}
		}

		internal static readonly CompressionCodec defaultCodec = new _CompressionCodec_288
			();
	}
}
