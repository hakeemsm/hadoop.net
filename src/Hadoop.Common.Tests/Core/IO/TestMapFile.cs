using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	public class TestMapFile
	{
		private static readonly Path TestDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestMapFile).Name);

		private static Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			LocalFileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(TestDir) && !fs.Delete(TestDir, true))
			{
				NUnit.Framework.Assert.Fail("Can't clean up test root dir");
			}
			fs.Mkdirs(TestDir);
		}

		private sealed class _Progressable_66 : Progressable
		{
			public _Progressable_66()
			{
			}

			public void Progress()
			{
			}
		}

		private static readonly Progressable defaultProgressable = new _Progressable_66();

		private sealed class _CompressionCodec_72 : CompressionCodec
		{
			public _CompressionCodec_72()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out)
			{
				return Org.Mockito.Mockito.Mock<CompressionOutputStream>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out, Compressor
				 compressor)
			{
				return Org.Mockito.Mockito.Mock<CompressionOutputStream>();
			}

			public override Type GetCompressorType()
			{
				return null;
			}

			public override Compressor CreateCompressor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override CompressionInputStream CreateInputStream(InputStream @in, Decompressor
				 decompressor)
			{
				return null;
			}

			public override Type GetDecompressorType()
			{
				return null;
			}

			public override Decompressor CreateDecompressor()
			{
				return null;
			}

			public override string GetDefaultExtension()
			{
				return null;
			}
		}

		private static readonly CompressionCodec defaultCodec = new _CompressionCodec_72(
			);

		/// <exception cref="System.IO.IOException"/>
		private MapFile.Writer CreateWriter(string fileName, Type keyClass, Type valueClass
			)
		{
			Path dirName = new Path(TestDir, fileName);
			MapFile.Writer.SetIndexInterval(conf, 4);
			return new MapFile.Writer(conf, dirName, MapFile.Writer.KeyClass(keyClass), MapFile.Writer
				.ValueClass(valueClass));
		}

		/// <exception cref="System.IO.IOException"/>
		private MapFile.Reader CreateReader(string fileName, Type keyClass)
		{
			Path dirName = new Path(TestDir, fileName);
			return new MapFile.Reader(dirName, conf, MapFile.Reader.Comparator(new WritableComparator
				(keyClass)));
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.getClosest()</c>
		/// method
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetClosestOnCurrentApi()
		{
			string TestPrefix = "testGetClosestOnCurrentApi.mapfile";
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = CreateWriter(TestPrefix, typeof(Text), typeof(Text));
				int FirstKey = 1;
				// Test keys: 11,21,31,...,91
				for (int i = FirstKey; i < 100; i += 10)
				{
					Text t = new Text(Extensions.ToString(i));
					writer.Append(t, t);
				}
				writer.Close();
				reader = CreateReader(TestPrefix, typeof(Text));
				Text key = new Text("55");
				Text value = new Text();
				// Test get closest with step forward
				Text closest = (Text)reader.GetClosest(key, value);
				Assert.Equal(new Text("61"), closest);
				// Test get closest with step back
				closest = (Text)reader.GetClosest(key, value, true);
				Assert.Equal(new Text("51"), closest);
				// Test get closest when we pass explicit key
				Text explicitKey = new Text("21");
				closest = (Text)reader.GetClosest(explicitKey, value);
				Assert.Equal(new Text("21"), explicitKey);
				// Test what happens at boundaries. Assert if searching a key that is
				// less than first key in the mapfile, that the first key is returned.
				key = new Text("00");
				closest = (Text)reader.GetClosest(key, value);
				Assert.Equal(FirstKey, System.Convert.ToInt32(closest.ToString
					()));
				// Assert that null is returned if key is > last entry in mapfile.
				key = new Text("92");
				closest = (Text)reader.GetClosest(key, value);
				NUnit.Framework.Assert.IsNull("Not null key in testGetClosestWithNewCode", closest
					);
				// If we were looking for the key before, we should get the last key
				closest = (Text)reader.GetClosest(key, value, true);
				Assert.Equal(new Text("91"), closest);
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.midKey()</c>
		/// method
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMidKeyOnCurrentApi()
		{
			// Write a mapfile of simple data: keys are
			string TestPrefix = "testMidKeyOnCurrentApi.mapfile";
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = CreateWriter(TestPrefix, typeof(IntWritable), typeof(IntWritable));
				// 0,1,....9
				int Size = 10;
				for (int i = 0; i < Size; i++)
				{
					writer.Append(new IntWritable(i), new IntWritable(i));
				}
				writer.Close();
				reader = CreateReader(TestPrefix, typeof(IntWritable));
				Assert.Equal(new IntWritable((Size - 1) / 2), reader.MidKey());
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.rename()</c>
		/// method
		/// </summary>
		[Fact]
		public virtual void TestRename()
		{
			string NewFileName = "test-new.mapfile";
			string OldFileName = "test-old.mapfile";
			MapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				writer = CreateWriter(OldFileName, typeof(IntWritable), typeof(IntWritable));
				writer.Close();
				MapFile.Rename(fs, new Path(TestDir, OldFileName).ToString(), new Path(TestDir, NewFileName
					).ToString());
				MapFile.Delete(fs, new Path(TestDir, NewFileName).ToString());
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail("testRename error " + ex);
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.rename()</c>
		/// 
		/// method with throwing
		/// <c>IOException</c>
		/// 
		/// </summary>
		[Fact]
		public virtual void TestRenameWithException()
		{
			string ErrorMessage = "Can't rename file";
			string NewFileName = "test-new.mapfile";
			string OldFileName = "test-old.mapfile";
			MapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				FileSystem spyFs = Org.Mockito.Mockito.Spy(fs);
				writer = CreateWriter(OldFileName, typeof(IntWritable), typeof(IntWritable));
				writer.Close();
				Path oldDir = new Path(TestDir, OldFileName);
				Path newDir = new Path(TestDir, NewFileName);
				Org.Mockito.Mockito.When(spyFs.Rename(oldDir, newDir)).ThenThrow(new IOException(
					ErrorMessage));
				MapFile.Rename(spyFs, oldDir.ToString(), newDir.ToString());
				NUnit.Framework.Assert.Fail("testRenameWithException no exception error !!!");
			}
			catch (IOException ex)
			{
				Assert.Equal("testRenameWithException invalid IOExceptionMessage !!!"
					, ex.Message, ErrorMessage);
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		[Fact]
		public virtual void TestRenameWithFalse()
		{
			string ErrorMessage = "Could not rename";
			string NewFileName = "test-new.mapfile";
			string OldFileName = "test-old.mapfile";
			MapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				FileSystem spyFs = Org.Mockito.Mockito.Spy(fs);
				writer = CreateWriter(OldFileName, typeof(IntWritable), typeof(IntWritable));
				writer.Close();
				Path oldDir = new Path(TestDir, OldFileName);
				Path newDir = new Path(TestDir, NewFileName);
				Org.Mockito.Mockito.When(spyFs.Rename(oldDir, newDir)).ThenReturn(false);
				MapFile.Rename(spyFs, oldDir.ToString(), newDir.ToString());
				NUnit.Framework.Assert.Fail("testRenameWithException no exception error !!!");
			}
			catch (IOException ex)
			{
				Assert.True("testRenameWithFalse invalid IOExceptionMessage error !!!"
					, ex.Message.StartsWith(ErrorMessage));
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test throwing
		/// <c>IOException</c>
		/// in
		/// <c>MapFile.Writer</c>
		/// constructor
		/// </summary>
		[Fact]
		public virtual void TestWriteWithFailDirCreation()
		{
			string ErrorMessage = "Mkdirs failed to create directory";
			Path dirName = new Path(TestDir, "fail.mapfile");
			MapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				FileSystem spyFs = Org.Mockito.Mockito.Spy(fs);
				Path pathSpy = Org.Mockito.Mockito.Spy(dirName);
				Org.Mockito.Mockito.When(pathSpy.GetFileSystem(conf)).ThenReturn(spyFs);
				Org.Mockito.Mockito.When(spyFs.Mkdirs(dirName)).ThenReturn(false);
				writer = new MapFile.Writer(conf, pathSpy, MapFile.Writer.KeyClass(typeof(IntWritable
					)), MapFile.Writer.ValueClass(typeof(Text)));
				NUnit.Framework.Assert.Fail("testWriteWithFailDirCreation error !!!");
			}
			catch (IOException ex)
			{
				Assert.True("testWriteWithFailDirCreation ex error !!!", ex.Message
					.StartsWith(ErrorMessage));
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.finalKey()</c>
		/// method
		/// </summary>
		[Fact]
		public virtual void TestOnFinalKey()
		{
			string TestMethodKey = "testOnFinalKey.mapfile";
			int Size = 10;
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = CreateWriter(TestMethodKey, typeof(IntWritable), typeof(IntWritable));
				for (int i = 0; i < Size; i++)
				{
					writer.Append(new IntWritable(i), new IntWritable(i));
				}
				writer.Close();
				reader = CreateReader(TestMethodKey, typeof(IntWritable));
				IntWritable expectedKey = new IntWritable(0);
				reader.FinalKey(expectedKey);
				Assert.Equal("testOnFinalKey not same !!!", expectedKey, new IntWritable
					(9));
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("testOnFinalKey error !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer</c>
		/// constructor with key, value
		/// and validate it with
		/// <c>keyClass(), valueClass()</c>
		/// methods
		/// </summary>
		[Fact]
		public virtual void TestKeyValueClasses()
		{
			Type keyClass = typeof(IntWritable);
			Type valueClass = typeof(Text);
			try
			{
				CreateWriter("testKeyValueClasses.mapfile", typeof(IntWritable), typeof(Text)).Close
					();
				NUnit.Framework.Assert.IsNotNull("writer key class null error !!!", MapFile.Writer
					.KeyClass(keyClass));
				NUnit.Framework.Assert.IsNotNull("writer value class null error !!!", MapFile.Writer
					.ValueClass(valueClass));
			}
			catch (IOException ex)
			{
				NUnit.Framework.Assert.Fail(ex.Message);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.getClosest()</c>
		/// with wrong class key
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestReaderGetClosest()
		{
			string TestMethodKey = "testReaderWithWrongKeyClass.mapfile";
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = CreateWriter(TestMethodKey, typeof(IntWritable), typeof(Text));
				for (int i = 0; i < 10; i++)
				{
					writer.Append(new IntWritable(i), new Text("value" + i));
				}
				writer.Close();
				reader = CreateReader(TestMethodKey, typeof(Text));
				reader.GetClosest(new Text("2"), new Text(string.Empty));
				NUnit.Framework.Assert.Fail("no excepted exception in testReaderWithWrongKeyClass !!!"
					);
			}
			catch (IOException)
			{
			}
			finally
			{
				/* Should be thrown to pass the test */
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.append()</c>
		/// with wrong key class
		/// </summary>
		[Fact]
		public virtual void TestReaderWithWrongValueClass()
		{
			string TestMethodKey = "testReaderWithWrongValueClass.mapfile";
			MapFile.Writer writer = null;
			try
			{
				writer = CreateWriter(TestMethodKey, typeof(IntWritable), typeof(Text));
				writer.Append(new IntWritable(0), new IntWritable(0));
				NUnit.Framework.Assert.Fail("no excepted exception in testReaderWithWrongKeyClass !!!"
					);
			}
			catch (IOException)
			{
			}
			finally
			{
				/* Should be thrown to pass the test */
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.next(key, value)</c>
		/// for iteration.
		/// </summary>
		[Fact]
		public virtual void TestReaderKeyIteration()
		{
			string TestMethodKey = "testReaderKeyIteration.mapfile";
			int Size = 10;
			int Iterations = 5;
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = CreateWriter(TestMethodKey, typeof(IntWritable), typeof(Text));
				int start = 0;
				for (int i = 0; i < Size; i++)
				{
					writer.Append(new IntWritable(i), new Text("Value:" + i));
				}
				writer.Close();
				reader = CreateReader(TestMethodKey, typeof(IntWritable));
				// test iteration
				Writable startValue = new Text("Value:" + start);
				int i_1 = 0;
				while (i_1++ < Iterations)
				{
					IntWritable key = new IntWritable(start);
					Writable value = startValue;
					while (reader.Next(key, value))
					{
						NUnit.Framework.Assert.IsNotNull(key);
						NUnit.Framework.Assert.IsNotNull(value);
					}
					reader.Reset();
				}
				Assert.True("reader seek error !!!", reader.Seek(new IntWritable
					(Size / 2)));
				NUnit.Framework.Assert.IsFalse("reader seek error !!!", reader.Seek(new IntWritable
					(Size * 2)));
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("reader seek error !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.testFix</c>
		/// method
		/// </summary>
		[Fact]
		public virtual void TestFix()
		{
			string IndexLessMapFile = "testFix.mapfile";
			int PairSize = 20;
			MapFile.Writer writer = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				Path dir = new Path(TestDir, IndexLessMapFile);
				writer = CreateWriter(IndexLessMapFile, typeof(IntWritable), typeof(Text));
				for (int i = 0; i < PairSize; i++)
				{
					writer.Append(new IntWritable(0), new Text("value"));
				}
				writer.Close();
				FilePath indexFile = new FilePath(".", "." + IndexLessMapFile + "/index");
				bool isDeleted = false;
				if (indexFile.Exists())
				{
					isDeleted = indexFile.Delete();
				}
				if (isDeleted)
				{
					Assert.True("testFix error !!!", MapFile.Fix(fs, dir, typeof(IntWritable
						), typeof(Text), true, conf) == PairSize);
				}
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testFix error !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test all available constructor for
		/// <c>MapFile.Writer</c>
		/// </summary>
		[Fact]
		public virtual void TestDeprecatedConstructors()
		{
			string path = new Path(TestDir, "writes.mapfile").ToString();
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				FileSystem fs = FileSystem.GetLocal(conf);
				writer = new MapFile.Writer(conf, fs, path, typeof(IntWritable), typeof(Text), SequenceFile.CompressionType
					.Record);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				writer = new MapFile.Writer(conf, fs, path, typeof(IntWritable), typeof(Text), SequenceFile.CompressionType
					.Record, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				writer = new MapFile.Writer(conf, fs, path, typeof(IntWritable), typeof(Text), SequenceFile.CompressionType
					.Record, defaultCodec, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				writer = new MapFile.Writer(conf, fs, path, WritableComparator.Get(typeof(Text)), 
					typeof(Text));
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				writer = new MapFile.Writer(conf, fs, path, WritableComparator.Get(typeof(Text)), 
					typeof(Text), SequenceFile.CompressionType.Record);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				writer = new MapFile.Writer(conf, fs, path, WritableComparator.Get(typeof(Text)), 
					typeof(Text), SequenceFile.CompressionType.Record, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.Close();
				reader = new MapFile.Reader(fs, path, WritableComparator.Get(typeof(IntWritable))
					, conf);
				NUnit.Framework.Assert.IsNotNull(reader);
				NUnit.Framework.Assert.IsNotNull("reader key is null !!!", reader.GetKeyClass());
				NUnit.Framework.Assert.IsNotNull("reader value in null", reader.GetValueClass());
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer</c>
		/// constructor
		/// with IllegalArgumentException
		/// </summary>
		[Fact]
		public virtual void TestKeyLessWriterCreation()
		{
			MapFile.Writer writer = null;
			try
			{
				writer = new MapFile.Writer(conf, TestDir);
				NUnit.Framework.Assert.Fail("fail in testKeyLessWriterCreation !!!");
			}
			catch (ArgumentException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("fail in testKeyLessWriterCreation. Other ex !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer</c>
		/// constructor with IOException
		/// </summary>
		[Fact]
		public virtual void TestPathExplosionWriterCreation()
		{
			Path path = new Path(TestDir, "testPathExplosionWriterCreation.mapfile");
			string TestErrorMessage = "Mkdirs failed to create directory " + path.GetName();
			MapFile.Writer writer = null;
			try
			{
				FileSystem fsSpy = Org.Mockito.Mockito.Spy(FileSystem.Get(conf));
				Path pathSpy = Org.Mockito.Mockito.Spy(path);
				Org.Mockito.Mockito.When(fsSpy.Mkdirs(path)).ThenThrow(new IOException(TestErrorMessage
					));
				Org.Mockito.Mockito.When(pathSpy.GetFileSystem(conf)).ThenReturn(fsSpy);
				writer = new MapFile.Writer(conf, pathSpy, MapFile.Writer.KeyClass(typeof(IntWritable
					)), MapFile.Writer.ValueClass(typeof(IntWritable)));
				NUnit.Framework.Assert.Fail("fail in testPathExplosionWriterCreation !!!");
			}
			catch (IOException ex)
			{
				Assert.Equal("testPathExplosionWriterCreation ex message error !!!"
					, ex.Message, TestErrorMessage);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("fail in testPathExplosionWriterCreation. Other ex !!!"
					);
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.append</c>
		/// method with desc order
		/// </summary>
		[Fact]
		public virtual void TestDescOrderWithThrowExceptionWriterAppend()
		{
			MapFile.Writer writer = null;
			try
			{
				writer = CreateWriter(".mapfile", typeof(IntWritable), typeof(Text));
				writer.Append(new IntWritable(2), new Text("value: " + 1));
				writer.Append(new IntWritable(2), new Text("value: " + 2));
				writer.Append(new IntWritable(2), new Text("value: " + 4));
				writer.Append(new IntWritable(1), new Text("value: " + 3));
				NUnit.Framework.Assert.Fail("testDescOrderWithThrowExceptionWriterAppend not expected exception error !!!"
					);
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testDescOrderWithThrowExceptionWriterAppend other ex throw !!!"
					);
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		[Fact]
		public virtual void TestMainMethodMapFile()
		{
			string inFile = "mainMethodMapFile.mapfile";
			string path = new Path(TestDir, inFile).ToString();
			string[] args = new string[] { path, path };
			MapFile.Writer writer = null;
			try
			{
				writer = CreateWriter(inFile, typeof(IntWritable), typeof(Text));
				writer.Append(new IntWritable(1), new Text("test_text1"));
				writer.Append(new IntWritable(2), new Text("test_text2"));
				writer.Close();
				MapFile.Main(args);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("testMainMethodMapFile error !!!");
			}
			finally
			{
				IOUtils.Cleanup(null, writer);
			}
		}

		/// <summary>Test getClosest feature.</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetClosest()
		{
			// Write a mapfile of simple data: keys are
			Path dirName = new Path(TestDir, "testGetClosest.mapfile");
			FileSystem fs = FileSystem.GetLocal(conf);
			Path qualifiedDirName = fs.MakeQualified(dirName);
			// Make an index entry for every third insertion.
			MapFile.Writer.SetIndexInterval(conf, 3);
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = new MapFile.Writer(conf, fs, qualifiedDirName.ToString(), typeof(Text), 
					typeof(Text));
				// Assert that the index interval is 1
				Assert.Equal(3, writer.GetIndexInterval());
				// Add entries up to 100 in intervals of ten.
				int FirstKey = 10;
				for (int i = FirstKey; i < 100; i += 10)
				{
					string iStr = Extensions.ToString(i);
					Text t = new Text(Runtime.Substring("00", iStr.Length) + iStr);
					writer.Append(t, t);
				}
				writer.Close();
				// Now do getClosest on created mapfile.
				reader = new MapFile.Reader(qualifiedDirName, conf);
				Text key = new Text("55");
				Text value = new Text();
				Text closest = (Text)reader.GetClosest(key, value);
				// Assert that closest after 55 is 60
				Assert.Equal(new Text("60"), closest);
				// Get closest that falls before the passed key: 50
				closest = (Text)reader.GetClosest(key, value, true);
				Assert.Equal(new Text("50"), closest);
				// Test get closest when we pass explicit key
				Text Twenty = new Text("20");
				closest = (Text)reader.GetClosest(Twenty, value);
				Assert.Equal(Twenty, closest);
				closest = (Text)reader.GetClosest(Twenty, value, true);
				Assert.Equal(Twenty, closest);
				// Test what happens at boundaries. Assert if searching a key that is
				// less than first key in the mapfile, that the first key is returned.
				key = new Text("00");
				closest = (Text)reader.GetClosest(key, value);
				Assert.Equal(FirstKey, System.Convert.ToInt32(closest.ToString
					()));
				// If we're looking for the first key before, and we pass in a key before
				// the first key in the file, we should get null
				closest = (Text)reader.GetClosest(key, value, true);
				NUnit.Framework.Assert.IsNull(closest);
				// Assert that null is returned if key is > last entry in mapfile.
				key = new Text("99");
				closest = (Text)reader.GetClosest(key, value);
				NUnit.Framework.Assert.IsNull(closest);
				// If we were looking for the key before, we should get the last key
				closest = (Text)reader.GetClosest(key, value, true);
				Assert.Equal(new Text("90"), closest);
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMidKey()
		{
			// Write a mapfile of simple data: keys are
			Path dirName = new Path(TestDir, "testMidKey.mapfile");
			FileSystem fs = FileSystem.GetLocal(conf);
			Path qualifiedDirName = fs.MakeQualified(dirName);
			MapFile.Writer writer = null;
			MapFile.Reader reader = null;
			try
			{
				writer = new MapFile.Writer(conf, fs, qualifiedDirName.ToString(), typeof(IntWritable
					), typeof(IntWritable));
				writer.Append(new IntWritable(1), new IntWritable(1));
				writer.Close();
				// Now do getClosest on created mapfile.
				reader = new MapFile.Reader(qualifiedDirName, conf);
				Assert.Equal(new IntWritable(1), reader.MidKey());
			}
			finally
			{
				IOUtils.Cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMidKeyEmpty()
		{
			// Write a mapfile of simple data: keys are
			Path dirName = new Path(TestDir, "testMidKeyEmpty.mapfile");
			FileSystem fs = FileSystem.GetLocal(conf);
			Path qualifiedDirName = fs.MakeQualified(dirName);
			MapFile.Writer writer = new MapFile.Writer(conf, fs, qualifiedDirName.ToString(), 
				typeof(IntWritable), typeof(IntWritable));
			writer.Close();
			// Now do getClosest on created mapfile.
			MapFile.Reader reader = new MapFile.Reader(qualifiedDirName, conf);
			try
			{
				Assert.Equal(null, reader.MidKey());
			}
			finally
			{
				reader.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMerge()
		{
			string TestMethodKey = "testMerge.mapfile";
			int Size = 10;
			int Iterations = 5;
			Path[] @in = new Path[5];
			IList<int> expected = new AList<int>();
			for (int j = 0; j < 5; j++)
			{
				using (MapFile.Writer writer = CreateWriter(TestMethodKey + "." + j, typeof(IntWritable
					), typeof(Text)))
				{
					@in[j] = new Path(TestDir, TestMethodKey + "." + j);
					for (int i = 0; i < Size; i++)
					{
						expected.AddItem(i + j);
						writer.Append(new IntWritable(i + j), new Text("Value:" + (i + j)));
					}
				}
			}
			// Sort expected values
			expected.Sort();
			// Merge all 5 files
			MapFile.Merger merger = new MapFile.Merger(conf);
			merger.Merge(@in, true, new Path(TestDir, TestMethodKey));
			using (MapFile.Reader reader = CreateReader(TestMethodKey, typeof(IntWritable)))
			{
				int start = 0;
				// test iteration
				Text startValue = new Text("Value:" + start);
				int i = 0;
				while (i++ < Iterations)
				{
					IEnumerator<int> expectedIterator = expected.GetEnumerator();
					IntWritable key = new IntWritable(start);
					Text value = startValue;
					IntWritable prev = new IntWritable(start);
					while (reader.Next(key, value))
					{
						Assert.True("Next key should be always equal or more", prev.Get
							() <= key.Get());
						Assert.Equal(expectedIterator.Next(), key.Get());
						prev.Set(key.Get());
					}
					reader.Reset();
				}
			}
			// inputs should be deleted
			for (int j_1 = 0; j_1 < @in.Length; j_1++)
			{
				Path path = @in[j_1];
				NUnit.Framework.Assert.IsFalse("inputs should be deleted", path.GetFileSystem(conf
					).Exists(path));
			}
		}
	}
}
