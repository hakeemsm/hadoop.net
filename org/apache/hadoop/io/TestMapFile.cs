using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestMapFile
	{
		private static readonly org.apache.hadoop.fs.Path TEST_DIR = new org.apache.hadoop.fs.Path
			(Sharpen.Runtime.getProperty("test.build.data", "/tmp"), Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.io.TestMapFile)).getSimpleName());

		private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			org.apache.hadoop.fs.LocalFileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			if (fs.exists(TEST_DIR) && !fs.delete(TEST_DIR, true))
			{
				NUnit.Framework.Assert.Fail("Can't clean up test root dir");
			}
			fs.mkdirs(TEST_DIR);
		}

		private sealed class _Progressable_66 : org.apache.hadoop.util.Progressable
		{
			public _Progressable_66()
			{
			}

			public void progress()
			{
			}
		}

		private static readonly org.apache.hadoop.util.Progressable defaultProgressable = 
			new _Progressable_66();

		private sealed class _CompressionCodec_72 : org.apache.hadoop.io.compress.CompressionCodec
		{
			public _CompressionCodec_72()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out)
			{
				return org.mockito.Mockito.mock<org.apache.hadoop.io.compress.CompressionOutputStream
					>();
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionOutputStream createOutputStream
				(java.io.OutputStream @out, org.apache.hadoop.io.compress.Compressor compressor)
			{
				return org.mockito.Mockito.mock<org.apache.hadoop.io.compress.CompressionOutputStream
					>();
			}

			public override java.lang.Class getCompressorType()
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Compressor createCompressor()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.compress.CompressionInputStream createInputStream
				(java.io.InputStream @in, org.apache.hadoop.io.compress.Decompressor decompressor
				)
			{
				return null;
			}

			public override java.lang.Class getDecompressorType()
			{
				return null;
			}

			public override org.apache.hadoop.io.compress.Decompressor createDecompressor()
			{
				return null;
			}

			public override string getDefaultExtension()
			{
				return null;
			}
		}

		private static readonly org.apache.hadoop.io.compress.CompressionCodec defaultCodec
			 = new _CompressionCodec_72();

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.io.MapFile.Writer createWriter(string fileName, java.lang.Class
			 keyClass, java.lang.Class valueClass)
		{
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, fileName
				);
			org.apache.hadoop.io.MapFile.Writer.setIndexInterval(conf, 4);
			return new org.apache.hadoop.io.MapFile.Writer(conf, dirName, org.apache.hadoop.io.MapFile.Writer
				.keyClass(keyClass), org.apache.hadoop.io.MapFile.Writer.valueClass(valueClass));
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.io.MapFile.Reader createReader(string fileName, java.lang.Class
			 keyClass)
		{
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, fileName
				);
			return new org.apache.hadoop.io.MapFile.Reader(dirName, conf, org.apache.hadoop.io.MapFile.Reader
				.comparator(new org.apache.hadoop.io.WritableComparator(keyClass)));
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.getClosest()</c>
		/// method
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetClosestOnCurrentApi()
		{
			string TEST_PREFIX = "testGetClosestOnCurrentApi.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = createWriter(TEST_PREFIX, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				int FIRST_KEY = 1;
				// Test keys: 11,21,31,...,91
				for (int i = FIRST_KEY; i < 100; i += 10)
				{
					org.apache.hadoop.io.Text t = new org.apache.hadoop.io.Text(int.toString(i));
					writer.append(t, t);
				}
				writer.close();
				reader = createReader(TEST_PREFIX, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
					)));
				org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text("55");
				org.apache.hadoop.io.Text value = new org.apache.hadoop.io.Text();
				// Test get closest with step forward
				org.apache.hadoop.io.Text closest = (org.apache.hadoop.io.Text)reader.getClosest(
					key, value);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("61"), closest);
				// Test get closest with step back
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value, true);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("51"), closest);
				// Test get closest when we pass explicit key
				org.apache.hadoop.io.Text explicitKey = new org.apache.hadoop.io.Text("21");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(explicitKey, value);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("21"), explicitKey);
				// Test what happens at boundaries. Assert if searching a key that is
				// less than first key in the mapfile, that the first key is returned.
				key = new org.apache.hadoop.io.Text("00");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value);
				NUnit.Framework.Assert.AreEqual(FIRST_KEY, System.Convert.ToInt32(closest.ToString
					()));
				// Assert that null is returned if key is > last entry in mapfile.
				key = new org.apache.hadoop.io.Text("92");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value);
				NUnit.Framework.Assert.IsNull("Not null key in testGetClosestWithNewCode", closest
					);
				// If we were looking for the key before, we should get the last key
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value, true);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("91"), closest);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.midKey()</c>
		/// method
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMidKeyOnCurrentApi()
		{
			// Write a mapfile of simple data: keys are
			string TEST_PREFIX = "testMidKeyOnCurrentApi.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = createWriter(TEST_PREFIX, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
				// 0,1,....9
				int SIZE = 10;
				for (int i = 0; i < SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(i), new org.apache.hadoop.io.IntWritable
						(i));
				}
				writer.close();
				reader = createReader(TEST_PREFIX, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.IntWritable((SIZE - 1) /
					 2), reader.midKey());
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.rename()</c>
		/// method
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testRename()
		{
			string NEW_FILE_NAME = "test-new.mapfile";
			string OLD_FILE_NAME = "test-old.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				writer = createWriter(OLD_FILE_NAME, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
				writer.close();
				org.apache.hadoop.io.MapFile.rename(fs, new org.apache.hadoop.fs.Path(TEST_DIR, OLD_FILE_NAME
					).ToString(), new org.apache.hadoop.fs.Path(TEST_DIR, NEW_FILE_NAME).ToString());
				org.apache.hadoop.io.MapFile.delete(fs, new org.apache.hadoop.fs.Path(TEST_DIR, NEW_FILE_NAME
					).ToString());
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.Fail("testRename error " + ex);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
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
		[NUnit.Framework.Test]
		public virtual void testRenameWithException()
		{
			string ERROR_MESSAGE = "Can't rename file";
			string NEW_FILE_NAME = "test-new.mapfile";
			string OLD_FILE_NAME = "test-old.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				org.apache.hadoop.fs.FileSystem spyFs = org.mockito.Mockito.spy(fs);
				writer = createWriter(OLD_FILE_NAME, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
				writer.close();
				org.apache.hadoop.fs.Path oldDir = new org.apache.hadoop.fs.Path(TEST_DIR, OLD_FILE_NAME
					);
				org.apache.hadoop.fs.Path newDir = new org.apache.hadoop.fs.Path(TEST_DIR, NEW_FILE_NAME
					);
				org.mockito.Mockito.when(spyFs.rename(oldDir, newDir)).thenThrow(new System.IO.IOException
					(ERROR_MESSAGE));
				org.apache.hadoop.io.MapFile.rename(spyFs, oldDir.ToString(), newDir.ToString());
				NUnit.Framework.Assert.Fail("testRenameWithException no exception error !!!");
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.AreEqual("testRenameWithException invalid IOExceptionMessage !!!"
					, ex.Message, ERROR_MESSAGE);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testRenameWithFalse()
		{
			string ERROR_MESSAGE = "Could not rename";
			string NEW_FILE_NAME = "test-new.mapfile";
			string OLD_FILE_NAME = "test-old.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				org.apache.hadoop.fs.FileSystem spyFs = org.mockito.Mockito.spy(fs);
				writer = createWriter(OLD_FILE_NAME, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
				writer.close();
				org.apache.hadoop.fs.Path oldDir = new org.apache.hadoop.fs.Path(TEST_DIR, OLD_FILE_NAME
					);
				org.apache.hadoop.fs.Path newDir = new org.apache.hadoop.fs.Path(TEST_DIR, NEW_FILE_NAME
					);
				org.mockito.Mockito.when(spyFs.rename(oldDir, newDir)).thenReturn(false);
				org.apache.hadoop.io.MapFile.rename(spyFs, oldDir.ToString(), newDir.ToString());
				NUnit.Framework.Assert.Fail("testRenameWithException no exception error !!!");
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.IsTrue("testRenameWithFalse invalid IOExceptionMessage error !!!"
					, ex.Message.StartsWith(ERROR_MESSAGE));
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test throwing
		/// <c>IOException</c>
		/// in
		/// <c>MapFile.Writer</c>
		/// constructor
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testWriteWithFailDirCreation()
		{
			string ERROR_MESSAGE = "Mkdirs failed to create directory";
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, "fail.mapfile"
				);
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				org.apache.hadoop.fs.FileSystem spyFs = org.mockito.Mockito.spy(fs);
				org.apache.hadoop.fs.Path pathSpy = org.mockito.Mockito.spy(dirName);
				org.mockito.Mockito.when(pathSpy.getFileSystem(conf)).thenReturn(spyFs);
				org.mockito.Mockito.when(spyFs.mkdirs(dirName)).thenReturn(false);
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, pathSpy, org.apache.hadoop.io.MapFile.Writer
					.keyClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					))), org.apache.hadoop.io.MapFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text))));
				NUnit.Framework.Assert.Fail("testWriteWithFailDirCreation error !!!");
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.IsTrue("testWriteWithFailDirCreation ex error !!!", ex.Message
					.StartsWith(ERROR_MESSAGE));
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.finalKey()</c>
		/// method
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testOnFinalKey()
		{
			string TEST_METHOD_KEY = "testOnFinalKey.mapfile";
			int SIZE = 10;
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = createWriter(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
				for (int i = 0; i < SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(i), new org.apache.hadoop.io.IntWritable
						(i));
				}
				writer.close();
				reader = createReader(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)));
				org.apache.hadoop.io.IntWritable expectedKey = new org.apache.hadoop.io.IntWritable
					(0);
				reader.finalKey(expectedKey);
				NUnit.Framework.Assert.AreEqual("testOnFinalKey not same !!!", expectedKey, new org.apache.hadoop.io.IntWritable
					(9));
			}
			catch (System.IO.IOException)
			{
				NUnit.Framework.Assert.Fail("testOnFinalKey error !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
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
		[NUnit.Framework.Test]
		public virtual void testKeyValueClasses()
		{
			java.lang.Class keyClass = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
				));
			java.lang.Class valueClass = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
				));
			try
			{
				createWriter("testKeyValueClasses.mapfile", Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
					))).close();
				NUnit.Framework.Assert.IsNotNull("writer key class null error !!!", org.apache.hadoop.io.MapFile.Writer
					.keyClass(keyClass));
				NUnit.Framework.Assert.IsNotNull("writer value class null error !!!", org.apache.hadoop.io.MapFile.Writer
					.valueClass(valueClass));
			}
			catch (System.IO.IOException ex)
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
		[NUnit.Framework.Test]
		public virtual void testReaderGetClosest()
		{
			string TEST_METHOD_KEY = "testReaderWithWrongKeyClass.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = createWriter(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				for (int i = 0; i < 10; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(i), new org.apache.hadoop.io.Text
						("value" + i));
				}
				writer.close();
				reader = createReader(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
					)));
				reader.getClosest(new org.apache.hadoop.io.Text("2"), new org.apache.hadoop.io.Text
					(string.Empty));
				NUnit.Framework.Assert.Fail("no excepted exception in testReaderWithWrongKeyClass !!!"
					);
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				/* Should be thrown to pass the test */
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.append()</c>
		/// with wrong key class
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testReaderWithWrongValueClass()
		{
			string TEST_METHOD_KEY = "testReaderWithWrongValueClass.mapfile";
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				writer = createWriter(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				writer.append(new org.apache.hadoop.io.IntWritable(0), new org.apache.hadoop.io.IntWritable
					(0));
				NUnit.Framework.Assert.Fail("no excepted exception in testReaderWithWrongKeyClass !!!"
					);
			}
			catch (System.IO.IOException)
			{
			}
			finally
			{
				/* Should be thrown to pass the test */
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Reader.next(key, value)</c>
		/// for iteration.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testReaderKeyIteration()
		{
			string TEST_METHOD_KEY = "testReaderKeyIteration.mapfile";
			int SIZE = 10;
			int ITERATIONS = 5;
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = createWriter(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				int start = 0;
				for (int i = 0; i < SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(i), new org.apache.hadoop.io.Text
						("Value:" + i));
				}
				writer.close();
				reader = createReader(TEST_METHOD_KEY, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)));
				// test iteration
				org.apache.hadoop.io.Writable startValue = new org.apache.hadoop.io.Text("Value:"
					 + start);
				int i_1 = 0;
				while (i_1++ < ITERATIONS)
				{
					org.apache.hadoop.io.IntWritable key = new org.apache.hadoop.io.IntWritable(start
						);
					org.apache.hadoop.io.Writable value = startValue;
					while (reader.next(key, value))
					{
						NUnit.Framework.Assert.IsNotNull(key);
						NUnit.Framework.Assert.IsNotNull(value);
					}
					reader.reset();
				}
				NUnit.Framework.Assert.IsTrue("reader seek error !!!", reader.seek(new org.apache.hadoop.io.IntWritable
					(SIZE / 2)));
				NUnit.Framework.Assert.IsFalse("reader seek error !!!", reader.seek(new org.apache.hadoop.io.IntWritable
					(SIZE * 2)));
			}
			catch (System.IO.IOException)
			{
				NUnit.Framework.Assert.Fail("reader seek error !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.testFix</c>
		/// method
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testFix()
		{
			string INDEX_LESS_MAP_FILE = "testFix.mapfile";
			int PAIR_SIZE = 20;
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(TEST_DIR, INDEX_LESS_MAP_FILE
					);
				writer = createWriter(INDEX_LESS_MAP_FILE, Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
					)));
				for (int i = 0; i < PAIR_SIZE; i++)
				{
					writer.append(new org.apache.hadoop.io.IntWritable(0), new org.apache.hadoop.io.Text
						("value"));
				}
				writer.close();
				java.io.File indexFile = new java.io.File(".", "." + INDEX_LESS_MAP_FILE + "/index"
					);
				bool isDeleted = false;
				if (indexFile.exists())
				{
					isDeleted = indexFile.delete();
				}
				if (isDeleted)
				{
					NUnit.Framework.Assert.IsTrue("testFix error !!!", org.apache.hadoop.io.MapFile.fix
						(fs, dir, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
						)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)), true, conf
						) == PAIR_SIZE);
				}
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testFix error !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test all available constructor for
		/// <c>MapFile.Writer</c>
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testDeprecatedConstructors()
		{
			string path = new org.apache.hadoop.fs.Path(TEST_DIR, "writes.mapfile").ToString(
				);
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
					);
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType.RECORD
					, defaultCodec, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, org.apache.hadoop.io.WritableComparator
					.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)));
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, org.apache.hadoop.io.WritableComparator
					.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, path, org.apache.hadoop.io.WritableComparator
					.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)), org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD, defaultProgressable);
				NUnit.Framework.Assert.IsNotNull(writer);
				writer.close();
				reader = new org.apache.hadoop.io.MapFile.Reader(fs, path, org.apache.hadoop.io.WritableComparator
					.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable))), 
					conf);
				NUnit.Framework.Assert.IsNotNull(reader);
				NUnit.Framework.Assert.IsNotNull("reader key is null !!!", reader.getKeyClass());
				NUnit.Framework.Assert.IsNotNull("reader value in null", reader.getValueClass());
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer</c>
		/// constructor
		/// with IllegalArgumentException
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testKeyLessWriterCreation()
		{
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, TEST_DIR);
				NUnit.Framework.Assert.Fail("fail in testKeyLessWriterCreation !!!");
			}
			catch (System.ArgumentException)
			{
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("fail in testKeyLessWriterCreation. Other ex !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer</c>
		/// constructor with IOException
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testPathExplosionWriterCreation()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(TEST_DIR, "testPathExplosionWriterCreation.mapfile"
				);
			string TEST_ERROR_MESSAGE = "Mkdirs failed to create directory " + path.getName();
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				org.apache.hadoop.fs.FileSystem fsSpy = org.mockito.Mockito.spy(org.apache.hadoop.fs.FileSystem
					.get(conf));
				org.apache.hadoop.fs.Path pathSpy = org.mockito.Mockito.spy(path);
				org.mockito.Mockito.when(fsSpy.mkdirs(path)).thenThrow(new System.IO.IOException(
					TEST_ERROR_MESSAGE));
				org.mockito.Mockito.when(pathSpy.getFileSystem(conf)).thenReturn(fsSpy);
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, pathSpy, org.apache.hadoop.io.MapFile.Writer
					.keyClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					))), org.apache.hadoop.io.MapFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable))));
				NUnit.Framework.Assert.Fail("fail in testPathExplosionWriterCreation !!!");
			}
			catch (System.IO.IOException ex)
			{
				NUnit.Framework.Assert.AreEqual("testPathExplosionWriterCreation ex message error !!!"
					, ex.Message, TEST_ERROR_MESSAGE);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("fail in testPathExplosionWriterCreation. Other ex !!!"
					);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>
		/// test
		/// <c>MapFile.Writer.append</c>
		/// method with desc order
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testDescOrderWithThrowExceptionWriterAppend()
		{
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				writer = createWriter(".mapfile", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				writer.append(new org.apache.hadoop.io.IntWritable(2), new org.apache.hadoop.io.Text
					("value: " + 1));
				writer.append(new org.apache.hadoop.io.IntWritable(2), new org.apache.hadoop.io.Text
					("value: " + 2));
				writer.append(new org.apache.hadoop.io.IntWritable(2), new org.apache.hadoop.io.Text
					("value: " + 4));
				writer.append(new org.apache.hadoop.io.IntWritable(1), new org.apache.hadoop.io.Text
					("value: " + 3));
				NUnit.Framework.Assert.Fail("testDescOrderWithThrowExceptionWriterAppend not expected exception error !!!"
					);
			}
			catch (System.IO.IOException)
			{
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testDescOrderWithThrowExceptionWriterAppend other ex throw !!!"
					);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testMainMethodMapFile()
		{
			string inFile = "mainMethodMapFile.mapfile";
			string path = new org.apache.hadoop.fs.Path(TEST_DIR, inFile).ToString();
			string[] args = new string[] { path, path };
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				writer = createWriter(inFile, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)));
				writer.append(new org.apache.hadoop.io.IntWritable(1), new org.apache.hadoop.io.Text
					("test_text1"));
				writer.append(new org.apache.hadoop.io.IntWritable(2), new org.apache.hadoop.io.Text
					("test_text2"));
				writer.close();
				org.apache.hadoop.io.MapFile.Main(args);
			}
			catch (System.Exception)
			{
				NUnit.Framework.Assert.Fail("testMainMethodMapFile error !!!");
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer);
			}
		}

		/// <summary>Test getClosest feature.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetClosest()
		{
			// Write a mapfile of simple data: keys are
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, "testGetClosest.mapfile"
				);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path qualifiedDirName = fs.makeQualified(dirName);
			// Make an index entry for every third insertion.
			org.apache.hadoop.io.MapFile.Writer.setIndexInterval(conf, 3);
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, qualifiedDirName.ToString
					(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.Text)));
				// Assert that the index interval is 1
				NUnit.Framework.Assert.AreEqual(3, writer.getIndexInterval());
				// Add entries up to 100 in intervals of ten.
				int FIRST_KEY = 10;
				for (int i = FIRST_KEY; i < 100; i += 10)
				{
					string iStr = int.toString(i);
					org.apache.hadoop.io.Text t = new org.apache.hadoop.io.Text(Sharpen.Runtime.substring
						("00", iStr.Length) + iStr);
					writer.append(t, t);
				}
				writer.close();
				// Now do getClosest on created mapfile.
				reader = new org.apache.hadoop.io.MapFile.Reader(qualifiedDirName, conf);
				org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text("55");
				org.apache.hadoop.io.Text value = new org.apache.hadoop.io.Text();
				org.apache.hadoop.io.Text closest = (org.apache.hadoop.io.Text)reader.getClosest(
					key, value);
				// Assert that closest after 55 is 60
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("60"), closest);
				// Get closest that falls before the passed key: 50
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value, true);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("50"), closest);
				// Test get closest when we pass explicit key
				org.apache.hadoop.io.Text TWENTY = new org.apache.hadoop.io.Text("20");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(TWENTY, value);
				NUnit.Framework.Assert.AreEqual(TWENTY, closest);
				closest = (org.apache.hadoop.io.Text)reader.getClosest(TWENTY, value, true);
				NUnit.Framework.Assert.AreEqual(TWENTY, closest);
				// Test what happens at boundaries. Assert if searching a key that is
				// less than first key in the mapfile, that the first key is returned.
				key = new org.apache.hadoop.io.Text("00");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value);
				NUnit.Framework.Assert.AreEqual(FIRST_KEY, System.Convert.ToInt32(closest.ToString
					()));
				// If we're looking for the first key before, and we pass in a key before
				// the first key in the file, we should get null
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value, true);
				NUnit.Framework.Assert.IsNull(closest);
				// Assert that null is returned if key is > last entry in mapfile.
				key = new org.apache.hadoop.io.Text("99");
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value);
				NUnit.Framework.Assert.IsNull(closest);
				// If we were looking for the key before, we should get the last key
				closest = (org.apache.hadoop.io.Text)reader.getClosest(key, value, true);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.Text("90"), closest);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMidKey()
		{
			// Write a mapfile of simple data: keys are
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, "testMidKey.mapfile"
				);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path qualifiedDirName = fs.makeQualified(dirName);
			org.apache.hadoop.io.MapFile.Writer writer = null;
			org.apache.hadoop.io.MapFile.Reader reader = null;
			try
			{
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, qualifiedDirName.ToString
					(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.IntWritable)));
				writer.append(new org.apache.hadoop.io.IntWritable(1), new org.apache.hadoop.io.IntWritable
					(1));
				writer.close();
				// Now do getClosest on created mapfile.
				reader = new org.apache.hadoop.io.MapFile.Reader(qualifiedDirName, conf);
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.IntWritable(1), reader.midKey
					());
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(null, writer, reader);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMidKeyEmpty()
		{
			// Write a mapfile of simple data: keys are
			org.apache.hadoop.fs.Path dirName = new org.apache.hadoop.fs.Path(TEST_DIR, "testMidKeyEmpty.mapfile"
				);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path qualifiedDirName = fs.makeQualified(dirName);
			org.apache.hadoop.io.MapFile.Writer writer = new org.apache.hadoop.io.MapFile.Writer
				(conf, fs, qualifiedDirName.ToString(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)));
			writer.close();
			// Now do getClosest on created mapfile.
			org.apache.hadoop.io.MapFile.Reader reader = new org.apache.hadoop.io.MapFile.Reader
				(qualifiedDirName, conf);
			try
			{
				NUnit.Framework.Assert.AreEqual(null, reader.midKey());
			}
			finally
			{
				reader.close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMerge()
		{
			string TEST_METHOD_KEY = "testMerge.mapfile";
			int SIZE = 10;
			int ITERATIONS = 5;
			org.apache.hadoop.fs.Path[] @in = new org.apache.hadoop.fs.Path[5];
			System.Collections.Generic.IList<int> expected = new System.Collections.Generic.List
				<int>();
			for (int j = 0; j < 5; j++)
			{
				using (org.apache.hadoop.io.MapFile.Writer writer = createWriter(TEST_METHOD_KEY 
					+ "." + j, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text))))
				{
					@in[j] = new org.apache.hadoop.fs.Path(TEST_DIR, TEST_METHOD_KEY + "." + j);
					for (int i = 0; i < SIZE; i++)
					{
						expected.add(i + j);
						writer.append(new org.apache.hadoop.io.IntWritable(i + j), new org.apache.hadoop.io.Text
							("Value:" + (i + j)));
					}
				}
			}
			// Sort expected values
			expected.Sort();
			// Merge all 5 files
			org.apache.hadoop.io.MapFile.Merger merger = new org.apache.hadoop.io.MapFile.Merger
				(conf);
			merger.merge(@in, true, new org.apache.hadoop.fs.Path(TEST_DIR, TEST_METHOD_KEY));
			using (org.apache.hadoop.io.MapFile.Reader reader = createReader(TEST_METHOD_KEY, 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable))))
			{
				int start = 0;
				// test iteration
				org.apache.hadoop.io.Text startValue = new org.apache.hadoop.io.Text("Value:" + start
					);
				int i = 0;
				while (i++ < ITERATIONS)
				{
					System.Collections.Generic.IEnumerator<int> expectedIterator = expected.GetEnumerator
						();
					org.apache.hadoop.io.IntWritable key = new org.apache.hadoop.io.IntWritable(start
						);
					org.apache.hadoop.io.Text value = startValue;
					org.apache.hadoop.io.IntWritable prev = new org.apache.hadoop.io.IntWritable(start
						);
					while (reader.next(key, value))
					{
						NUnit.Framework.Assert.IsTrue("Next key should be always equal or more", prev.get
							() <= key.get());
						NUnit.Framework.Assert.AreEqual(expectedIterator.Current, key.get());
						prev.set(key.get());
					}
					reader.reset();
				}
			}
			// inputs should be deleted
			for (int j_1 = 0; j_1 < @in.Length; j_1++)
			{
				org.apache.hadoop.fs.Path path = @in[j_1];
				NUnit.Framework.Assert.IsFalse("inputs should be deleted", path.getFileSystem(conf
					).exists(path));
			}
		}
	}
}
