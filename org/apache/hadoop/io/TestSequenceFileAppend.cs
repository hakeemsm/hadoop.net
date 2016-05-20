using Sharpen;

namespace org.apache.hadoop.io
{
	public class TestSequenceFileAppend
	{
		private static org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.fs.FileSystem fs;

		private static org.apache.hadoop.fs.Path ROOT_PATH = new org.apache.hadoop.fs.Path
			(Sharpen.Runtime.getProperty("test.build.data", "build/test/data"));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
			fs = org.apache.hadoop.fs.FileSystem.get(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.AfterClass]
		public static void tearDown()
		{
			fs.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAppend()
		{
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(ROOT_PATH, "testseqappend.seq"
				);
			fs.delete(file, true);
			org.apache.hadoop.io.Text key1 = new org.apache.hadoop.io.Text("Key1");
			org.apache.hadoop.io.Text value1 = new org.apache.hadoop.io.Text("Value1");
			org.apache.hadoop.io.Text value2 = new org.apache.hadoop.io.Text("Updated");
			org.apache.hadoop.io.SequenceFile.Metadata metadata = new org.apache.hadoop.io.SequenceFile.Metadata
				();
			metadata.set(key1, value1);
			org.apache.hadoop.io.SequenceFile.Writer.Option metadataOption = org.apache.hadoop.io.SequenceFile.Writer
				.metadata(metadata);
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(file), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(Sharpen.Runtime.getClassForType(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(Sharpen.Runtime.getClassForType(typeof(string))), metadataOption);
			writer.append(1L, "one");
			writer.append(2L, "two");
			writer.close();
			verify2Values(file);
			metadata.set(key1, value2);
			writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
				.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
				(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
				(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
				, metadataOption);
			// Verify the Meta data is not changed
			NUnit.Framework.Assert.AreEqual(value1, writer.metadata.get(key1));
			writer.append(3L, "three");
			writer.append(4L, "four");
			writer.close();
			verifyAll4Values(file);
			// Verify the Meta data readable after append
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(conf, org.apache.hadoop.io.SequenceFile.Reader.file(file));
			NUnit.Framework.Assert.AreEqual(value1, reader.getMetadata().get(key1));
			reader.close();
			// Verify failure if the compression details are different
			try
			{
				org.apache.hadoop.io.SequenceFile.Writer.Option wrongCompressOption = org.apache.hadoop.io.SequenceFile.Writer
					.compression(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, new org.apache.hadoop.io.compress.GzipCodec
					());
				writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
					, wrongCompressOption);
				writer.close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (System.ArgumentException)
			{
			}
			// Expected exception. Ignore it
			try
			{
				org.apache.hadoop.io.SequenceFile.Writer.Option wrongCompressOption = org.apache.hadoop.io.SequenceFile.Writer
					.compression(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.DefaultCodec
					());
				writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
					, wrongCompressOption);
				writer.close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (System.ArgumentException)
			{
			}
			// Expected exception. Ignore it
			fs.deleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAppendRecordCompression()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(ROOT_PATH, "testseqappendblockcompr.seq"
				);
			fs.delete(file, true);
			org.apache.hadoop.io.SequenceFile.Writer.Option compressOption = org.apache.hadoop.io.SequenceFile.Writer
				.compression(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, new org.apache.hadoop.io.compress.GzipCodec
				());
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(file), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(Sharpen.Runtime.getClassForType(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(Sharpen.Runtime.getClassForType(typeof(string))), compressOption);
			writer.append(1L, "one");
			writer.append(2L, "two");
			writer.close();
			verify2Values(file);
			writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
				.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
				(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
				(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
				, compressOption);
			writer.append(3L, "three");
			writer.append(4L, "four");
			writer.close();
			verifyAll4Values(file);
			fs.deleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAppendBlockCompression()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(ROOT_PATH, "testseqappendblockcompr.seq"
				);
			fs.delete(file, true);
			org.apache.hadoop.io.SequenceFile.Writer.Option compressOption = org.apache.hadoop.io.SequenceFile.Writer
				.compression(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.GzipCodec
				());
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(file), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(Sharpen.Runtime.getClassForType(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(Sharpen.Runtime.getClassForType(typeof(string))), compressOption);
			writer.append(1L, "one");
			writer.append(2L, "two");
			writer.close();
			verify2Values(file);
			writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
				.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
				(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
				(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
				, compressOption);
			writer.append(3L, "three");
			writer.append(4L, "four");
			writer.close();
			verifyAll4Values(file);
			// Verify failure if the compression details are different or not Provided
			try
			{
				writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
					);
				writer.close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (System.ArgumentException)
			{
			}
			// Expected exception. Ignore it
			// Verify failure if the compression details are different
			try
			{
				org.apache.hadoop.io.SequenceFile.Writer.Option wrongCompressOption = org.apache.hadoop.io.SequenceFile.Writer
					.compression(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, new org.apache.hadoop.io.compress.GzipCodec
					());
				writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
					, wrongCompressOption);
				writer.close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (System.ArgumentException)
			{
			}
			// Expected exception. Ignore it
			try
			{
				org.apache.hadoop.io.SequenceFile.Writer.Option wrongCompressOption = org.apache.hadoop.io.SequenceFile.Writer
					.compression(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.DefaultCodec
					());
				writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
					, wrongCompressOption);
				writer.close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (System.ArgumentException)
			{
			}
			// Expected exception. Ignore it
			fs.deleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testAppendSort()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(ROOT_PATH, "testseqappendSort.seq"
				);
			fs.delete(file, true);
			org.apache.hadoop.fs.Path sortedFile = new org.apache.hadoop.fs.Path(ROOT_PATH, "testseqappendSort.seq.sort"
				);
			fs.delete(sortedFile, true);
			org.apache.hadoop.io.SequenceFile.Sorter sorter = new org.apache.hadoop.io.SequenceFile.Sorter
				(fs, new org.apache.hadoop.io.serializer.JavaSerializationComparator<long>(), Sharpen.Runtime.getClassForType
				(typeof(long)), Sharpen.Runtime.getClassForType(typeof(string)), conf);
			org.apache.hadoop.io.SequenceFile.Writer.Option compressOption = org.apache.hadoop.io.SequenceFile.Writer
				.compression(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.GzipCodec
				());
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(file), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(Sharpen.Runtime.getClassForType(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(Sharpen.Runtime.getClassForType(typeof(string))), compressOption);
			writer.append(2L, "two");
			writer.append(1L, "one");
			writer.close();
			writer = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
				.file(file), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
				(typeof(long))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
				(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.appendIfExists(true)
				, compressOption);
			writer.append(4L, "four");
			writer.append(3L, "three");
			writer.close();
			// Sort file after append
			sorter.sort(file, sortedFile);
			verifyAll4Values(sortedFile);
			fs.deleteOnExit(file);
			fs.deleteOnExit(sortedFile);
		}

		/// <exception cref="System.IO.IOException"/>
		private void verify2Values(org.apache.hadoop.fs.Path file)
		{
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(conf, org.apache.hadoop.io.SequenceFile.Reader.file(file));
			NUnit.Framework.Assert.AreEqual(1L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("one", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.AreEqual(2L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("two", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.next((object)null));
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyAll4Values(org.apache.hadoop.fs.Path file)
		{
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(conf, org.apache.hadoop.io.SequenceFile.Reader.file(file));
			NUnit.Framework.Assert.AreEqual(1L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("one", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.AreEqual(2L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("two", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.AreEqual(3L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("three", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.AreEqual(4L, reader.next((object)null));
			NUnit.Framework.Assert.AreEqual("four", reader.getCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.next((object)null));
			reader.close();
		}
	}
}
