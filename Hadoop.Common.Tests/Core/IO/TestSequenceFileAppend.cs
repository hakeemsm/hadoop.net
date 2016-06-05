using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Test;


namespace Org.Apache.Hadoop.IO
{
	public class TestSequenceFileAppend
	{
		private static Configuration conf;

		private static FileSystem fs;

		private static Path RootPath = new Path(Runtime.GetProperty("test.build.data", "build/test/data"
			));

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			conf.Set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization"
				);
			conf.Set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
			fs = FileSystem.Get(conf);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			fs.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppend()
		{
			Path file = new Path(RootPath, "testseqappend.seq");
			fs.Delete(file, true);
			Text key1 = new Text("Key1");
			Text value1 = new Text("Value1");
			Text value2 = new Text("Updated");
			SequenceFile.Metadata metadata = new SequenceFile.Metadata();
			metadata.Set(key1, value1);
			SequenceFile.Writer.Option metadataOption = SequenceFile.Writer.Metadata(metadata
				);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.
				File(file), SequenceFile.Writer.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass
				(typeof(string)), metadataOption);
			writer.Append(1L, "one");
			writer.Append(2L, "two");
			writer.Close();
			Verify2Values(file);
			metadata.Set(key1, value2);
			writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
				.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
				.AppendIfExists(true), metadataOption);
			// Verify the Meta data is not changed
			Assert.Equal(value1, writer.metadata.Get(key1));
			writer.Append(3L, "three");
			writer.Append(4L, "four");
			writer.Close();
			VerifyAll4Values(file);
			// Verify the Meta data readable after append
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.File
				(file));
			Assert.Equal(value1, reader.GetMetadata().Get(key1));
			reader.Close();
			// Verify failure if the compression details are different
			try
			{
				SequenceFile.Writer.Option wrongCompressOption = SequenceFile.Writer.Compression(
					SequenceFile.CompressionType.Record, new GzipCodec());
				writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
					.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
					.AppendIfExists(true), wrongCompressOption);
				writer.Close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (ArgumentException)
			{
			}
			// Expected exception. Ignore it
			try
			{
				SequenceFile.Writer.Option wrongCompressOption = SequenceFile.Writer.Compression(
					SequenceFile.CompressionType.Block, new DefaultCodec());
				writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
					.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
					.AppendIfExists(true), wrongCompressOption);
				writer.Close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (ArgumentException)
			{
			}
			// Expected exception. Ignore it
			fs.DeleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppendRecordCompression()
		{
			GenericTestUtils.AssumeInNativeProfile();
			Path file = new Path(RootPath, "testseqappendblockcompr.seq");
			fs.Delete(file, true);
			SequenceFile.Writer.Option compressOption = SequenceFile.Writer.Compression(SequenceFile.CompressionType
				.Record, new GzipCodec());
			SequenceFile.Writer writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.
				File(file), SequenceFile.Writer.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass
				(typeof(string)), compressOption);
			writer.Append(1L, "one");
			writer.Append(2L, "two");
			writer.Close();
			Verify2Values(file);
			writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
				.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
				.AppendIfExists(true), compressOption);
			writer.Append(3L, "three");
			writer.Append(4L, "four");
			writer.Close();
			VerifyAll4Values(file);
			fs.DeleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppendBlockCompression()
		{
			GenericTestUtils.AssumeInNativeProfile();
			Path file = new Path(RootPath, "testseqappendblockcompr.seq");
			fs.Delete(file, true);
			SequenceFile.Writer.Option compressOption = SequenceFile.Writer.Compression(SequenceFile.CompressionType
				.Block, new GzipCodec());
			SequenceFile.Writer writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.
				File(file), SequenceFile.Writer.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass
				(typeof(string)), compressOption);
			writer.Append(1L, "one");
			writer.Append(2L, "two");
			writer.Close();
			Verify2Values(file);
			writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
				.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
				.AppendIfExists(true), compressOption);
			writer.Append(3L, "three");
			writer.Append(4L, "four");
			writer.Close();
			VerifyAll4Values(file);
			// Verify failure if the compression details are different or not Provided
			try
			{
				writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
					.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
					.AppendIfExists(true));
				writer.Close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (ArgumentException)
			{
			}
			// Expected exception. Ignore it
			// Verify failure if the compression details are different
			try
			{
				SequenceFile.Writer.Option wrongCompressOption = SequenceFile.Writer.Compression(
					SequenceFile.CompressionType.Record, new GzipCodec());
				writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
					.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
					.AppendIfExists(true), wrongCompressOption);
				writer.Close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (ArgumentException)
			{
			}
			// Expected exception. Ignore it
			try
			{
				SequenceFile.Writer.Option wrongCompressOption = SequenceFile.Writer.Compression(
					SequenceFile.CompressionType.Block, new DefaultCodec());
				writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
					.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
					.AppendIfExists(true), wrongCompressOption);
				writer.Close();
				NUnit.Framework.Assert.Fail("Expected IllegalArgumentException for compression options"
					);
			}
			catch (ArgumentException)
			{
			}
			// Expected exception. Ignore it
			fs.DeleteOnExit(file);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppendSort()
		{
			GenericTestUtils.AssumeInNativeProfile();
			Path file = new Path(RootPath, "testseqappendSort.seq");
			fs.Delete(file, true);
			Path sortedFile = new Path(RootPath, "testseqappendSort.seq.sort");
			fs.Delete(sortedFile, true);
			SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, new JavaSerializationComparator
				<long>(), typeof(long), typeof(string), conf);
			SequenceFile.Writer.Option compressOption = SequenceFile.Writer.Compression(SequenceFile.CompressionType
				.Block, new GzipCodec());
			SequenceFile.Writer writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.
				File(file), SequenceFile.Writer.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass
				(typeof(string)), compressOption);
			writer.Append(2L, "two");
			writer.Append(1L, "one");
			writer.Close();
			writer = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(file), SequenceFile.Writer
				.KeyClass(typeof(long)), SequenceFile.Writer.ValueClass(typeof(string)), SequenceFile.Writer
				.AppendIfExists(true), compressOption);
			writer.Append(4L, "four");
			writer.Append(3L, "three");
			writer.Close();
			// Sort file after append
			sorter.Sort(file, sortedFile);
			VerifyAll4Values(sortedFile);
			fs.DeleteOnExit(file);
			fs.DeleteOnExit(sortedFile);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Verify2Values(Path file)
		{
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.File
				(file));
			Assert.Equal(1L, reader.Next((object)null));
			Assert.Equal("one", reader.GetCurrentValue((object)null));
			Assert.Equal(2L, reader.Next((object)null));
			Assert.Equal("two", reader.GetCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.Next((object)null));
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyAll4Values(Path file)
		{
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.File
				(file));
			Assert.Equal(1L, reader.Next((object)null));
			Assert.Equal("one", reader.GetCurrentValue((object)null));
			Assert.Equal(2L, reader.Next((object)null));
			Assert.Equal("two", reader.GetCurrentValue((object)null));
			Assert.Equal(3L, reader.Next((object)null));
			Assert.Equal("three", reader.GetCurrentValue((object)null));
			Assert.Equal(4L, reader.Next((object)null));
			Assert.Equal("four", reader.GetCurrentValue((object)null));
			NUnit.Framework.Assert.IsNull(reader.Next((object)null));
			reader.Close();
		}
	}
}
