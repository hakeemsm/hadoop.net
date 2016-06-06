using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Serializer.Avro;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestSequenceFile : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.TestSequenceFile
			));

		private Configuration conf = new Configuration();

		public TestSequenceFile()
		{
		}

		public TestSequenceFile(string name)
			: base(name)
		{
		}

		/// <summary>Unit tests for SequenceFile.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestZlibSequenceFile()
		{
			Log.Info("Testing SequenceFile with DefaultCodec");
			CompressedSeqFileTest(new DefaultCodec());
			Log.Info("Successfully tested SequenceFile with DefaultCodec");
		}

		/// <exception cref="System.Exception"/>
		public virtual void CompressedSeqFileTest(CompressionCodec codec)
		{
			int count = 1024 * 10;
			int megabytes = 1;
			int factor = 5;
			Path file = new Path(Runtime.GetProperty("test.build.data", ".") + "/test.seq");
			Path recordCompressedFile = new Path(Runtime.GetProperty("test.build.data", ".") 
				+ "/test.rc.seq");
			Path blockCompressedFile = new Path(Runtime.GetProperty("test.build.data", ".") +
				 "/test.bc.seq");
			int seed = new Random().Next();
			Log.Info("Seed = " + seed);
			FileSystem fs = FileSystem.GetLocal(conf);
			try
			{
				// SequenceFile.Writer
				WriteTest(fs, count, seed, file, SequenceFile.CompressionType.None, null);
				ReadTest(fs, count, seed, file);
				SortTest(fs, count, megabytes, factor, false, file);
				CheckSort(fs, count, seed, file);
				SortTest(fs, count, megabytes, factor, true, file);
				CheckSort(fs, count, seed, file);
				MergeTest(fs, count, seed, file, SequenceFile.CompressionType.None, false, factor
					, megabytes);
				CheckSort(fs, count, seed, file);
				MergeTest(fs, count, seed, file, SequenceFile.CompressionType.None, true, factor, 
					megabytes);
				CheckSort(fs, count, seed, file);
				// SequenceFile.RecordCompressWriter
				WriteTest(fs, count, seed, recordCompressedFile, SequenceFile.CompressionType.Record
					, codec);
				ReadTest(fs, count, seed, recordCompressedFile);
				SortTest(fs, count, megabytes, factor, false, recordCompressedFile);
				CheckSort(fs, count, seed, recordCompressedFile);
				SortTest(fs, count, megabytes, factor, true, recordCompressedFile);
				CheckSort(fs, count, seed, recordCompressedFile);
				MergeTest(fs, count, seed, recordCompressedFile, SequenceFile.CompressionType.Record
					, false, factor, megabytes);
				CheckSort(fs, count, seed, recordCompressedFile);
				MergeTest(fs, count, seed, recordCompressedFile, SequenceFile.CompressionType.Record
					, true, factor, megabytes);
				CheckSort(fs, count, seed, recordCompressedFile);
				// SequenceFile.BlockCompressWriter
				WriteTest(fs, count, seed, blockCompressedFile, SequenceFile.CompressionType.Block
					, codec);
				ReadTest(fs, count, seed, blockCompressedFile);
				SortTest(fs, count, megabytes, factor, false, blockCompressedFile);
				CheckSort(fs, count, seed, blockCompressedFile);
				SortTest(fs, count, megabytes, factor, true, blockCompressedFile);
				CheckSort(fs, count, seed, blockCompressedFile);
				MergeTest(fs, count, seed, blockCompressedFile, SequenceFile.CompressionType.Block
					, false, factor, megabytes);
				CheckSort(fs, count, seed, blockCompressedFile);
				MergeTest(fs, count, seed, blockCompressedFile, SequenceFile.CompressionType.Block
					, true, factor, megabytes);
				CheckSort(fs, count, seed, blockCompressedFile);
			}
			finally
			{
				fs.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteTest(FileSystem fs, int count, int seed, Path file, SequenceFile.CompressionType
			 compressionType, CompressionCodec codec)
		{
			fs.Delete(file, true);
			Log.Info("creating " + count + " records with " + compressionType + " compression"
				);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(RandomDatum
				), typeof(RandomDatum), compressionType, codec);
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				writer.Append(key, value);
			}
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadTest(FileSystem fs, int count, int seed, Path file)
		{
			Log.Debug("reading " + count + " records");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			RandomDatum k = new RandomDatum();
			RandomDatum v = new RandomDatum();
			DataOutputBuffer rawKey = new DataOutputBuffer();
			SequenceFile.ValueBytes rawValue = reader.CreateValueBytes();
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				try
				{
					if ((i % 5) == 0)
					{
						// Testing 'raw' apis
						rawKey.Reset();
						reader.NextRaw(rawKey, rawValue);
					}
					else
					{
						// Testing 'non-raw' apis 
						if ((i % 2) == 0)
						{
							reader.Next(k);
							reader.GetCurrentValue(v);
						}
						else
						{
							reader.Next(k, v);
						}
						// Check
						if (!k.Equals(key))
						{
							throw new RuntimeException("wrong key at " + i);
						}
						if (!v.Equals(value))
						{
							throw new RuntimeException("wrong value at " + i);
						}
					}
				}
				catch (IOException ioe)
				{
					Log.Info("Problem on row " + i);
					Log.Info("Expected key = " + key);
					Log.Info("Expected len = " + key.GetLength());
					Log.Info("Actual key = " + k);
					Log.Info("Actual len = " + k.GetLength());
					Log.Info("Expected value = " + value);
					Log.Info("Expected len = " + value.GetLength());
					Log.Info("Actual value = " + v);
					Log.Info("Actual len = " + v.GetLength());
					Log.Info("Key equals: " + k.Equals(key));
					Log.Info("value equals: " + v.Equals(value));
					throw;
				}
			}
			reader.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void SortTest(FileSystem fs, int count, int megabytes, int factor, bool fast
			, Path file)
		{
			fs.Delete(new Path(file + ".sorted"), true);
			SequenceFile.Sorter sorter = NewSorter(fs, fast, megabytes, factor);
			Log.Debug("sorting " + count + " records");
			sorter.Sort(file, file.Suffix(".sorted"));
			Log.Info("done sorting " + count + " debug");
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckSort(FileSystem fs, int count, int seed, Path file)
		{
			Log.Info("sorting " + count + " records in memory for debug");
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			SortedDictionary<RandomDatum, RandomDatum> map = new SortedDictionary<RandomDatum
				, RandomDatum>();
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				map[key] = value;
			}
			Log.Debug("checking order of " + count + " records");
			RandomDatum k = new RandomDatum();
			RandomDatum v = new RandomDatum();
			IEnumerator<KeyValuePair<RandomDatum, RandomDatum>> iterator = map.GetEnumerator(
				);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.Suffix(".sorted"), 
				conf);
			for (int i_1 = 0; i_1 < count; i_1++)
			{
				KeyValuePair<RandomDatum, RandomDatum> entry = iterator.Next();
				RandomDatum key = entry.Key;
				RandomDatum value = entry.Value;
				reader.Next(k, v);
				if (!k.Equals(key))
				{
					throw new RuntimeException("wrong key at " + i_1);
				}
				if (!v.Equals(value))
				{
					throw new RuntimeException("wrong value at " + i_1);
				}
			}
			reader.Close();
			Log.Debug("sucessfully checked " + count + " records");
		}

		/// <exception cref="System.IO.IOException"/>
		private void MergeTest(FileSystem fs, int count, int seed, Path file, SequenceFile.CompressionType
			 compressionType, bool fast, int factor, int megabytes)
		{
			Log.Debug("creating " + factor + " files with " + count / factor + " records");
			SequenceFile.Writer[] writers = new SequenceFile.Writer[factor];
			Path[] names = new Path[factor];
			Path[] sortedNames = new Path[factor];
			for (int i = 0; i < factor; i++)
			{
				names[i] = file.Suffix("." + i);
				sortedNames[i] = names[i].Suffix(".sorted");
				fs.Delete(names[i], true);
				fs.Delete(sortedNames[i], true);
				writers[i] = SequenceFile.CreateWriter(fs, conf, names[i], typeof(RandomDatum), typeof(
					RandomDatum), compressionType);
			}
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i_1 = 0; i_1 < count; i_1++)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				writers[i_1 % factor].Append(key, value);
			}
			for (int i_2 = 0; i_2 < factor; i_2++)
			{
				writers[i_2].Close();
			}
			for (int i_3 = 0; i_3 < factor; i_3++)
			{
				Log.Debug("sorting file " + i_3 + " with " + count / factor + " records");
				NewSorter(fs, fast, megabytes, factor).Sort(names[i_3], sortedNames[i_3]);
			}
			Log.Info("merging " + factor + " files with " + count / factor + " debug");
			fs.Delete(new Path(file + ".sorted"), true);
			NewSorter(fs, fast, megabytes, factor).Merge(sortedNames, file.Suffix(".sorted"));
		}

		private SequenceFile.Sorter NewSorter(FileSystem fs, bool fast, int megabytes, int
			 factor)
		{
			SequenceFile.Sorter sorter = fast ? new SequenceFile.Sorter(fs, new RandomDatum.Comparator
				(), typeof(RandomDatum), typeof(RandomDatum), conf) : new SequenceFile.Sorter(fs
				, typeof(RandomDatum), typeof(RandomDatum), conf);
			sorter.SetMemory(megabytes * 1024 * 1024);
			sorter.SetFactor(factor);
			return sorter;
		}

		/// <summary>Unit tests for SequenceFile metadata.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSequenceFileMetadata()
		{
			Log.Info("Testing SequenceFile with metadata");
			int count = 1024 * 10;
			CompressionCodec codec = new DefaultCodec();
			Path file = new Path(Runtime.GetProperty("test.build.data", ".") + "/test.seq.metadata"
				);
			Path sortedFile = new Path(Runtime.GetProperty("test.build.data", ".") + "/test.sorted.seq.metadata"
				);
			Path recordCompressedFile = new Path(Runtime.GetProperty("test.build.data", ".") 
				+ "/test.rc.seq.metadata");
			Path blockCompressedFile = new Path(Runtime.GetProperty("test.build.data", ".") +
				 "/test.bc.seq.metadata");
			FileSystem fs = FileSystem.GetLocal(conf);
			SequenceFile.Metadata theMetadata = new SequenceFile.Metadata();
			theMetadata.Set(new Text("name_1"), new Text("value_1"));
			theMetadata.Set(new Text("name_2"), new Text("value_2"));
			theMetadata.Set(new Text("name_3"), new Text("value_3"));
			theMetadata.Set(new Text("name_4"), new Text("value_4"));
			int seed = new Random().Next();
			try
			{
				// SequenceFile.Writer
				WriteMetadataTest(fs, count, seed, file, SequenceFile.CompressionType.None, null, 
					theMetadata);
				SequenceFile.Metadata aMetadata = ReadMetadata(fs, file);
				if (!theMetadata.Equals(aMetadata))
				{
					Log.Info("The original metadata:\n" + theMetadata.ToString());
					Log.Info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new RuntimeException("metadata not match:  " + 1);
				}
				// SequenceFile.RecordCompressWriter
				WriteMetadataTest(fs, count, seed, recordCompressedFile, SequenceFile.CompressionType
					.Record, codec, theMetadata);
				aMetadata = ReadMetadata(fs, recordCompressedFile);
				if (!theMetadata.Equals(aMetadata))
				{
					Log.Info("The original metadata:\n" + theMetadata.ToString());
					Log.Info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new RuntimeException("metadata not match:  " + 2);
				}
				// SequenceFile.BlockCompressWriter
				WriteMetadataTest(fs, count, seed, blockCompressedFile, SequenceFile.CompressionType
					.Block, codec, theMetadata);
				aMetadata = ReadMetadata(fs, blockCompressedFile);
				if (!theMetadata.Equals(aMetadata))
				{
					Log.Info("The original metadata:\n" + theMetadata.ToString());
					Log.Info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new RuntimeException("metadata not match:  " + 3);
				}
				// SequenceFile.Sorter
				SortMetadataTest(fs, file, sortedFile, theMetadata);
				aMetadata = ReadMetadata(fs, recordCompressedFile);
				if (!theMetadata.Equals(aMetadata))
				{
					Log.Info("The original metadata:\n" + theMetadata.ToString());
					Log.Info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new RuntimeException("metadata not match:  " + 4);
				}
			}
			finally
			{
				fs.Close();
			}
			Log.Info("Successfully tested SequenceFile with metadata");
		}

		/// <exception cref="System.IO.IOException"/>
		private SequenceFile.Metadata ReadMetadata(FileSystem fs, Path file)
		{
			Log.Info("reading file: " + file.ToString());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			SequenceFile.Metadata meta = reader.GetMetadata();
			reader.Close();
			return meta;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteMetadataTest(FileSystem fs, int count, int seed, Path file, SequenceFile.CompressionType
			 compressionType, CompressionCodec codec, SequenceFile.Metadata metadata)
		{
			fs.Delete(file, true);
			Log.Info("creating " + count + " records with metadata and with " + compressionType
				 + " compression");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, file, typeof(RandomDatum
				), typeof(RandomDatum), compressionType, codec, null, metadata);
			RandomDatum.Generator generator = new RandomDatum.Generator(seed);
			for (int i = 0; i < count; i++)
			{
				generator.Next();
				RandomDatum key = generator.GetKey();
				RandomDatum value = generator.GetValue();
				writer.Append(key, value);
			}
			writer.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void SortMetadataTest(FileSystem fs, Path unsortedFile, Path sortedFile, 
			SequenceFile.Metadata metadata)
		{
			fs.Delete(sortedFile, true);
			Log.Info("sorting: " + unsortedFile + " to: " + sortedFile);
			WritableComparator comparator = WritableComparator.Get(typeof(RandomDatum));
			SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, comparator, typeof(RandomDatum
				), typeof(RandomDatum), conf, metadata);
			sorter.Sort(new Path[] { unsortedFile }, sortedFile, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestClose()
		{
			Configuration conf = new Configuration();
			LocalFileSystem fs = FileSystem.GetLocal(conf);
			// create a sequence file 1
			Path path1 = new Path(Runtime.GetProperty("test.build.data", ".") + "/test1.seq");
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, path1, typeof(Text
				), typeof(NullWritable), SequenceFile.CompressionType.Block);
			writer.Append(new Text("file1-1"), NullWritable.Get());
			writer.Append(new Text("file1-2"), NullWritable.Get());
			writer.Close();
			Path path2 = new Path(Runtime.GetProperty("test.build.data", ".") + "/test2.seq");
			writer = SequenceFile.CreateWriter(fs, conf, path2, typeof(Text), typeof(NullWritable
				), SequenceFile.CompressionType.Block);
			writer.Append(new Text("file2-1"), NullWritable.Get());
			writer.Append(new Text("file2-2"), NullWritable.Get());
			writer.Close();
			// Create a reader which uses 4 BuiltInZLibInflater instances
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path1, conf);
			// Returns the 4 BuiltInZLibInflater instances to the CodecPool
			reader.Close();
			// The second close _could_ erroneously returns the same 
			// 4 BuiltInZLibInflater instances to the CodecPool again
			reader.Close();
			// The first reader gets 4 BuiltInZLibInflater instances from the CodecPool
			SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, path1, conf);
			// read first value from reader1
			Text text = new Text();
			reader1.Next(text);
			Assert.Equal("file1-1", text.ToString());
			// The second reader _could_ get the same 4 BuiltInZLibInflater 
			// instances from the CodePool as reader1
			SequenceFile.Reader reader2 = new SequenceFile.Reader(fs, path2, conf);
			// read first value from reader2
			reader2.Next(text);
			Assert.Equal("file2-1", text.ToString());
			// read second value from reader1
			reader1.Next(text);
			Assert.Equal("file1-2", text.ToString());
			// read second value from reader2 (this throws an exception)
			reader2.Next(text);
			Assert.Equal("file2-2", text.ToString());
			NUnit.Framework.Assert.IsFalse(reader1.Next(text));
			NUnit.Framework.Assert.IsFalse(reader2.Next(text));
		}

		/// <summary>Test that makes sure the FileSystem passed to createWriter</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCreateUsesFsArg()
		{
			FileSystem fs = FileSystem.GetLocal(conf);
			FileSystem spyFs = Org.Mockito.Mockito.Spy(fs);
			Path p = new Path(Runtime.GetProperty("test.build.data", ".") + "/testCreateUsesFSArg.seq"
				);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(spyFs, conf, p, typeof(NullWritable
				), typeof(NullWritable));
			writer.Close();
			Org.Mockito.Mockito.Verify(spyFs).GetDefaultReplication(p);
		}

		private class TestFSDataInputStream : FSDataInputStream
		{
			private bool closed = false;

			/// <exception cref="System.IO.IOException"/>
			private TestFSDataInputStream(InputStream @in)
				: base(@in)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				closed = true;
				base.Close();
			}

			public virtual bool IsClosed()
			{
				return closed;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCloseForErroneousSequenceFile()
		{
			Configuration conf = new Configuration();
			LocalFileSystem fs = FileSystem.GetLocal(conf);
			// create an empty file (which is not a valid sequence file)
			Path path = new Path(Runtime.GetProperty("test.build.data", ".") + "/broken.seq");
			fs.Create(path).Close();
			// try to create SequenceFile.Reader
			TestSequenceFile.TestFSDataInputStream[] openedFile = new TestSequenceFile.TestFSDataInputStream
				[1];
			try
			{
				new _Reader_509(openedFile, fs, path, conf);
				// this method is called by the SequenceFile.Reader constructor, overwritten, so we can access the opened file
				Fail("IOException expected.");
			}
			catch (IOException)
			{
			}
			NUnit.Framework.Assert.IsNotNull(path + " should have been opened.", openedFile[0
				]);
			Assert.True("InputStream for " + path + " should have been closed."
				, openedFile[0].IsClosed());
		}

		private sealed class _Reader_509 : SequenceFile.Reader
		{
			public _Reader_509(TestSequenceFile.TestFSDataInputStream[] openedFile, FileSystem
				 baseArg1, Path baseArg2, Configuration baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.openedFile = openedFile;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override FSDataInputStream OpenFile(FileSystem fs, Path file, 
				int bufferSize, long length)
			{
				InputStream @in = base.OpenFile(fs, file, bufferSize, length);
				openedFile[0] = new TestSequenceFile.TestFSDataInputStream(@in);
				return openedFile[0];
			}

			private readonly TestSequenceFile.TestFSDataInputStream[] openedFile;
		}

		/// <summary>
		/// Test that makes sure createWriter succeeds on a file that was
		/// already created
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCreateWriterOnExistingFile()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path name = new Path(new Path(Runtime.GetProperty("test.build.data", "."), "createWriterOnExistingFile"
				), "file");
			fs.Create(name);
			SequenceFile.CreateWriter(fs, conf, name, typeof(RandomDatum), typeof(RandomDatum
				), 512, (short)1, 4096, false, SequenceFile.CompressionType.None, null, new SequenceFile.Metadata
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecursiveSeqFileCreate()
		{
			FileSystem fs = FileSystem.GetLocal(conf);
			Path name = new Path(new Path(Runtime.GetProperty("test.build.data", "."), "recursiveCreateDir"
				), "file");
			bool createParent = false;
			try
			{
				SequenceFile.CreateWriter(fs, conf, name, typeof(RandomDatum), typeof(RandomDatum
					), 512, (short)1, 4096, createParent, SequenceFile.CompressionType.None, null, new 
					SequenceFile.Metadata());
				Fail("Expected an IOException due to missing parent");
			}
			catch (IOException)
			{
			}
			// Expected
			createParent = true;
			SequenceFile.CreateWriter(fs, conf, name, typeof(RandomDatum), typeof(RandomDatum
				), 512, (short)1, 4096, createParent, SequenceFile.CompressionType.None, null, new 
				SequenceFile.Metadata());
		}

		// should succeed, fails if exception thrown
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSerializationAvailability()
		{
			Configuration conf = new Configuration();
			Path path = new Path(Runtime.GetProperty("test.build.data", "."), "serializationAvailability"
				);
			// Check if any serializers aren't found.
			try
			{
				SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(path), SequenceFile.Writer
					.KeyClass(typeof(string)), SequenceFile.Writer.ValueClass(typeof(NullWritable)));
				// Note: This may also fail someday if JavaSerialization
				// is activated by default.
				Fail("Must throw IOException for missing serializer for the Key class");
			}
			catch (IOException e)
			{
				Assert.True(e.Message.StartsWith("Could not find a serializer for the Key class: '"
					 + typeof(string).FullName + "'."));
			}
			try
			{
				SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(path), SequenceFile.Writer
					.KeyClass(typeof(NullWritable)), SequenceFile.Writer.ValueClass(typeof(string)));
				// Note: This may also fail someday if JavaSerialization
				// is activated by default.
				Fail("Must throw IOException for missing serializer for the Value class");
			}
			catch (IOException e)
			{
				Assert.True(e.Message.StartsWith("Could not find a serializer for the Value class: '"
					 + typeof(string).FullName + "'."));
			}
			// Write a simple file to test deserialization failures with
			WriteTest(FileSystem.Get(conf), 1, 1, path, SequenceFile.CompressionType.None, null
				);
			// Remove Writable serializations, to enforce error.
			conf.SetStrings(CommonConfigurationKeys.IoSerializationsKey, typeof(AvroReflectSerialization
				).FullName);
			// Now check if any deserializers aren't found.
			try
			{
				new SequenceFile.Reader(conf, SequenceFile.Reader.File(path));
				Fail("Must throw IOException for missing deserializer for the Key class");
			}
			catch (IOException e)
			{
				Assert.True(e.Message.StartsWith("Could not find a deserializer for the Key class: '"
					 + typeof(RandomDatum).FullName + "'."));
			}
		}

		/// <summary>For debugging and testing.</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int count = 1024 * 1024;
			int megabytes = 1;
			int factor = 10;
			bool create = true;
			bool rwonly = false;
			bool check = false;
			bool fast = false;
			bool merge = false;
			string compressType = "NONE";
			string compressionCodec = "org.apache.hadoop.io.compress.DefaultCodec";
			Path file = null;
			int seed = new Random().Next();
			string usage = "Usage: SequenceFile " + "[-count N] " + "[-seed #] [-check] [-compressType <NONE|RECORD|BLOCK>] "
				 + "-codec <compressionCodec> " + "[[-rwonly] | {[-megabytes M] [-factor F] [-nocreate] [-fast] [-merge]}] "
				 + " file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			FileSystem fs = null;
			try
			{
				for (int i = 0; i < args.Length; ++i)
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
							if (args[i].Equals("-megabytes"))
							{
								megabytes = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-factor"))
								{
									factor = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (args[i].Equals("-seed"))
									{
										seed = System.Convert.ToInt32(args[++i]);
									}
									else
									{
										if (args[i].Equals("-rwonly"))
										{
											rwonly = true;
										}
										else
										{
											if (args[i].Equals("-nocreate"))
											{
												create = false;
											}
											else
											{
												if (args[i].Equals("-check"))
												{
													check = true;
												}
												else
												{
													if (args[i].Equals("-fast"))
													{
														fast = true;
													}
													else
													{
														if (args[i].Equals("-merge"))
														{
															merge = true;
														}
														else
														{
															if (args[i].Equals("-compressType"))
															{
																compressType = args[++i];
															}
															else
															{
																if (args[i].Equals("-codec"))
																{
																	compressionCodec = args[++i];
																}
																else
																{
																	// file is required parameter
																	file = new Path(args[i]);
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				TestSequenceFile test = new TestSequenceFile();
				fs = file.GetFileSystem(test.conf);
				Log.Info("count = " + count);
				Log.Info("megabytes = " + megabytes);
				Log.Info("factor = " + factor);
				Log.Info("create = " + create);
				Log.Info("seed = " + seed);
				Log.Info("rwonly = " + rwonly);
				Log.Info("check = " + check);
				Log.Info("fast = " + fast);
				Log.Info("merge = " + merge);
				Log.Info("compressType = " + compressType);
				Log.Info("compressionCodec = " + compressionCodec);
				Log.Info("file = " + file);
				if (rwonly && (!create || merge || fast))
				{
					System.Console.Error.WriteLine(usage);
					System.Environment.Exit(-1);
				}
				SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.ValueOf
					(compressType);
				CompressionCodec codec = (CompressionCodec)ReflectionUtils.NewInstance(test.conf.
					GetClassByName(compressionCodec), test.conf);
				if (rwonly || (create && !merge))
				{
					test.WriteTest(fs, count, seed, file, compressionType, codec);
					test.ReadTest(fs, count, seed, file);
				}
				if (!rwonly)
				{
					if (merge)
					{
						test.MergeTest(fs, count, seed, file, compressionType, fast, factor, megabytes);
					}
					else
					{
						test.SortTest(fs, count, megabytes, factor, fast, file);
					}
				}
				if (check)
				{
					test.CheckSort(fs, count, seed, file);
				}
			}
			finally
			{
				fs.Close();
			}
		}
	}
}
