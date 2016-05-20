using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Support for flat files of binary key/value pairs.</summary>
	public class TestSequenceFile : NUnit.Framework.TestCase
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestSequenceFile
			)));

		private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		public TestSequenceFile()
		{
		}

		public TestSequenceFile(string name)
			: base(name)
		{
		}

		/// <summary>Unit tests for SequenceFile.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testZlibSequenceFile()
		{
			LOG.info("Testing SequenceFile with DefaultCodec");
			compressedSeqFileTest(new org.apache.hadoop.io.compress.DefaultCodec());
			LOG.info("Successfully tested SequenceFile with DefaultCodec");
		}

		/// <exception cref="System.Exception"/>
		public virtual void compressedSeqFileTest(org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			int count = 1024 * 10;
			int megabytes = 1;
			int factor = 5;
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/test.seq");
			org.apache.hadoop.fs.Path recordCompressedFile = new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("test.build.data", ".") + "/test.rc.seq");
			org.apache.hadoop.fs.Path blockCompressedFile = new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("test.build.data", ".") + "/test.bc.seq");
			int seed = new java.util.Random().nextInt();
			LOG.info("Seed = " + seed);
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			try
			{
				// SequenceFile.Writer
				writeTest(fs, count, seed, file, org.apache.hadoop.io.SequenceFile.CompressionType
					.NONE, null);
				readTest(fs, count, seed, file);
				sortTest(fs, count, megabytes, factor, false, file);
				checkSort(fs, count, seed, file);
				sortTest(fs, count, megabytes, factor, true, file);
				checkSort(fs, count, seed, file);
				mergeTest(fs, count, seed, file, org.apache.hadoop.io.SequenceFile.CompressionType
					.NONE, false, factor, megabytes);
				checkSort(fs, count, seed, file);
				mergeTest(fs, count, seed, file, org.apache.hadoop.io.SequenceFile.CompressionType
					.NONE, true, factor, megabytes);
				checkSort(fs, count, seed, file);
				// SequenceFile.RecordCompressWriter
				writeTest(fs, count, seed, recordCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD, codec);
				readTest(fs, count, seed, recordCompressedFile);
				sortTest(fs, count, megabytes, factor, false, recordCompressedFile);
				checkSort(fs, count, seed, recordCompressedFile);
				sortTest(fs, count, megabytes, factor, true, recordCompressedFile);
				checkSort(fs, count, seed, recordCompressedFile);
				mergeTest(fs, count, seed, recordCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD, false, factor, megabytes);
				checkSort(fs, count, seed, recordCompressedFile);
				mergeTest(fs, count, seed, recordCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD, true, factor, megabytes);
				checkSort(fs, count, seed, recordCompressedFile);
				// SequenceFile.BlockCompressWriter
				writeTest(fs, count, seed, blockCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.BLOCK, codec);
				readTest(fs, count, seed, blockCompressedFile);
				sortTest(fs, count, megabytes, factor, false, blockCompressedFile);
				checkSort(fs, count, seed, blockCompressedFile);
				sortTest(fs, count, megabytes, factor, true, blockCompressedFile);
				checkSort(fs, count, seed, blockCompressedFile);
				mergeTest(fs, count, seed, blockCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.BLOCK, false, factor, megabytes);
				checkSort(fs, count, seed, blockCompressedFile);
				mergeTest(fs, count, seed, blockCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.BLOCK, true, factor, megabytes);
				checkSort(fs, count, seed, blockCompressedFile);
			}
			finally
			{
				fs.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeTest(org.apache.hadoop.fs.FileSystem fs, int count, int seed, org.apache.hadoop.fs.Path
			 file, org.apache.hadoop.io.SequenceFile.CompressionType compressionType, org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			fs.delete(file, true);
			LOG.info("creating " + count + " records with " + compressionType + " compression"
				);
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(fs, conf, file, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)), compressionType
				, codec);
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i = 0; i < count; i++)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				writer.append(key, value);
			}
			writer.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void readTest(org.apache.hadoop.fs.FileSystem fs, int count, int seed, org.apache.hadoop.fs.Path
			 file)
		{
			LOG.debug("reading " + count + " records");
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, file, conf);
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			org.apache.hadoop.io.RandomDatum k = new org.apache.hadoop.io.RandomDatum();
			org.apache.hadoop.io.RandomDatum v = new org.apache.hadoop.io.RandomDatum();
			org.apache.hadoop.io.DataOutputBuffer rawKey = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.SequenceFile.ValueBytes rawValue = reader.createValueBytes();
			for (int i = 0; i < count; i++)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				try
				{
					if ((i % 5) == 0)
					{
						// Testing 'raw' apis
						rawKey.reset();
						reader.nextRaw(rawKey, rawValue);
					}
					else
					{
						// Testing 'non-raw' apis 
						if ((i % 2) == 0)
						{
							reader.next(k);
							reader.getCurrentValue(v);
						}
						else
						{
							reader.next(k, v);
						}
						// Check
						if (!k.Equals(key))
						{
							throw new System.Exception("wrong key at " + i);
						}
						if (!v.Equals(value))
						{
							throw new System.Exception("wrong value at " + i);
						}
					}
				}
				catch (System.IO.IOException ioe)
				{
					LOG.info("Problem on row " + i);
					LOG.info("Expected key = " + key);
					LOG.info("Expected len = " + key.getLength());
					LOG.info("Actual key = " + k);
					LOG.info("Actual len = " + k.getLength());
					LOG.info("Expected value = " + value);
					LOG.info("Expected len = " + value.getLength());
					LOG.info("Actual value = " + v);
					LOG.info("Actual len = " + v.getLength());
					LOG.info("Key equals: " + k.Equals(key));
					LOG.info("value equals: " + v.Equals(value));
					throw;
				}
			}
			reader.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void sortTest(org.apache.hadoop.fs.FileSystem fs, int count, int megabytes
			, int factor, bool fast, org.apache.hadoop.fs.Path file)
		{
			fs.delete(new org.apache.hadoop.fs.Path(file + ".sorted"), true);
			org.apache.hadoop.io.SequenceFile.Sorter sorter = newSorter(fs, fast, megabytes, 
				factor);
			LOG.debug("sorting " + count + " records");
			sorter.sort(file, file.suffix(".sorted"));
			LOG.info("done sorting " + count + " debug");
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkSort(org.apache.hadoop.fs.FileSystem fs, int count, int seed, org.apache.hadoop.fs.Path
			 file)
		{
			LOG.info("sorting " + count + " records in memory for debug");
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.RandomDatum, org.apache.hadoop.io.RandomDatum
				> map = new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.RandomDatum
				, org.apache.hadoop.io.RandomDatum>();
			for (int i = 0; i < count; i++)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				map[key] = value;
			}
			LOG.debug("checking order of " + count + " records");
			org.apache.hadoop.io.RandomDatum k = new org.apache.hadoop.io.RandomDatum();
			org.apache.hadoop.io.RandomDatum v = new org.apache.hadoop.io.RandomDatum();
			System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.RandomDatum
				, org.apache.hadoop.io.RandomDatum>> iterator = map.GetEnumerator();
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, file.suffix(".sorted"), conf);
			for (int i_1 = 0; i_1 < count; i_1++)
			{
				System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.RandomDatum, org.apache.hadoop.io.RandomDatum
					> entry = iterator.Current;
				org.apache.hadoop.io.RandomDatum key = entry.Key;
				org.apache.hadoop.io.RandomDatum value = entry.Value;
				reader.next(k, v);
				if (!k.Equals(key))
				{
					throw new System.Exception("wrong key at " + i_1);
				}
				if (!v.Equals(value))
				{
					throw new System.Exception("wrong value at " + i_1);
				}
			}
			reader.close();
			LOG.debug("sucessfully checked " + count + " records");
		}

		/// <exception cref="System.IO.IOException"/>
		private void mergeTest(org.apache.hadoop.fs.FileSystem fs, int count, int seed, org.apache.hadoop.fs.Path
			 file, org.apache.hadoop.io.SequenceFile.CompressionType compressionType, bool fast
			, int factor, int megabytes)
		{
			LOG.debug("creating " + factor + " files with " + count / factor + " records");
			org.apache.hadoop.io.SequenceFile.Writer[] writers = new org.apache.hadoop.io.SequenceFile.Writer
				[factor];
			org.apache.hadoop.fs.Path[] names = new org.apache.hadoop.fs.Path[factor];
			org.apache.hadoop.fs.Path[] sortedNames = new org.apache.hadoop.fs.Path[factor];
			for (int i = 0; i < factor; i++)
			{
				names[i] = file.suffix("." + i);
				sortedNames[i] = names[i].suffix(".sorted");
				fs.delete(names[i], true);
				fs.delete(sortedNames[i], true);
				writers[i] = org.apache.hadoop.io.SequenceFile.createWriter(fs, conf, names[i], Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.RandomDatum)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.RandomDatum)), compressionType);
			}
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i_1 = 0; i_1 < count; i_1++)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				writers[i_1 % factor].append(key, value);
			}
			for (int i_2 = 0; i_2 < factor; i_2++)
			{
				writers[i_2].close();
			}
			for (int i_3 = 0; i_3 < factor; i_3++)
			{
				LOG.debug("sorting file " + i_3 + " with " + count / factor + " records");
				newSorter(fs, fast, megabytes, factor).sort(names[i_3], sortedNames[i_3]);
			}
			LOG.info("merging " + factor + " files with " + count / factor + " debug");
			fs.delete(new org.apache.hadoop.fs.Path(file + ".sorted"), true);
			newSorter(fs, fast, megabytes, factor).merge(sortedNames, file.suffix(".sorted"));
		}

		private org.apache.hadoop.io.SequenceFile.Sorter newSorter(org.apache.hadoop.fs.FileSystem
			 fs, bool fast, int megabytes, int factor)
		{
			org.apache.hadoop.io.SequenceFile.Sorter sorter = fast ? new org.apache.hadoop.io.SequenceFile.Sorter
				(fs, new org.apache.hadoop.io.RandomDatum.Comparator(), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.RandomDatum)), Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.RandomDatum)), conf) : new org.apache.hadoop.io.SequenceFile.Sorter
				(fs, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)), conf);
			sorter.setMemory(megabytes * 1024 * 1024);
			sorter.setFactor(factor);
			return sorter;
		}

		/// <summary>Unit tests for SequenceFile metadata.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testSequenceFileMetadata()
		{
			LOG.info("Testing SequenceFile with metadata");
			int count = 1024 * 10;
			org.apache.hadoop.io.compress.CompressionCodec codec = new org.apache.hadoop.io.compress.DefaultCodec
				();
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/test.seq.metadata");
			org.apache.hadoop.fs.Path sortedFile = new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("test.build.data", ".") + "/test.sorted.seq.metadata");
			org.apache.hadoop.fs.Path recordCompressedFile = new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("test.build.data", ".") + "/test.rc.seq.metadata");
			org.apache.hadoop.fs.Path blockCompressedFile = new org.apache.hadoop.fs.Path(Sharpen.Runtime
				.getProperty("test.build.data", ".") + "/test.bc.seq.metadata");
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.io.SequenceFile.Metadata theMetadata = new org.apache.hadoop.io.SequenceFile.Metadata
				();
			theMetadata.set(new org.apache.hadoop.io.Text("name_1"), new org.apache.hadoop.io.Text
				("value_1"));
			theMetadata.set(new org.apache.hadoop.io.Text("name_2"), new org.apache.hadoop.io.Text
				("value_2"));
			theMetadata.set(new org.apache.hadoop.io.Text("name_3"), new org.apache.hadoop.io.Text
				("value_3"));
			theMetadata.set(new org.apache.hadoop.io.Text("name_4"), new org.apache.hadoop.io.Text
				("value_4"));
			int seed = new java.util.Random().nextInt();
			try
			{
				// SequenceFile.Writer
				writeMetadataTest(fs, count, seed, file, org.apache.hadoop.io.SequenceFile.CompressionType
					.NONE, null, theMetadata);
				org.apache.hadoop.io.SequenceFile.Metadata aMetadata = readMetadata(fs, file);
				if (!theMetadata.equals(aMetadata))
				{
					LOG.info("The original metadata:\n" + theMetadata.ToString());
					LOG.info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new System.Exception("metadata not match:  " + 1);
				}
				// SequenceFile.RecordCompressWriter
				writeMetadataTest(fs, count, seed, recordCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.RECORD, codec, theMetadata);
				aMetadata = readMetadata(fs, recordCompressedFile);
				if (!theMetadata.equals(aMetadata))
				{
					LOG.info("The original metadata:\n" + theMetadata.ToString());
					LOG.info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new System.Exception("metadata not match:  " + 2);
				}
				// SequenceFile.BlockCompressWriter
				writeMetadataTest(fs, count, seed, blockCompressedFile, org.apache.hadoop.io.SequenceFile.CompressionType
					.BLOCK, codec, theMetadata);
				aMetadata = readMetadata(fs, blockCompressedFile);
				if (!theMetadata.equals(aMetadata))
				{
					LOG.info("The original metadata:\n" + theMetadata.ToString());
					LOG.info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new System.Exception("metadata not match:  " + 3);
				}
				// SequenceFile.Sorter
				sortMetadataTest(fs, file, sortedFile, theMetadata);
				aMetadata = readMetadata(fs, recordCompressedFile);
				if (!theMetadata.equals(aMetadata))
				{
					LOG.info("The original metadata:\n" + theMetadata.ToString());
					LOG.info("The retrieved metadata:\n" + aMetadata.ToString());
					throw new System.Exception("metadata not match:  " + 4);
				}
			}
			finally
			{
				fs.close();
			}
			LOG.info("Successfully tested SequenceFile with metadata");
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.io.SequenceFile.Metadata readMetadata(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.fs.Path file)
		{
			LOG.info("reading file: " + file.ToString());
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, file, conf);
			org.apache.hadoop.io.SequenceFile.Metadata meta = reader.getMetadata();
			reader.close();
			return meta;
		}

		/// <exception cref="System.IO.IOException"/>
		private void writeMetadataTest(org.apache.hadoop.fs.FileSystem fs, int count, int
			 seed, org.apache.hadoop.fs.Path file, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.io.SequenceFile.Metadata
			 metadata)
		{
			fs.delete(file, true);
			LOG.info("creating " + count + " records with metadata and with " + compressionType
				 + " compression");
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(fs, conf, file, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)), compressionType
				, codec, null, metadata);
			org.apache.hadoop.io.RandomDatum.Generator generator = new org.apache.hadoop.io.RandomDatum.Generator
				(seed);
			for (int i = 0; i < count; i++)
			{
				generator.next();
				org.apache.hadoop.io.RandomDatum key = generator.getKey();
				org.apache.hadoop.io.RandomDatum value = generator.getValue();
				writer.append(key, value);
			}
			writer.close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void sortMetadataTest(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 unsortedFile, org.apache.hadoop.fs.Path sortedFile, org.apache.hadoop.io.SequenceFile.Metadata
			 metadata)
		{
			fs.delete(sortedFile, true);
			LOG.info("sorting: " + unsortedFile + " to: " + sortedFile);
			org.apache.hadoop.io.WritableComparator comparator = org.apache.hadoop.io.WritableComparator
				.get(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)));
			org.apache.hadoop.io.SequenceFile.Sorter sorter = new org.apache.hadoop.io.SequenceFile.Sorter
				(fs, comparator, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)), conf
				, metadata);
			sorter.sort(new org.apache.hadoop.fs.Path[] { unsortedFile }, sortedFile, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testClose()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.LocalFileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			// create a sequence file 1
			org.apache.hadoop.fs.Path path1 = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/test1.seq");
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(fs, conf, path1, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable)), 
				org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK);
			writer.append(new org.apache.hadoop.io.Text("file1-1"), org.apache.hadoop.io.NullWritable
				.get());
			writer.append(new org.apache.hadoop.io.Text("file1-2"), org.apache.hadoop.io.NullWritable
				.get());
			writer.close();
			org.apache.hadoop.fs.Path path2 = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/test2.seq");
			writer = org.apache.hadoop.io.SequenceFile.createWriter(fs, conf, path2, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.Text)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable
				)), org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK);
			writer.append(new org.apache.hadoop.io.Text("file2-1"), org.apache.hadoop.io.NullWritable
				.get());
			writer.append(new org.apache.hadoop.io.Text("file2-2"), org.apache.hadoop.io.NullWritable
				.get());
			writer.close();
			// Create a reader which uses 4 BuiltInZLibInflater instances
			org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, path1, conf);
			// Returns the 4 BuiltInZLibInflater instances to the CodecPool
			reader.close();
			// The second close _could_ erroneously returns the same 
			// 4 BuiltInZLibInflater instances to the CodecPool again
			reader.close();
			// The first reader gets 4 BuiltInZLibInflater instances from the CodecPool
			org.apache.hadoop.io.SequenceFile.Reader reader1 = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, path1, conf);
			// read first value from reader1
			org.apache.hadoop.io.Text text = new org.apache.hadoop.io.Text();
			reader1.next(text);
			NUnit.Framework.Assert.AreEqual("file1-1", text.ToString());
			// The second reader _could_ get the same 4 BuiltInZLibInflater 
			// instances from the CodePool as reader1
			org.apache.hadoop.io.SequenceFile.Reader reader2 = new org.apache.hadoop.io.SequenceFile.Reader
				(fs, path2, conf);
			// read first value from reader2
			reader2.next(text);
			NUnit.Framework.Assert.AreEqual("file2-1", text.ToString());
			// read second value from reader1
			reader1.next(text);
			NUnit.Framework.Assert.AreEqual("file1-2", text.ToString());
			// read second value from reader2 (this throws an exception)
			reader2.next(text);
			NUnit.Framework.Assert.AreEqual("file2-2", text.ToString());
			NUnit.Framework.Assert.IsFalse(reader1.next(text));
			NUnit.Framework.Assert.IsFalse(reader2.next(text));
		}

		/// <summary>Test that makes sure the FileSystem passed to createWriter</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testCreateUsesFsArg()
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.FileSystem spyFs = org.mockito.Mockito.spy(fs);
			org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/testCreateUsesFSArg.seq");
			org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
				.createWriter(spyFs, conf, p, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable)));
			writer.close();
			org.mockito.Mockito.verify(spyFs).getDefaultReplication(p);
		}

		private class TestFSDataInputStream : org.apache.hadoop.fs.FSDataInputStream
		{
			private bool closed = false;

			/// <exception cref="System.IO.IOException"/>
			private TestFSDataInputStream(java.io.InputStream @in)
				: base(@in)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				closed = true;
				base.close();
			}

			public virtual bool isClosed()
			{
				return closed;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testCloseForErroneousSequenceFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.LocalFileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal
				(conf);
			// create an empty file (which is not a valid sequence file)
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", ".") + "/broken.seq");
			fs.create(path).close();
			// try to create SequenceFile.Reader
			org.apache.hadoop.io.TestSequenceFile.TestFSDataInputStream[] openedFile = new org.apache.hadoop.io.TestSequenceFile.TestFSDataInputStream
				[1];
			try
			{
				new _Reader_509(openedFile, fs, path, conf);
				// this method is called by the SequenceFile.Reader constructor, overwritten, so we can access the opened file
				fail("IOException expected.");
			}
			catch (System.IO.IOException)
			{
			}
			NUnit.Framework.Assert.IsNotNull(path + " should have been opened.", openedFile[0
				]);
			NUnit.Framework.Assert.IsTrue("InputStream for " + path + " should have been closed."
				, openedFile[0].isClosed());
		}

		private sealed class _Reader_509 : org.apache.hadoop.io.SequenceFile.Reader
		{
			public _Reader_509(org.apache.hadoop.io.TestSequenceFile.TestFSDataInputStream[] 
				openedFile, org.apache.hadoop.fs.FileSystem baseArg1, org.apache.hadoop.fs.Path 
				baseArg2, org.apache.hadoop.conf.Configuration baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.openedFile = openedFile;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override org.apache.hadoop.fs.FSDataInputStream openFile(org.apache.hadoop.fs.FileSystem
				 fs, org.apache.hadoop.fs.Path file, int bufferSize, long length)
			{
				java.io.InputStream @in = base.openFile(fs, file, bufferSize, length);
				openedFile[0] = new org.apache.hadoop.io.TestSequenceFile.TestFSDataInputStream(@in
					);
				return openedFile[0];
			}

			private readonly org.apache.hadoop.io.TestSequenceFile.TestFSDataInputStream[] openedFile;
		}

		/// <summary>
		/// Test that makes sure createWriter succeeds on a file that was
		/// already created
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCreateWriterOnExistingFile()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path name = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(Sharpen.Runtime.getProperty("test.build.data", "."), "createWriterOnExistingFile"
				), "file");
			fs.create(name);
			org.apache.hadoop.io.SequenceFile.createWriter(fs, conf, name, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.RandomDatum)), Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.RandomDatum)), 512, (short)1, 4096, false, org.apache.hadoop.io.SequenceFile.CompressionType
				.NONE, null, new org.apache.hadoop.io.SequenceFile.Metadata());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRecursiveSeqFileCreate()
		{
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.fs.Path name = new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path
				(Sharpen.Runtime.getProperty("test.build.data", "."), "recursiveCreateDir"), "file"
				);
			bool createParent = false;
			try
			{
				org.apache.hadoop.io.SequenceFile.createWriter(fs, conf, name, Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.RandomDatum)), Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.RandomDatum)), 512, (short)1, 4096, createParent, org.apache.hadoop.io.SequenceFile.CompressionType
					.NONE, null, new org.apache.hadoop.io.SequenceFile.Metadata());
				fail("Expected an IOException due to missing parent");
			}
			catch (System.IO.IOException)
			{
			}
			// Expected
			createParent = true;
			org.apache.hadoop.io.SequenceFile.createWriter(fs, conf, name, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.RandomDatum)), Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.RandomDatum)), 512, (short)1, 4096, createParent, org.apache.hadoop.io.SequenceFile.CompressionType
				.NONE, null, new org.apache.hadoop.io.SequenceFile.Metadata());
		}

		// should succeed, fails if exception thrown
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSerializationAvailability()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(Sharpen.Runtime.getProperty
				("test.build.data", "."), "serializationAvailability");
			// Check if any serializers aren't found.
			try
			{
				org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(path), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(string))), org.apache.hadoop.io.SequenceFile.Writer.valueClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.NullWritable))));
				// Note: This may also fail someday if JavaSerialization
				// is activated by default.
				fail("Must throw IOException for missing serializer for the Key class");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Could not find a serializer for the Key class: '"
					 + Sharpen.Runtime.getClassForType(typeof(string)).getName() + "'."));
			}
			try
			{
				org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(path), org.apache.hadoop.io.SequenceFile.Writer.keyClass(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.NullWritable))), org.apache.hadoop.io.SequenceFile.Writer
					.valueClass(Sharpen.Runtime.getClassForType(typeof(string))));
				// Note: This may also fail someday if JavaSerialization
				// is activated by default.
				fail("Must throw IOException for missing serializer for the Value class");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Could not find a serializer for the Value class: '"
					 + Sharpen.Runtime.getClassForType(typeof(string)).getName() + "'."));
			}
			// Write a simple file to test deserialization failures with
			writeTest(org.apache.hadoop.fs.FileSystem.get(conf), 1, 1, path, org.apache.hadoop.io.SequenceFile.CompressionType
				.NONE, null);
			// Remove Writable serializations, to enforce error.
			conf.setStrings(org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.serializer.avro.AvroReflectSerialization
				)).getName());
			// Now check if any deserializers aren't found.
			try
			{
				new org.apache.hadoop.io.SequenceFile.Reader(conf, org.apache.hadoop.io.SequenceFile.Reader
					.file(path));
				fail("Must throw IOException for missing deserializer for the Key class");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Could not find a deserializer for the Key class: '"
					 + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)).getName
					() + "'."));
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
			org.apache.hadoop.fs.Path file = null;
			int seed = new java.util.Random().nextInt();
			string usage = "Usage: SequenceFile " + "[-count N] " + "[-seed #] [-check] [-compressType <NONE|RECORD|BLOCK>] "
				 + "-codec <compressionCodec> " + "[[-rwonly] | {[-megabytes M] [-factor F] [-nocreate] [-fast] [-merge]}] "
				 + " file";
			if (args.Length == 0)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			org.apache.hadoop.fs.FileSystem fs = null;
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
																	file = new org.apache.hadoop.fs.Path(args[i]);
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
				org.apache.hadoop.io.TestSequenceFile test = new org.apache.hadoop.io.TestSequenceFile
					();
				fs = file.getFileSystem(test.conf);
				LOG.info("count = " + count);
				LOG.info("megabytes = " + megabytes);
				LOG.info("factor = " + factor);
				LOG.info("create = " + create);
				LOG.info("seed = " + seed);
				LOG.info("rwonly = " + rwonly);
				LOG.info("check = " + check);
				LOG.info("fast = " + fast);
				LOG.info("merge = " + merge);
				LOG.info("compressType = " + compressType);
				LOG.info("compressionCodec = " + compressionCodec);
				LOG.info("file = " + file);
				if (rwonly && (!create || merge || fast))
				{
					System.Console.Error.WriteLine(usage);
					System.Environment.Exit(-1);
				}
				org.apache.hadoop.io.SequenceFile.CompressionType compressionType = org.apache.hadoop.io.SequenceFile.CompressionType
					.valueOf(compressType);
				org.apache.hadoop.io.compress.CompressionCodec codec = (org.apache.hadoop.io.compress.CompressionCodec
					)org.apache.hadoop.util.ReflectionUtils.newInstance(test.conf.getClassByName(compressionCodec
					), test.conf);
				if (rwonly || (create && !merge))
				{
					test.writeTest(fs, count, seed, file, compressionType, codec);
					test.readTest(fs, count, seed, file);
				}
				if (!rwonly)
				{
					if (merge)
					{
						test.mergeTest(fs, count, seed, file, compressionType, fast, factor, megabytes);
					}
					else
					{
						test.sortTest(fs, count, megabytes, factor, fast, file);
					}
				}
				if (check)
				{
					test.checkSort(fs, count, seed, file);
				}
			}
			finally
			{
				fs.close();
			}
		}
	}
}
