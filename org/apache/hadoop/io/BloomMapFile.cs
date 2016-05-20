using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// This class extends
	/// <see cref="MapFile"/>
	/// and provides very much the same
	/// functionality. However, it uses dynamic Bloom filters to provide
	/// quick membership test for keys, and it offers a fast version of
	/// <see cref="Reader.get(WritableComparable{T}, Writable)"/>
	/// operation, especially in
	/// case of sparsely populated MapFile-s.
	/// </summary>
	public class BloomMapFile
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BloomMapFile
			)));

		public const string BLOOM_FILE_NAME = "bloom";

		public const int HASH_COUNT = 5;

		/// <exception cref="System.IO.IOException"/>
		public static void delete(org.apache.hadoop.fs.FileSystem fs, string name)
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(name);
			org.apache.hadoop.fs.Path data = new org.apache.hadoop.fs.Path(dir, org.apache.hadoop.io.MapFile
				.DATA_FILE_NAME);
			org.apache.hadoop.fs.Path index = new org.apache.hadoop.fs.Path(dir, org.apache.hadoop.io.MapFile
				.INDEX_FILE_NAME);
			org.apache.hadoop.fs.Path bloom = new org.apache.hadoop.fs.Path(dir, BLOOM_FILE_NAME
				);
			fs.delete(data, true);
			fs.delete(index, true);
			fs.delete(bloom, true);
			fs.delete(dir, true);
		}

		private static byte[] byteArrayForBloomKey(org.apache.hadoop.io.DataOutputBuffer 
			buf)
		{
			int cleanLength = buf.getLength();
			byte[] ba = buf.getData();
			if (cleanLength != ba.Length)
			{
				ba = new byte[cleanLength];
				System.Array.Copy(buf.getData(), 0, ba, 0, cleanLength);
			}
			return ba;
		}

		public class Writer : org.apache.hadoop.io.MapFile.Writer
		{
			private org.apache.hadoop.util.bloom.DynamicBloomFilter bloomFilter;

			private int numKeys;

			private int vectorSize;

			private org.apache.hadoop.util.bloom.Key bloomKey = new org.apache.hadoop.util.bloom.Key
				();

			private org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.fs.FileSystem fs;

			private org.apache.hadoop.fs.Path dir;

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.util.Progressable
				 progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress, codec), progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress, org.apache.hadoop.util.Progressable progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress), progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress, org.apache.hadoop.io.compress.CompressionCodec
				 codec, org.apache.hadoop.util.Progressable progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress, codec), progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress, org.apache.hadoop.util.Progressable
				 progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress), progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path
				 dir, params org.apache.hadoop.io.SequenceFile.Writer.Option[] options)
				: base(conf, dir, options)
			{
				this.fs = dir.getFileSystem(conf);
				this.dir = dir;
				initBloomFilter(conf);
			}

			private void initBloomFilter(org.apache.hadoop.conf.Configuration conf)
			{
				lock (this)
				{
					numKeys = conf.getInt("io.mapfile.bloom.size", 1024 * 1024);
					// vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
					// single key, where <code> is the number of hash functions,
					// <code>n</code> is the number of keys and <code>c</code> is the desired
					// max. error rate.
					// Our desired error rate is by default 0.005, i.e. 0.5%
					float errorRate = conf.getFloat("io.mapfile.bloom.error.rate", 0.005f);
					vectorSize = (int)System.Math.ceil((double)(-HASH_COUNT * numKeys) / System.Math.
						log(1.0 - System.Math.pow(errorRate, 1.0 / HASH_COUNT)));
					bloomFilter = new org.apache.hadoop.util.bloom.DynamicBloomFilter(vectorSize, HASH_COUNT
						, org.apache.hadoop.util.hash.Hash.getHashType(conf), numKeys);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void append(org.apache.hadoop.io.WritableComparable key, org.apache.hadoop.io.Writable
				 val)
			{
				lock (this)
				{
					base.append(key, val);
					buf.reset();
					key.write(buf);
					bloomKey.set(byteArrayForBloomKey(buf), 1.0);
					bloomFilter.add(bloomKey);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				lock (this)
				{
					base.close();
					java.io.DataOutputStream @out = fs.create(new org.apache.hadoop.fs.Path(dir, BLOOM_FILE_NAME
						), true);
					try
					{
						bloomFilter.write(@out);
						@out.flush();
						@out.close();
						@out = null;
					}
					finally
					{
						org.apache.hadoop.io.IOUtils.closeStream(@out);
					}
				}
			}
		}

		public class Reader : org.apache.hadoop.io.MapFile.Reader
		{
			private org.apache.hadoop.util.bloom.DynamicBloomFilter bloomFilter;

			private org.apache.hadoop.io.DataOutputBuffer buf = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.util.bloom.Key bloomKey = new org.apache.hadoop.util.bloom.Key
				();

			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.Path dir, org.apache.hadoop.conf.Configuration
				 conf, params org.apache.hadoop.io.SequenceFile.Reader.Option[] options)
				: base(dir, conf, options)
			{
				initBloomFilter(dir, conf);
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.conf.Configuration
				 conf)
				: this(new org.apache.hadoop.fs.Path(dirName), conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.io.WritableComparator
				 comparator, org.apache.hadoop.conf.Configuration conf, bool open)
				: this(new org.apache.hadoop.fs.Path(dirName), conf, comparator(comparator))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[System.Obsolete]
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.io.WritableComparator
				 comparator, org.apache.hadoop.conf.Configuration conf)
				: this(new org.apache.hadoop.fs.Path(dirName), conf, comparator(comparator))
			{
			}

			private void initBloomFilter(org.apache.hadoop.fs.Path dirName, org.apache.hadoop.conf.Configuration
				 conf)
			{
				java.io.DataInputStream @in = null;
				try
				{
					org.apache.hadoop.fs.FileSystem fs = dirName.getFileSystem(conf);
					@in = fs.open(new org.apache.hadoop.fs.Path(dirName, BLOOM_FILE_NAME));
					bloomFilter = new org.apache.hadoop.util.bloom.DynamicBloomFilter();
					bloomFilter.readFields(@in);
					@in.close();
					@in = null;
				}
				catch (System.IO.IOException ioe)
				{
					LOG.warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
					bloomFilter = null;
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.closeStream(@in);
				}
			}

			/// <summary>Checks if this MapFile has the indicated key.</summary>
			/// <remarks>
			/// Checks if this MapFile has the indicated key. The membership test is
			/// performed using a Bloom filter, so the result has always non-zero
			/// probability of false positives.
			/// </remarks>
			/// <param name="key">key to check</param>
			/// <returns>false iff key doesn't exist, true if key probably exists.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool probablyHasKey(org.apache.hadoop.io.WritableComparable key)
			{
				if (bloomFilter == null)
				{
					return true;
				}
				buf.reset();
				key.write(buf);
				bloomKey.set(byteArrayForBloomKey(buf), 1.0);
				return bloomFilter.membershipTest(bloomKey);
			}

			/// <summary>
			/// Fast version of the
			/// <see cref="Reader.get(WritableComparable{T}, Writable)"/>
			/// method. First
			/// it checks the Bloom filter for the existence of the key, and only if
			/// present it performs the real get operation. This yields significant
			/// performance improvements for get operations on sparsely populated files.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.Writable get(org.apache.hadoop.io.WritableComparable
				 key, org.apache.hadoop.io.Writable val)
			{
				lock (this)
				{
					if (!probablyHasKey(key))
					{
						return null;
					}
					return base.get(key, val);
				}
			}

			/// <summary>Retrieve the Bloom filter used by this instance of the Reader.</summary>
			/// <returns>
			/// a Bloom filter (see
			/// <see cref="org.apache.hadoop.util.bloom.Filter"/>
			/// )
			/// </returns>
			public virtual org.apache.hadoop.util.bloom.Filter getBloomFilter()
			{
				return bloomFilter;
			}
		}
	}
}
