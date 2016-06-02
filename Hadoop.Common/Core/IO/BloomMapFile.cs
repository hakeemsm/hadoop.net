using System;
using System.IO;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Util.Bloom;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// This class extends
	/// <see cref="MapFile"/>
	/// and provides very much the same
	/// functionality. However, it uses dynamic Bloom filters to provide
	/// quick membership test for keys, and it offers a fast version of
	/// <see cref="Reader.Get(WritableComparable{T}, Writable)"/>
	/// operation, especially in
	/// case of sparsely populated MapFile-s.
	/// </summary>
	public class BloomMapFile
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(BloomMapFile));

		public const string BloomFileName = "bloom";

		public const int HashCount = 5;

		/// <exception cref="System.IO.IOException"/>
		public static void Delete(FileSystem fs, string name)
		{
			Path dir = new Path(name);
			Path data = new Path(dir, MapFile.DataFileName);
			Path index = new Path(dir, MapFile.IndexFileName);
			Path bloom = new Path(dir, BloomFileName);
			fs.Delete(data, true);
			fs.Delete(index, true);
			fs.Delete(bloom, true);
			fs.Delete(dir, true);
		}

		private static byte[] ByteArrayForBloomKey(DataOutputBuffer buf)
		{
			int cleanLength = buf.GetLength();
			byte[] ba = buf.GetData();
			if (cleanLength != ba.Length)
			{
				ba = new byte[cleanLength];
				System.Array.Copy(buf.GetData(), 0, ba, 0, cleanLength);
			}
			return ba;
		}

		public class Writer : MapFile.Writer
		{
			private DynamicBloomFilter bloomFilter;

			private int numKeys;

			private int vectorSize;

			private Key bloomKey = new Key();

			private DataOutputBuffer buf = new DataOutputBuffer();

			private FileSystem fs;

			private Path dir;

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress, CompressionCodec codec, Progressable
				 progress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress, codec), Progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress, Progressable progress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress), Progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress, CompressionCodec
				 codec, Progressable progress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress, codec), Progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress, Progressable 
				progress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress), Progressable(progress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, Path dir, params SequenceFile.Writer.Option[] options
				)
				: base(conf, dir, options)
			{
				this.fs = dir.GetFileSystem(conf);
				this.dir = dir;
				InitBloomFilter(conf);
			}

			private void InitBloomFilter(Configuration conf)
			{
				lock (this)
				{
					numKeys = conf.GetInt("io.mapfile.bloom.size", 1024 * 1024);
					// vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
					// single key, where <code> is the number of hash functions,
					// <code>n</code> is the number of keys and <code>c</code> is the desired
					// max. error rate.
					// Our desired error rate is by default 0.005, i.e. 0.5%
					float errorRate = conf.GetFloat("io.mapfile.bloom.error.rate", 0.005f);
					vectorSize = (int)Math.Ceil((double)(-HashCount * numKeys) / Math.Log(1.0 - Math.
						Pow(errorRate, 1.0 / HashCount)));
					bloomFilter = new DynamicBloomFilter(vectorSize, HashCount, Org.Apache.Hadoop.Util.Hash.Hash
						.GetHashType(conf), numKeys);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Append(WritableComparable key, Writable val)
			{
				lock (this)
				{
					base.Append(key, val);
					buf.Reset();
					key.Write(buf);
					bloomKey.Set(ByteArrayForBloomKey(buf), 1.0);
					bloomFilter.Add(bloomKey);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				lock (this)
				{
					base.Close();
					DataOutputStream @out = fs.Create(new Path(dir, BloomFileName), true);
					try
					{
						bloomFilter.Write(@out);
						@out.Flush();
						@out.Close();
						@out = null;
					}
					finally
					{
						IOUtils.CloseStream(@out);
					}
				}
			}
		}

		public class Reader : MapFile.Reader
		{
			private DynamicBloomFilter bloomFilter;

			private DataOutputBuffer buf = new DataOutputBuffer();

			private Key bloomKey = new Key();

			/// <exception cref="System.IO.IOException"/>
			public Reader(Path dir, Configuration conf, params SequenceFile.Reader.Option[] options
				)
				: base(dir, conf, options)
			{
				InitBloomFilter(dir, conf);
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Reader(FileSystem fs, string dirName, Configuration conf)
				: this(new Path(dirName), conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Reader(FileSystem fs, string dirName, WritableComparator comparator, Configuration
				 conf, bool open)
				: this(new Path(dirName), conf, Comparator(comparator))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			[Obsolete]
			public Reader(FileSystem fs, string dirName, WritableComparator comparator, Configuration
				 conf)
				: this(new Path(dirName), conf, Comparator(comparator))
			{
			}

			private void InitBloomFilter(Path dirName, Configuration conf)
			{
				DataInputStream @in = null;
				try
				{
					FileSystem fs = dirName.GetFileSystem(conf);
					@in = fs.Open(new Path(dirName, BloomFileName));
					bloomFilter = new DynamicBloomFilter();
					bloomFilter.ReadFields(@in);
					@in.Close();
					@in = null;
				}
				catch (IOException ioe)
				{
					Log.Warn("Can't open BloomFilter: " + ioe + " - fallback to MapFile.");
					bloomFilter = null;
				}
				finally
				{
					IOUtils.CloseStream(@in);
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
			public virtual bool ProbablyHasKey(WritableComparable key)
			{
				if (bloomFilter == null)
				{
					return true;
				}
				buf.Reset();
				key.Write(buf);
				bloomKey.Set(ByteArrayForBloomKey(buf), 1.0);
				return bloomFilter.MembershipTest(bloomKey);
			}

			/// <summary>
			/// Fast version of the
			/// <see cref="Reader.Get(WritableComparable{T}, Writable)"/>
			/// method. First
			/// it checks the Bloom filter for the existence of the key, and only if
			/// present it performs the real get operation. This yields significant
			/// performance improvements for get operations on sparsely populated files.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override Writable Get(WritableComparable key, Writable val)
			{
				lock (this)
				{
					if (!ProbablyHasKey(key))
					{
						return null;
					}
					return base.Get(key, val);
				}
			}

			/// <summary>Retrieve the Bloom filter used by this instance of the Reader.</summary>
			/// <returns>
			/// a Bloom filter (see
			/// <see cref="Org.Apache.Hadoop.Util.Bloom.Filter"/>
			/// )
			/// </returns>
			public virtual Filter GetBloomFilter()
			{
				return bloomFilter;
			}
		}
	}
}
