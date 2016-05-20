using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A file-based map from keys to values.</summary>
	/// <remarks>
	/// A file-based map from keys to values.
	/// <p>A map is a directory containing two files, the <code>data</code> file,
	/// containing all keys and values in the map, and a smaller <code>index</code>
	/// file, containing a fraction of the keys.  The fraction is determined by
	/// <see cref="Writer.getIndexInterval()"/>
	/// .
	/// <p>The index file is read entirely into memory.  Thus key implementations
	/// should try to keep themselves small.
	/// <p>Map files are created by adding entries in-order.  To maintain a large
	/// database, perform updates by copying the previous version of a database and
	/// merging in a sorted change list, to create a new version of the database in
	/// a new file.  Sorting large change lists can be done with
	/// <see cref="Sorter"/>
	/// .
	/// </remarks>
	public class MapFile
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.MapFile)));

		/// <summary>The name of the index file.</summary>
		public const string INDEX_FILE_NAME = "index";

		/// <summary>The name of the data file.</summary>
		public const string DATA_FILE_NAME = "data";

		protected internal MapFile()
		{
		}

		/// <summary>Writes a new map.</summary>
		public class Writer : java.io.Closeable
		{
			private org.apache.hadoop.io.SequenceFile.Writer data;

			private org.apache.hadoop.io.SequenceFile.Writer index;

			private const string INDEX_INTERVAL = "io.map.index.interval";

			private int indexInterval = 128;

			private long size;

			private org.apache.hadoop.io.LongWritable position = new org.apache.hadoop.io.LongWritable
				();

			private org.apache.hadoop.io.WritableComparator comparator;

			private org.apache.hadoop.io.DataInputBuffer inBuf = new org.apache.hadoop.io.DataInputBuffer
				();

			private org.apache.hadoop.io.DataOutputBuffer outBuf = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.io.WritableComparable lastKey;

			/// <summary>What's the position (in bytes) we wrote when we got the last index</summary>
			private long lastIndexPos = -1;

			/// <summary>What was size when we last wrote an index.</summary>
			/// <remarks>
			/// What was size when we last wrote an index. Set to MIN_VALUE to ensure that
			/// we have an index at position zero -- midKey will throw an exception if this
			/// is not the case
			/// </remarks>
			private long lastIndexKeyCount = long.MinValue;

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress, org.apache.hadoop.util.Progressable progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress), progressable(progress))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.util.Progressable
				 progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress, codec), progressable(progress))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
				 compress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), keyClass(keyClass), valueClass
					(valClass), compression(compress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...)} instead."
				)]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress, org.apache.hadoop.util.Progressable
				 progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress), progressable(progress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FileSystem
				 fs, string dirName, org.apache.hadoop.io.WritableComparator comparator, java.lang.Class
				 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compress, org.apache.hadoop.io.compress.CompressionCodec
				 codec, org.apache.hadoop.util.Progressable progress)
				: this(conf, new org.apache.hadoop.fs.Path(dirName), comparator(comparator), valueClass
					(valClass), compression(compress, codec), progressable(progress))
			{
			}

			public interface Option : org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				// no public ctor
				// the following fields are used only for checking key order
				// our options are a superset of sequence file writer options
			}

			private class KeyClassOption : org.apache.hadoop.util.Options.ClassOption, org.apache.hadoop.io.MapFile.Writer.Option
			{
				internal KeyClassOption(java.lang.Class value)
					: base(value)
				{
				}
			}

			private class ComparatorOption : org.apache.hadoop.io.MapFile.Writer.Option
			{
				private readonly org.apache.hadoop.io.WritableComparator value;

				internal ComparatorOption(org.apache.hadoop.io.WritableComparator value)
				{
					this.value = value;
				}

				internal virtual org.apache.hadoop.io.WritableComparator getValue()
				{
					return value;
				}
			}

			public static org.apache.hadoop.io.MapFile.Writer.Option keyClass(java.lang.Class
				 value)
			{
				return new org.apache.hadoop.io.MapFile.Writer.KeyClassOption(value);
			}

			public static org.apache.hadoop.io.MapFile.Writer.Option comparator(org.apache.hadoop.io.WritableComparator
				 value)
			{
				return new org.apache.hadoop.io.MapFile.Writer.ComparatorOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option valueClass(java.lang.Class
				 value)
			{
				return org.apache.hadoop.io.SequenceFile.Writer.valueClass(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option compression(org.apache.hadoop.io.SequenceFile.CompressionType
				 type)
			{
				return org.apache.hadoop.io.SequenceFile.Writer.compression(type);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option compression(org.apache.hadoop.io.SequenceFile.CompressionType
				 type, org.apache.hadoop.io.compress.CompressionCodec codec)
			{
				return org.apache.hadoop.io.SequenceFile.Writer.compression(type, codec);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option progressable(org.apache.hadoop.util.Progressable
				 value)
			{
				return org.apache.hadoop.io.SequenceFile.Writer.progressable(value);
			}

			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path
				 dirName, params org.apache.hadoop.io.SequenceFile.Writer.Option[] opts)
			{
				org.apache.hadoop.io.MapFile.Writer.KeyClassOption keyClassOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.MapFile.Writer.KeyClassOption>(opts);
				org.apache.hadoop.io.MapFile.Writer.ComparatorOption comparatorOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.MapFile.Writer.ComparatorOption>(opts);
				if ((keyClassOption == null) == (comparatorOption == null))
				{
					throw new System.ArgumentException("key class or comparator option " + "must be set"
						);
				}
				this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);
				java.lang.Class keyClass;
				if (keyClassOption == null)
				{
					this.comparator = comparatorOption.getValue();
					keyClass = comparator.getKeyClass();
				}
				else
				{
					keyClass = (java.lang.Class)keyClassOption.getValue();
					this.comparator = org.apache.hadoop.io.WritableComparator.get(keyClass, conf);
				}
				this.lastKey = comparator.newKey();
				org.apache.hadoop.fs.FileSystem fs = dirName.getFileSystem(conf);
				if (!fs.mkdirs(dirName))
				{
					throw new System.IO.IOException("Mkdirs failed to create directory " + dirName);
				}
				org.apache.hadoop.fs.Path dataFile = new org.apache.hadoop.fs.Path(dirName, DATA_FILE_NAME
					);
				org.apache.hadoop.fs.Path indexFile = new org.apache.hadoop.fs.Path(dirName, INDEX_FILE_NAME
					);
				org.apache.hadoop.io.SequenceFile.Writer.Option[] dataOptions = org.apache.hadoop.util.Options
					.prependOptions(opts, org.apache.hadoop.io.SequenceFile.Writer.file(dataFile), org.apache.hadoop.io.SequenceFile.Writer
					.keyClass(keyClass));
				this.data = org.apache.hadoop.io.SequenceFile.createWriter(conf, dataOptions);
				org.apache.hadoop.io.SequenceFile.Writer.Option[] indexOptions = org.apache.hadoop.util.Options
					.prependOptions(opts, org.apache.hadoop.io.SequenceFile.Writer.file(indexFile), 
					org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
					.valueClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
					))), org.apache.hadoop.io.SequenceFile.Writer.compression(org.apache.hadoop.io.SequenceFile.CompressionType
					.BLOCK));
				this.index = org.apache.hadoop.io.SequenceFile.createWriter(conf, indexOptions);
			}

			/// <summary>The number of entries that are added before an index entry is added.</summary>
			public virtual int getIndexInterval()
			{
				return indexInterval;
			}

			/// <summary>Sets the index interval.</summary>
			/// <seealso cref="getIndexInterval()"/>
			public virtual void setIndexInterval(int interval)
			{
				indexInterval = interval;
			}

			/// <summary>Sets the index interval and stores it in conf</summary>
			/// <seealso cref="getIndexInterval()"/>
			public static void setIndexInterval(org.apache.hadoop.conf.Configuration conf, int
				 interval)
			{
				conf.setInt(INDEX_INTERVAL, interval);
			}

			/// <summary>Close the map.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				lock (this)
				{
					data.close();
					index.close();
				}
			}

			/// <summary>Append a key/value pair to the map.</summary>
			/// <remarks>
			/// Append a key/value pair to the map.  The key must be greater or equal
			/// to the previous key added to the map.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.WritableComparable key, org.apache.hadoop.io.Writable
				 val)
			{
				lock (this)
				{
					checkKey(key);
					long pos = data.getLength();
					// Only write an index if we've changed positions. In a block compressed
					// file, this means we write an entry at the start of each block      
					if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos)
					{
						position.set(pos);
						// point to current eof
						index.append(key, position);
						lastIndexPos = pos;
						lastIndexKeyCount = size;
					}
					data.append(key, val);
					// append key/value to data
					size++;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void checkKey(org.apache.hadoop.io.WritableComparable key)
			{
				// check that keys are well-ordered
				if (size != 0 && comparator.compare(lastKey, key) > 0)
				{
					throw new System.IO.IOException("key out of order: " + key + " after " + lastKey);
				}
				// update lastKey with a copy of key by writing and reading
				outBuf.reset();
				key.write(outBuf);
				// write new key
				inBuf.reset(outBuf.getData(), outBuf.getLength());
				lastKey.readFields(inBuf);
			}
			// read into lastKey
		}

		/// <summary>Provide access to an existing map.</summary>
		public class Reader : java.io.Closeable
		{
			/// <summary>Number of index entries to skip between each entry.</summary>
			/// <remarks>
			/// Number of index entries to skip between each entry.  Zero by default.
			/// Setting this to values larger than zero can facilitate opening large map
			/// files using less memory.
			/// </remarks>
			private int INDEX_SKIP = 0;

			private org.apache.hadoop.io.WritableComparator comparator;

			private org.apache.hadoop.io.WritableComparable nextKey;

			private long seekPosition = -1;

			private int seekIndex = -1;

			private long firstPosition;

			private org.apache.hadoop.io.SequenceFile.Reader data;

			private org.apache.hadoop.io.SequenceFile.Reader index;

			private bool indexClosed = false;

			private int count = -1;

			private org.apache.hadoop.io.WritableComparable[] keys;

			private long[] positions;

			// the data, on disk
			// whether the index Reader was closed
			// the index, in memory
			/// <summary>Returns the class of keys in this file.</summary>
			public virtual java.lang.Class getKeyClass()
			{
				return data.getKeyClass();
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual java.lang.Class getValueClass()
			{
				return data.getValueClass();
			}

			public interface Option : org.apache.hadoop.io.SequenceFile.Reader.Option
			{
			}

			public static org.apache.hadoop.io.MapFile.Reader.Option comparator(org.apache.hadoop.io.WritableComparator
				 value)
			{
				return new org.apache.hadoop.io.MapFile.Reader.ComparatorOption(value);
			}

			internal class ComparatorOption : org.apache.hadoop.io.MapFile.Reader.Option
			{
				private readonly org.apache.hadoop.io.WritableComparator value;

				internal ComparatorOption(org.apache.hadoop.io.WritableComparator value)
				{
					this.value = value;
				}

				internal virtual org.apache.hadoop.io.WritableComparator getValue()
				{
					return value;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.Path dir, org.apache.hadoop.conf.Configuration
				 conf, params org.apache.hadoop.io.SequenceFile.Reader.Option[] opts)
			{
				org.apache.hadoop.io.MapFile.Reader.ComparatorOption comparatorOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.MapFile.Reader.ComparatorOption>(opts);
				org.apache.hadoop.io.WritableComparator comparator = comparatorOption == null ? null
					 : comparatorOption.getValue();
				INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
				open(dir, comparator, conf, opts);
			}

			/// <summary>Construct a map reader for the named map.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute]
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.conf.Configuration
				 conf)
				: this(new org.apache.hadoop.fs.Path(dirName), conf)
			{
			}

			/// <summary>Construct a map reader for the named map using the named comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute]
			public Reader(org.apache.hadoop.fs.FileSystem fs, string dirName, org.apache.hadoop.io.WritableComparator
				 comparator, org.apache.hadoop.conf.Configuration conf)
				: this(new org.apache.hadoop.fs.Path(dirName), conf, comparator(comparator))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void open(org.apache.hadoop.fs.Path dir, org.apache.hadoop.io.WritableComparator
				 comparator, org.apache.hadoop.conf.Configuration conf, params org.apache.hadoop.io.SequenceFile.Reader.Option
				[] options)
			{
				lock (this)
				{
					org.apache.hadoop.fs.Path dataFile = new org.apache.hadoop.fs.Path(dir, DATA_FILE_NAME
						);
					org.apache.hadoop.fs.Path indexFile = new org.apache.hadoop.fs.Path(dir, INDEX_FILE_NAME
						);
					// open the data
					this.data = createDataFileReader(dataFile, conf, options);
					this.firstPosition = data.getPosition();
					if (comparator == null)
					{
						java.lang.Class cls;
						cls = data.getKeyClass().asSubclass<org.apache.hadoop.io.WritableComparable>();
						this.comparator = org.apache.hadoop.io.WritableComparator.get(cls, conf);
					}
					else
					{
						this.comparator = comparator;
					}
					// open the index
					org.apache.hadoop.io.SequenceFile.Reader.Option[] indexOptions = org.apache.hadoop.util.Options
						.prependOptions(options, org.apache.hadoop.io.SequenceFile.Reader.file(indexFile
						));
					this.index = new org.apache.hadoop.io.SequenceFile.Reader(conf, indexOptions);
				}
			}

			/// <summary>
			/// Override this method to specialize the type of
			/// <see cref="Reader"/>
			/// returned.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual org.apache.hadoop.io.SequenceFile.Reader createDataFileReader
				(org.apache.hadoop.fs.Path dataFile, org.apache.hadoop.conf.Configuration conf, 
				params org.apache.hadoop.io.SequenceFile.Reader.Option[] options)
			{
				org.apache.hadoop.io.SequenceFile.Reader.Option[] newOptions = org.apache.hadoop.util.Options
					.prependOptions(options, org.apache.hadoop.io.SequenceFile.Reader.file(dataFile)
					);
				return new org.apache.hadoop.io.SequenceFile.Reader(conf, newOptions);
			}

			/// <exception cref="System.IO.IOException"/>
			private void readIndex()
			{
				// read the index entirely into memory
				if (this.keys != null)
				{
					return;
				}
				this.count = 0;
				this.positions = new long[1024];
				try
				{
					int skip = INDEX_SKIP;
					org.apache.hadoop.io.LongWritable position = new org.apache.hadoop.io.LongWritable
						();
					org.apache.hadoop.io.WritableComparable lastKey = null;
					long lastIndex = -1;
					System.Collections.Generic.List<org.apache.hadoop.io.WritableComparable> keyBuilder
						 = new System.Collections.Generic.List<org.apache.hadoop.io.WritableComparable>(
						1024);
					while (true)
					{
						org.apache.hadoop.io.WritableComparable k = comparator.newKey();
						if (!index.next(k, position))
						{
							break;
						}
						// check order to make sure comparator is compatible
						if (lastKey != null && comparator.compare(lastKey, k) > 0)
						{
							throw new System.IO.IOException("key out of order: " + k + " after " + lastKey);
						}
						lastKey = k;
						if (skip > 0)
						{
							skip--;
							continue;
						}
						else
						{
							// skip this entry
							skip = INDEX_SKIP;
						}
						// reset skip
						// don't read an index that is the same as the previous one. Block
						// compressed map files used to do this (multiple entries would point
						// at the same block)
						if (position.get() == lastIndex)
						{
							continue;
						}
						if (count == positions.Length)
						{
							positions = java.util.Arrays.copyOf(positions, positions.Length * 2);
						}
						keyBuilder.add(k);
						positions[count] = position.get();
						count++;
					}
					this.keys = Sharpen.Collections.ToArray(keyBuilder, new org.apache.hadoop.io.WritableComparable
						[count]);
					positions = java.util.Arrays.copyOf(positions, count);
				}
				catch (java.io.EOFException)
				{
					LOG.warn("Unexpected EOF reading " + index + " at entry #" + count + ".  Ignoring."
						);
				}
				finally
				{
					indexClosed = true;
					index.close();
				}
			}

			/// <summary>Re-positions the reader before its first key.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void reset()
			{
				lock (this)
				{
					data.seek(firstPosition);
				}
			}

			/// <summary>Get the key at approximately the middle of the file.</summary>
			/// <remarks>
			/// Get the key at approximately the middle of the file. Or null if the
			/// file is empty.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.WritableComparable midKey()
			{
				lock (this)
				{
					readIndex();
					if (count == 0)
					{
						return null;
					}
					return keys[(count - 1) / 2];
				}
			}

			/// <summary>Reads the final key from the file.</summary>
			/// <param name="key">key to read into</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void finalKey(org.apache.hadoop.io.WritableComparable key)
			{
				lock (this)
				{
					long originalPosition = data.getPosition();
					// save position
					try
					{
						readIndex();
						// make sure index is valid
						if (count > 0)
						{
							data.seek(positions[count - 1]);
						}
						else
						{
							// skip to last indexed entry
							reset();
						}
						// start at the beginning
						while (data.next(key))
						{
						}
					}
					finally
					{
						// scan to eof
						data.seek(originalPosition);
					}
				}
			}

			// restore position
			/// <summary>
			/// Positions the reader at the named key, or if none such exists, at the
			/// first entry after the named key.
			/// </summary>
			/// <remarks>
			/// Positions the reader at the named key, or if none such exists, at the
			/// first entry after the named key.  Returns true iff the named key exists
			/// in this map.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool seek(org.apache.hadoop.io.WritableComparable key)
			{
				lock (this)
				{
					return seekInternal(key) == 0;
				}
			}

			/// <summary>
			/// Positions the reader at the named key, or if none such exists, at the
			/// first entry after the named key.
			/// </summary>
			/// <returns>
			/// 0   - exact match found
			/// &lt; 0 - positioned at next record
			/// 1   - no more records in file
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private int seekInternal(org.apache.hadoop.io.WritableComparable key)
			{
				lock (this)
				{
					return seekInternal(key, false);
				}
			}

			/// <summary>
			/// Positions the reader at the named key, or if none such exists, at the
			/// key that falls just before or just after dependent on how the
			/// <code>before</code> parameter is set.
			/// </summary>
			/// <param name="before">
			/// - IF true, and <code>key</code> does not exist, position
			/// file at entry that falls just before <code>key</code>.  Otherwise,
			/// position file at record that sorts just after.
			/// </param>
			/// <returns>
			/// 0   - exact match found
			/// &lt; 0 - positioned at next record
			/// 1   - no more records in file
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private int seekInternal(org.apache.hadoop.io.WritableComparable key, bool before
				)
			{
				lock (this)
				{
					readIndex();
					// make sure index is read
					if (seekIndex != -1 && seekIndex + 1 < count && comparator.compare(key, keys[seekIndex
						 + 1]) < 0 && comparator.compare(key, nextKey) >= 0)
					{
					}
					else
					{
						// seeked before
						// before next indexed
						// but after last seeked
						// do nothing
						seekIndex = binarySearch(key);
						if (seekIndex < 0)
						{
							// decode insertion point
							seekIndex = -seekIndex - 2;
						}
						if (seekIndex == -1)
						{
							// belongs before first entry
							seekPosition = firstPosition;
						}
						else
						{
							// use beginning of file
							seekPosition = positions[seekIndex];
						}
					}
					// else use index
					data.seek(seekPosition);
					if (nextKey == null)
					{
						nextKey = comparator.newKey();
					}
					// If we're looking for the key before, we need to keep track
					// of the position we got the current key as well as the position
					// of the key before it.
					long prevPosition = -1;
					long curPosition = seekPosition;
					while (data.next(nextKey))
					{
						int c = comparator.compare(key, nextKey);
						if (c <= 0)
						{
							// at or beyond desired
							if (before && c != 0)
							{
								if (prevPosition == -1)
								{
									// We're on the first record of this index block
									// and we've already passed the search key. Therefore
									// we must be at the beginning of the file, so seek
									// to the beginning of this block and return c
									data.seek(curPosition);
								}
								else
								{
									// We have a previous record to back up to
									data.seek(prevPosition);
									data.next(nextKey);
									// now that we've rewound, the search key must be greater than this key
									return 1;
								}
							}
							return c;
						}
						if (before)
						{
							prevPosition = curPosition;
							curPosition = data.getPosition();
						}
					}
					return 1;
				}
			}

			private int binarySearch(org.apache.hadoop.io.WritableComparable key)
			{
				int low = 0;
				int high = count - 1;
				while (low <= high)
				{
					int mid = (int)(((uint)(low + high)) >> 1);
					org.apache.hadoop.io.WritableComparable midVal = keys[mid];
					int cmp = comparator.compare(midVal, key);
					if (cmp < 0)
					{
						low = mid + 1;
					}
					else
					{
						if (cmp > 0)
						{
							high = mid - 1;
						}
						else
						{
							return mid;
						}
					}
				}
				// key found
				return -(low + 1);
			}

			// key not found.
			/// <summary>
			/// Read the next key/value pair in the map into <code>key</code> and
			/// <code>val</code>.
			/// </summary>
			/// <remarks>
			/// Read the next key/value pair in the map into <code>key</code> and
			/// <code>val</code>.  Returns true if such a pair exists and false when at
			/// the end of the map
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool next(org.apache.hadoop.io.WritableComparable key, org.apache.hadoop.io.Writable
				 val)
			{
				lock (this)
				{
					return data.next(key, val);
				}
			}

			/// <summary>Return the value for the named key, or null if none exists.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.Writable get(org.apache.hadoop.io.WritableComparable
				 key, org.apache.hadoop.io.Writable val)
			{
				lock (this)
				{
					if (seek(key))
					{
						data.getCurrentValue(val);
						return val;
					}
					else
					{
						return null;
					}
				}
			}

			/// <summary>Finds the record that is the closest match to the specified key.</summary>
			/// <remarks>
			/// Finds the record that is the closest match to the specified key.
			/// Returns <code>key</code> or if it does not exist, at the first entry
			/// after the named key.
			/// -     * @param key       - key that we're trying to find
			/// -     * @param val       - data value if key is found
			/// -     * @return          - the key that was the closest match or null if eof.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.WritableComparable getClosest(org.apache.hadoop.io.WritableComparable
				 key, org.apache.hadoop.io.Writable val)
			{
				lock (this)
				{
					return getClosest(key, val, false);
				}
			}

			/// <summary>Finds the record that is the closest match to the specified key.</summary>
			/// <param name="key">- key that we're trying to find</param>
			/// <param name="val">- data value if key is found</param>
			/// <param name="before">
			/// - IF true, and <code>key</code> does not exist, return
			/// the first entry that falls just before the <code>key</code>.  Otherwise,
			/// return the record that sorts just after.
			/// </param>
			/// <returns>- the key that was the closest match or null if eof.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.WritableComparable getClosest(org.apache.hadoop.io.WritableComparable
				 key, org.apache.hadoop.io.Writable val, bool before)
			{
				lock (this)
				{
					int c = seekInternal(key, before);
					// If we didn't get an exact match, and we ended up in the wrong
					// direction relative to the query key, return null since we
					// must be at the beginning or end of the file.
					if ((!before && c > 0) || (before && c < 0))
					{
						return null;
					}
					data.getCurrentValue(val);
					return nextKey;
				}
			}

			/// <summary>Close the map.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				lock (this)
				{
					if (!indexClosed)
					{
						index.close();
					}
					data.close();
				}
			}
		}

		/// <summary>Renames an existing map directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void rename(org.apache.hadoop.fs.FileSystem fs, string oldName, string
			 newName)
		{
			org.apache.hadoop.fs.Path oldDir = new org.apache.hadoop.fs.Path(oldName);
			org.apache.hadoop.fs.Path newDir = new org.apache.hadoop.fs.Path(newName);
			if (!fs.rename(oldDir, newDir))
			{
				throw new System.IO.IOException("Could not rename " + oldDir + " to " + newDir);
			}
		}

		/// <summary>Deletes the named map file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void delete(org.apache.hadoop.fs.FileSystem fs, string name)
		{
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(name);
			org.apache.hadoop.fs.Path data = new org.apache.hadoop.fs.Path(dir, DATA_FILE_NAME
				);
			org.apache.hadoop.fs.Path index = new org.apache.hadoop.fs.Path(dir, INDEX_FILE_NAME
				);
			fs.delete(data, true);
			fs.delete(index, true);
			fs.delete(dir, true);
		}

		/// <summary>This method attempts to fix a corrupt MapFile by re-creating its index.</summary>
		/// <param name="fs">filesystem</param>
		/// <param name="dir">directory containing the MapFile data and index</param>
		/// <param name="keyClass">key class (has to be a subclass of Writable)</param>
		/// <param name="valueClass">value class (has to be a subclass of Writable)</param>
		/// <param name="dryrun">do not perform any changes, just report what needs to be done
		/// 	</param>
		/// <returns>number of valid entries in this MapFile, or -1 if no fixing was needed</returns>
		/// <exception cref="System.Exception"/>
		public static long fix(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 dir, java.lang.Class keyClass, java.lang.Class valueClass, bool dryrun, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string dr = (dryrun ? "[DRY RUN ] " : string.Empty);
			org.apache.hadoop.fs.Path data = new org.apache.hadoop.fs.Path(dir, DATA_FILE_NAME
				);
			org.apache.hadoop.fs.Path index = new org.apache.hadoop.fs.Path(dir, INDEX_FILE_NAME
				);
			int indexInterval = conf.getInt(org.apache.hadoop.io.MapFile.Writer.INDEX_INTERVAL
				, 128);
			if (!fs.exists(data))
			{
				// there's nothing we can do to fix this!
				throw new System.Exception(dr + "Missing data file in " + dir + ", impossible to fix this."
					);
			}
			if (fs.exists(index))
			{
				// no fixing needed
				return -1;
			}
			org.apache.hadoop.io.SequenceFile.Reader dataReader = new org.apache.hadoop.io.SequenceFile.Reader
				(conf, org.apache.hadoop.io.SequenceFile.Reader.file(data));
			if (!dataReader.getKeyClass().Equals(keyClass))
			{
				throw new System.Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass
					.getName() + ", got " + dataReader.getKeyClass().getName());
			}
			if (!dataReader.getValueClass().Equals(valueClass))
			{
				throw new System.Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass
					.getName() + ", got " + dataReader.getValueClass().getName());
			}
			long cnt = 0L;
			org.apache.hadoop.io.Writable key = org.apache.hadoop.util.ReflectionUtils.newInstance
				(keyClass, conf);
			org.apache.hadoop.io.Writable value = org.apache.hadoop.util.ReflectionUtils.newInstance
				(valueClass, conf);
			org.apache.hadoop.io.SequenceFile.Writer indexWriter = null;
			if (!dryrun)
			{
				indexWriter = org.apache.hadoop.io.SequenceFile.createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(index), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
					.valueClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
					))));
			}
			try
			{
				long pos = 0L;
				org.apache.hadoop.io.LongWritable position = new org.apache.hadoop.io.LongWritable
					();
				while (dataReader.next(key, value))
				{
					cnt++;
					if (cnt % indexInterval == 0)
					{
						position.set(pos);
						if (!dryrun)
						{
							indexWriter.append(key, position);
						}
					}
					pos = dataReader.getPosition();
				}
			}
			catch
			{
			}
			// truncated data file. swallow it.
			dataReader.close();
			if (!dryrun)
			{
				indexWriter.close();
			}
			return cnt;
		}

		/// <summary>Class to merge multiple MapFiles of same Key and Value types to one MapFile
		/// 	</summary>
		public class Merger
		{
			private org.apache.hadoop.conf.Configuration conf;

			private org.apache.hadoop.io.WritableComparator comparator = null;

			private org.apache.hadoop.io.MapFile.Reader[] inReaders;

			private org.apache.hadoop.io.MapFile.Writer outWriter;

			private java.lang.Class valueClass = null;

			private java.lang.Class keyClass = null;

			/// <exception cref="System.IO.IOException"/>
			public Merger(org.apache.hadoop.conf.Configuration conf)
			{
				this.conf = conf;
			}

			/// <summary>Merge multiple MapFiles to one Mapfile</summary>
			/// <param name="inMapFiles"/>
			/// <param name="outMapFile"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void merge(org.apache.hadoop.fs.Path[] inMapFiles, bool deleteInputs
				, org.apache.hadoop.fs.Path outMapFile)
			{
				try
				{
					open(inMapFiles, outMapFile);
					mergePass();
				}
				finally
				{
					close();
				}
				if (deleteInputs)
				{
					for (int i = 0; i < inMapFiles.Length; i++)
					{
						org.apache.hadoop.fs.Path path = inMapFiles[i];
						delete(path.getFileSystem(conf), path.ToString());
					}
				}
			}

			/*
			* Open all input files for reading and verify the key and value types. And
			* open Output file for writing
			*/
			/// <exception cref="System.IO.IOException"/>
			private void open(org.apache.hadoop.fs.Path[] inMapFiles, org.apache.hadoop.fs.Path
				 outMapFile)
			{
				inReaders = new org.apache.hadoop.io.MapFile.Reader[inMapFiles.Length];
				for (int i = 0; i < inMapFiles.Length; i++)
				{
					org.apache.hadoop.io.MapFile.Reader reader = new org.apache.hadoop.io.MapFile.Reader
						(inMapFiles[i], conf);
					if (keyClass == null || valueClass == null)
					{
						keyClass = (java.lang.Class)reader.getKeyClass();
						valueClass = (java.lang.Class)reader.getValueClass();
					}
					else
					{
						if (keyClass != reader.getKeyClass() || valueClass != reader.getValueClass())
						{
							throw new org.apache.hadoop.HadoopIllegalArgumentException("Input files cannot be merged as they"
								 + " have different Key and Value classes");
						}
					}
					inReaders[i] = reader;
				}
				if (comparator == null)
				{
					java.lang.Class cls;
					cls = keyClass.asSubclass<org.apache.hadoop.io.WritableComparable>();
					this.comparator = org.apache.hadoop.io.WritableComparator.get(cls, conf);
				}
				else
				{
					if (comparator.getKeyClass() != keyClass)
					{
						throw new org.apache.hadoop.HadoopIllegalArgumentException("Input files cannot be merged as they"
							 + " have different Key class compared to" + " specified comparator");
					}
				}
				outWriter = new org.apache.hadoop.io.MapFile.Writer(conf, outMapFile, org.apache.hadoop.io.MapFile.Writer
					.keyClass(keyClass), org.apache.hadoop.io.MapFile.Writer.valueClass(valueClass));
			}

			/// <summary>
			/// Merge all input files to output map file.<br />
			/// 1.
			/// </summary>
			/// <remarks>
			/// Merge all input files to output map file.<br />
			/// 1. Read first key/value from all input files to keys/values array. <br />
			/// 2. Select the least key and corresponding value. <br />
			/// 3. Write the selected key and value to output file. <br />
			/// 4. Replace the already written key/value in keys/values arrays with the
			/// next key/value from the selected input <br />
			/// 5. Repeat step 2-4 till all keys are read. <br />
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private void mergePass()
			{
				// re-usable array
				org.apache.hadoop.io.WritableComparable[] keys = new org.apache.hadoop.io.WritableComparable
					[inReaders.Length];
				org.apache.hadoop.io.Writable[] values = new org.apache.hadoop.io.Writable[inReaders
					.Length];
				// Read first key/value from all inputs
				for (int i = 0; i < inReaders.Length; i++)
				{
					keys[i] = org.apache.hadoop.util.ReflectionUtils.newInstance(keyClass, null);
					values[i] = org.apache.hadoop.util.ReflectionUtils.newInstance(valueClass, null);
					if (!inReaders[i].next(keys[i], values[i]))
					{
						// Handle empty files
						keys[i] = null;
						values[i] = null;
					}
				}
				do
				{
					int currentEntry = -1;
					org.apache.hadoop.io.WritableComparable currentKey = null;
					org.apache.hadoop.io.Writable currentValue = null;
					for (int i_1 = 0; i_1 < keys.Length; i_1++)
					{
						if (keys[i_1] == null)
						{
							// Skip Readers reached EOF
							continue;
						}
						if (currentKey == null || comparator.compare(currentKey, keys[i_1]) > 0)
						{
							currentEntry = i_1;
							currentKey = keys[i_1];
							currentValue = values[i_1];
						}
					}
					if (currentKey == null)
					{
						// Merge Complete
						break;
					}
					// Write the selected key/value to merge stream
					outWriter.append(currentKey, currentValue);
					// Replace the already written key/value in keys/values arrays with the
					// next key/value from the selected input
					if (!inReaders[currentEntry].next(keys[currentEntry], values[currentEntry]))
					{
						// EOF for this file
						keys[currentEntry] = null;
						values[currentEntry] = null;
					}
				}
				while (true);
			}

			/// <exception cref="System.IO.IOException"/>
			private void close()
			{
				for (int i = 0; i < inReaders.Length; i++)
				{
					org.apache.hadoop.io.IOUtils.closeStream(inReaders[i]);
					inReaders[i] = null;
				}
				if (outWriter != null)
				{
					outWriter.close();
					outWriter = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			string usage = "Usage: MapFile inFile outFile";
			if (args.Length != 2)
			{
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			string @in = args[0];
			string @out = args[1];
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf
				);
			org.apache.hadoop.io.MapFile.Reader reader = null;
			org.apache.hadoop.io.MapFile.Writer writer = null;
			try
			{
				reader = new org.apache.hadoop.io.MapFile.Reader(fs, @in, conf);
				writer = new org.apache.hadoop.io.MapFile.Writer(conf, fs, @out, reader.getKeyClass
					().asSubclass<org.apache.hadoop.io.WritableComparable>(), reader.getValueClass()
					);
				org.apache.hadoop.io.WritableComparable key = org.apache.hadoop.util.ReflectionUtils
					.newInstance(reader.getKeyClass().asSubclass<org.apache.hadoop.io.WritableComparable
					>(), conf);
				org.apache.hadoop.io.Writable value = org.apache.hadoop.util.ReflectionUtils.newInstance
					(reader.getValueClass().asSubclass<org.apache.hadoop.io.Writable>(), conf);
				while (reader.next(key, value))
				{
					// copy all entries
					writer.append(key, value);
				}
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, writer, reader);
			}
		}
	}
}
