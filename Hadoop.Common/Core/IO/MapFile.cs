using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A file-based map from keys to values.</summary>
	/// <remarks>
	/// A file-based map from keys to values.
	/// <p>A map is a directory containing two files, the <code>data</code> file,
	/// containing all keys and values in the map, and a smaller <code>index</code>
	/// file, containing a fraction of the keys.  The fraction is determined by
	/// <see cref="Writer.GetIndexInterval()"/>
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
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.MapFile
			));

		/// <summary>The name of the index file.</summary>
		public const string IndexFileName = "index";

		/// <summary>The name of the data file.</summary>
		public const string DataFileName = "data";

		protected internal MapFile()
		{
		}

		/// <summary>Writes a new map.</summary>
		public class Writer : IDisposable
		{
			private SequenceFile.Writer data;

			private SequenceFile.Writer index;

			private const string IndexInterval = "io.map.index.interval";

			private int indexInterval = 128;

			private long size;

			private LongWritable position = new LongWritable();

			private WritableComparator comparator;

			private DataInputBuffer inBuf = new DataInputBuffer();

			private DataOutputBuffer outBuf = new DataOutputBuffer();

			private WritableComparable lastKey;

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
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress, Org.Apache.Hadoop.Util.Progressable
				 progress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress), Progressable(progress))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress, CompressionCodec codec, Org.Apache.Hadoop.Util.Progressable
				 progress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress, codec), Progressable(progress))
			{
			}

			/// <summary>Create the named map for keys of the named class.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, Type keyClass, Type
				 valClass, SequenceFile.CompressionType compress)
				: this(conf, new Path(dirName), KeyClass(keyClass), ValueClass(valClass), Compression
					(compress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...)} instead."
				)]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress, Org.Apache.Hadoop.Util.Progressable
				 progress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress), Progressable(progress))
			{
			}

			/// <summary>Create the named map using the named key comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Writer(Configuration, Path, Option...) instead.")]
			public Writer(Configuration conf, FileSystem fs, string dirName, WritableComparator
				 comparator, Type valClass, SequenceFile.CompressionType compress, CompressionCodec
				 codec, Org.Apache.Hadoop.Util.Progressable progress)
				: this(conf, new Path(dirName), Comparator(comparator), ValueClass(valClass), Compression
					(compress, codec), Progressable(progress))
			{
			}

			public interface Option : SequenceFile.Writer.Option
			{
				// no public ctor
				// the following fields are used only for checking key order
				// our options are a superset of sequence file writer options
			}

			private class KeyClassOption : Options.ClassOption, MapFile.Writer.Option
			{
				internal KeyClassOption(Type value)
					: base(value)
				{
				}
			}

			private class ComparatorOption : MapFile.Writer.Option
			{
				private readonly WritableComparator value;

				internal ComparatorOption(WritableComparator value)
				{
					this.value = value;
				}

				internal virtual WritableComparator GetValue()
				{
					return value;
				}
			}

			public static MapFile.Writer.Option KeyClass(Type value)
			{
				return new MapFile.Writer.KeyClassOption(value);
			}

			public static MapFile.Writer.Option Comparator(WritableComparator value)
			{
				return new MapFile.Writer.ComparatorOption(value);
			}

			public static SequenceFile.Writer.Option ValueClass(Type value)
			{
				return SequenceFile.Writer.ValueClass(value);
			}

			public static SequenceFile.Writer.Option Compression(SequenceFile.CompressionType
				 type)
			{
				return SequenceFile.Writer.Compression(type);
			}

			public static SequenceFile.Writer.Option Compression(SequenceFile.CompressionType
				 type, CompressionCodec codec)
			{
				return SequenceFile.Writer.Compression(type, codec);
			}

			public static SequenceFile.Writer.Option Progressable(Progressable value)
			{
				return SequenceFile.Writer.Progressable(value);
			}

			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, Path dirName, params SequenceFile.Writer.Option
				[] opts)
			{
				MapFile.Writer.KeyClassOption keyClassOption = Options.GetOption<MapFile.Writer.KeyClassOption
					>(opts);
				MapFile.Writer.ComparatorOption comparatorOption = Options.GetOption<MapFile.Writer.ComparatorOption
					>(opts);
				if ((keyClassOption == null) == (comparatorOption == null))
				{
					throw new ArgumentException("key class or comparator option " + "must be set");
				}
				this.indexInterval = conf.GetInt(IndexInterval, this.indexInterval);
				Type keyClass;
				if (keyClassOption == null)
				{
					this.comparator = comparatorOption.GetValue();
					keyClass = comparator.GetKeyClass();
				}
				else
				{
					keyClass = (Type)keyClassOption.GetValue();
					this.comparator = WritableComparator.Get(keyClass, conf);
				}
				this.lastKey = comparator.NewKey();
				FileSystem fs = dirName.GetFileSystem(conf);
				if (!fs.Mkdirs(dirName))
				{
					throw new IOException("Mkdirs failed to create directory " + dirName);
				}
				Path dataFile = new Path(dirName, DataFileName);
				Path indexFile = new Path(dirName, IndexFileName);
				SequenceFile.Writer.Option[] dataOptions = Options.PrependOptions(opts, SequenceFile.Writer
					.File(dataFile), SequenceFile.Writer.KeyClass(keyClass));
				this.data = SequenceFile.CreateWriter(conf, dataOptions);
				SequenceFile.Writer.Option[] indexOptions = Options.PrependOptions(opts, SequenceFile.Writer
					.File(indexFile), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass
					(typeof(LongWritable)), SequenceFile.Writer.Compression(SequenceFile.CompressionType
					.Block));
				this.index = SequenceFile.CreateWriter(conf, indexOptions);
			}

			/// <summary>The number of entries that are added before an index entry is added.</summary>
			public virtual int GetIndexInterval()
			{
				return indexInterval;
			}

			/// <summary>Sets the index interval.</summary>
			/// <seealso cref="GetIndexInterval()"/>
			public virtual void SetIndexInterval(int interval)
			{
				indexInterval = interval;
			}

			/// <summary>Sets the index interval and stores it in conf</summary>
			/// <seealso cref="GetIndexInterval()"/>
			public static void SetIndexInterval(Configuration conf, int interval)
			{
				conf.SetInt(IndexInterval, interval);
			}

			/// <summary>Close the map.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				lock (this)
				{
					data.Close();
					index.Close();
				}
			}

			/// <summary>Append a key/value pair to the map.</summary>
			/// <remarks>
			/// Append a key/value pair to the map.  The key must be greater or equal
			/// to the previous key added to the map.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(WritableComparable key, IWritable val)
			{
				lock (this)
				{
					CheckKey(key);
					long pos = data.GetLength();
					// Only write an index if we've changed positions. In a block compressed
					// file, this means we write an entry at the start of each block      
					if (size >= lastIndexKeyCount + indexInterval && pos > lastIndexPos)
					{
						position.Set(pos);
						// point to current eof
						index.Append(key, position);
						lastIndexPos = pos;
						lastIndexKeyCount = size;
					}
					data.Append(key, val);
					// append key/value to data
					size++;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckKey(WritableComparable key)
			{
				// check that keys are well-ordered
				if (size != 0 && comparator.Compare(lastKey, key) > 0)
				{
					throw new IOException("key out of order: " + key + " after " + lastKey);
				}
				// update lastKey with a copy of key by writing and reading
				outBuf.Reset();
				key.Write(outBuf);
				// write new key
				inBuf.Reset(outBuf.GetData(), outBuf.GetLength());
				lastKey.ReadFields(inBuf);
			}
			// read into lastKey
		}

		/// <summary>Provide access to an existing map.</summary>
		public class Reader : IDisposable
		{
			/// <summary>Number of index entries to skip between each entry.</summary>
			/// <remarks>
			/// Number of index entries to skip between each entry.  Zero by default.
			/// Setting this to values larger than zero can facilitate opening large map
			/// files using less memory.
			/// </remarks>
			private int IndexSkip = 0;

			private WritableComparator comparator;

			private WritableComparable nextKey;

			private long seekPosition = -1;

			private int seekIndex = -1;

			private long firstPosition;

			private SequenceFile.Reader data;

			private SequenceFile.Reader index;

			private bool indexClosed = false;

			private int count = -1;

			private WritableComparable[] keys;

			private long[] positions;

			// the data, on disk
			// whether the index Reader was closed
			// the index, in memory
			/// <summary>Returns the class of keys in this file.</summary>
			public virtual Type GetKeyClass()
			{
				return data.GetKeyClass();
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual Type GetValueClass()
			{
				return data.GetValueClass();
			}

			public interface Option : SequenceFile.Reader.Option
			{
			}

			public static MapFile.Reader.Option Comparator(WritableComparator value)
			{
				return new MapFile.Reader.ComparatorOption(value);
			}

			internal class ComparatorOption : MapFile.Reader.Option
			{
				private readonly WritableComparator value;

				internal ComparatorOption(WritableComparator value)
				{
					this.value = value;
				}

				internal virtual WritableComparator GetValue()
				{
					return value;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public Reader(Path dir, Configuration conf, params SequenceFile.Reader.Option[] opts
				)
			{
				MapFile.Reader.ComparatorOption comparatorOption = Options.GetOption<MapFile.Reader.ComparatorOption
					>(opts);
				WritableComparator comparator = comparatorOption == null ? null : comparatorOption
					.GetValue();
				IndexSkip = conf.GetInt("io.map.index.skip", 0);
				Open(dir, comparator, conf, opts);
			}

			/// <summary>Construct a map reader for the named map.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute]
			public Reader(FileSystem fs, string dirName, Configuration conf)
				: this(new Path(dirName), conf)
			{
			}

			/// <summary>Construct a map reader for the named map using the named comparator.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute]
			public Reader(FileSystem fs, string dirName, WritableComparator comparator, Configuration
				 conf)
				: this(new Path(dirName), conf, Comparator(comparator))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void Open(Path dir, WritableComparator comparator, Configuration
				 conf, params SequenceFile.Reader.Option[] options)
			{
				lock (this)
				{
					Path dataFile = new Path(dir, DataFileName);
					Path indexFile = new Path(dir, IndexFileName);
					// open the data
					this.data = CreateDataFileReader(dataFile, conf, options);
					this.firstPosition = data.GetPosition();
					if (comparator == null)
					{
						Type cls;
						cls = data.GetKeyClass().AsSubclass<WritableComparable>();
						this.comparator = WritableComparator.Get(cls, conf);
					}
					else
					{
						this.comparator = comparator;
					}
					// open the index
					SequenceFile.Reader.Option[] indexOptions = Options.PrependOptions(options, SequenceFile.Reader
						.File(indexFile));
					this.index = new SequenceFile.Reader(conf, indexOptions);
				}
			}

			/// <summary>
			/// Override this method to specialize the type of
			/// <see cref="Reader"/>
			/// returned.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual SequenceFile.Reader CreateDataFileReader(Path dataFile
				, Configuration conf, params SequenceFile.Reader.Option[] options)
			{
				SequenceFile.Reader.Option[] newOptions = Options.PrependOptions(options, SequenceFile.Reader
					.File(dataFile));
				return new SequenceFile.Reader(conf, newOptions);
			}

			/// <exception cref="System.IO.IOException"/>
			private void ReadIndex()
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
					int skip = IndexSkip;
					LongWritable position = new LongWritable();
					WritableComparable lastKey = null;
					long lastIndex = -1;
					AList<WritableComparable> keyBuilder = new AList<WritableComparable>(1024);
					while (true)
					{
						WritableComparable k = comparator.NewKey();
						if (!index.Next(k, position))
						{
							break;
						}
						// check order to make sure comparator is compatible
						if (lastKey != null && comparator.Compare(lastKey, k) > 0)
						{
							throw new IOException("key out of order: " + k + " after " + lastKey);
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
							skip = IndexSkip;
						}
						// reset skip
						// don't read an index that is the same as the previous one. Block
						// compressed map files used to do this (multiple entries would point
						// at the same block)
						if (position.Get() == lastIndex)
						{
							continue;
						}
						if (count == positions.Length)
						{
							positions = Arrays.CopyOf(positions, positions.Length * 2);
						}
						keyBuilder.AddItem(k);
						positions[count] = position.Get();
						count++;
					}
					this.keys = Sharpen.Collections.ToArray(keyBuilder, new WritableComparable[count]
						);
					positions = Arrays.CopyOf(positions, count);
				}
				catch (EOFException)
				{
					Log.Warn("Unexpected EOF reading " + index + " at entry #" + count + ".  Ignoring."
						);
				}
				finally
				{
					indexClosed = true;
					index.Close();
				}
			}

			/// <summary>Re-positions the reader before its first key.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reset()
			{
				lock (this)
				{
					data.Seek(firstPosition);
				}
			}

			/// <summary>Get the key at approximately the middle of the file.</summary>
			/// <remarks>
			/// Get the key at approximately the middle of the file. Or null if the
			/// file is empty.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual WritableComparable MidKey()
			{
				lock (this)
				{
					ReadIndex();
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
			public virtual void FinalKey(WritableComparable key)
			{
				lock (this)
				{
					long originalPosition = data.GetPosition();
					// save position
					try
					{
						ReadIndex();
						// make sure index is valid
						if (count > 0)
						{
							data.Seek(positions[count - 1]);
						}
						else
						{
							// skip to last indexed entry
							Reset();
						}
						// start at the beginning
						while (data.Next(key))
						{
						}
					}
					finally
					{
						// scan to eof
						data.Seek(originalPosition);
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
			public virtual bool Seek(WritableComparable key)
			{
				lock (this)
				{
					return SeekInternal(key) == 0;
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
			private int SeekInternal(WritableComparable key)
			{
				lock (this)
				{
					return SeekInternal(key, false);
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
			private int SeekInternal(WritableComparable key, bool before)
			{
				lock (this)
				{
					ReadIndex();
					// make sure index is read
					if (seekIndex != -1 && seekIndex + 1 < count && comparator.Compare(key, keys[seekIndex
						 + 1]) < 0 && comparator.Compare(key, nextKey) >= 0)
					{
					}
					else
					{
						// seeked before
						// before next indexed
						// but after last seeked
						// do nothing
						seekIndex = BinarySearch(key);
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
					data.Seek(seekPosition);
					if (nextKey == null)
					{
						nextKey = comparator.NewKey();
					}
					// If we're looking for the key before, we need to keep track
					// of the position we got the current key as well as the position
					// of the key before it.
					long prevPosition = -1;
					long curPosition = seekPosition;
					while (data.Next(nextKey))
					{
						int c = comparator.Compare(key, nextKey);
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
									data.Seek(curPosition);
								}
								else
								{
									// We have a previous record to back up to
									data.Seek(prevPosition);
									data.Next(nextKey);
									// now that we've rewound, the search key must be greater than this key
									return 1;
								}
							}
							return c;
						}
						if (before)
						{
							prevPosition = curPosition;
							curPosition = data.GetPosition();
						}
					}
					return 1;
				}
			}

			private int BinarySearch(WritableComparable key)
			{
				int low = 0;
				int high = count - 1;
				while (low <= high)
				{
					int mid = (int)(((uint)(low + high)) >> 1);
					WritableComparable midVal = keys[mid];
					int cmp = comparator.Compare(midVal, key);
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
			public virtual bool Next(WritableComparable key, IWritable val)
			{
				lock (this)
				{
					return data.Next(key, val);
				}
			}

			/// <summary>Return the value for the named key, or null if none exists.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual IWritable Get(WritableComparable key, IWritable val)
			{
				lock (this)
				{
					if (Seek(key))
					{
						data.GetCurrentValue(val);
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
			public virtual WritableComparable GetClosest(WritableComparable key, IWritable val
				)
			{
				lock (this)
				{
					return GetClosest(key, val, false);
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
			public virtual WritableComparable GetClosest(WritableComparable key, IWritable val
				, bool before)
			{
				lock (this)
				{
					int c = SeekInternal(key, before);
					// If we didn't get an exact match, and we ended up in the wrong
					// direction relative to the query key, return null since we
					// must be at the beginning or end of the file.
					if ((!before && c > 0) || (before && c < 0))
					{
						return null;
					}
					data.GetCurrentValue(val);
					return nextKey;
				}
			}

			/// <summary>Close the map.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				lock (this)
				{
					if (!indexClosed)
					{
						index.Close();
					}
					data.Close();
				}
			}
		}

		/// <summary>Renames an existing map directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Rename(FileSystem fs, string oldName, string newName)
		{
			Path oldDir = new Path(oldName);
			Path newDir = new Path(newName);
			if (!fs.Rename(oldDir, newDir))
			{
				throw new IOException("Could not rename " + oldDir + " to " + newDir);
			}
		}

		/// <summary>Deletes the named map file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Delete(FileSystem fs, string name)
		{
			Path dir = new Path(name);
			Path data = new Path(dir, DataFileName);
			Path index = new Path(dir, IndexFileName);
			fs.Delete(data, true);
			fs.Delete(index, true);
			fs.Delete(dir, true);
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
		public static long Fix(FileSystem fs, Path dir, Type keyClass, Type valueClass, bool
			 dryrun, Configuration conf)
		{
			string dr = (dryrun ? "[DRY RUN ] " : string.Empty);
			Path data = new Path(dir, DataFileName);
			Path index = new Path(dir, IndexFileName);
			int indexInterval = conf.GetInt(MapFile.Writer.IndexInterval, 128);
			if (!fs.Exists(data))
			{
				// there's nothing we can do to fix this!
				throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this."
					);
			}
			if (fs.Exists(index))
			{
				// no fixing needed
				return -1;
			}
			SequenceFile.Reader dataReader = new SequenceFile.Reader(conf, SequenceFile.Reader
				.File(data));
			if (!dataReader.GetKeyClass().Equals(keyClass))
			{
				throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.FullName
					 + ", got " + dataReader.GetKeyClass().FullName);
			}
			if (!dataReader.GetValueClass().Equals(valueClass))
			{
				throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass
					.FullName + ", got " + dataReader.GetValueClass().FullName);
			}
			long cnt = 0L;
			IWritable key = ReflectionUtils.NewInstance(keyClass, conf);
			IWritable value = ReflectionUtils.NewInstance(valueClass, conf);
			SequenceFile.Writer indexWriter = null;
			if (!dryrun)
			{
				indexWriter = SequenceFile.CreateWriter(conf, SequenceFile.Writer.File(index), SequenceFile.Writer
					.KeyClass(keyClass), SequenceFile.Writer.ValueClass(typeof(LongWritable)));
			}
			try
			{
				long pos = 0L;
				LongWritable position = new LongWritable();
				while (dataReader.Next(key, value))
				{
					cnt++;
					if (cnt % indexInterval == 0)
					{
						position.Set(pos);
						if (!dryrun)
						{
							indexWriter.Append(key, position);
						}
					}
					pos = dataReader.GetPosition();
				}
			}
			catch
			{
			}
			// truncated data file. swallow it.
			dataReader.Close();
			if (!dryrun)
			{
				indexWriter.Close();
			}
			return cnt;
		}

		/// <summary>Class to merge multiple MapFiles of same Key and Value types to one MapFile
		/// 	</summary>
		public class Merger
		{
			private Configuration conf;

			private WritableComparator comparator = null;

			private MapFile.Reader[] inReaders;

			private MapFile.Writer outWriter;

			private Type valueClass = null;

			private Type keyClass = null;

			/// <exception cref="System.IO.IOException"/>
			public Merger(Configuration conf)
			{
				this.conf = conf;
			}

			/// <summary>Merge multiple MapFiles to one Mapfile</summary>
			/// <param name="inMapFiles"/>
			/// <param name="outMapFile"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Merge(Path[] inMapFiles, bool deleteInputs, Path outMapFile)
			{
				try
				{
					Open(inMapFiles, outMapFile);
					MergePass();
				}
				finally
				{
					Close();
				}
				if (deleteInputs)
				{
					for (int i = 0; i < inMapFiles.Length; i++)
					{
						Path path = inMapFiles[i];
						Delete(path.GetFileSystem(conf), path.ToString());
					}
				}
			}

			/*
			* Open all input files for reading and verify the key and value types. And
			* open Output file for writing
			*/
			/// <exception cref="System.IO.IOException"/>
			private void Open(Path[] inMapFiles, Path outMapFile)
			{
				inReaders = new MapFile.Reader[inMapFiles.Length];
				for (int i = 0; i < inMapFiles.Length; i++)
				{
					MapFile.Reader reader = new MapFile.Reader(inMapFiles[i], conf);
					if (keyClass == null || valueClass == null)
					{
						keyClass = (Type)reader.GetKeyClass();
						valueClass = (Type)reader.GetValueClass();
					}
					else
					{
						if (keyClass != reader.GetKeyClass() || valueClass != reader.GetValueClass())
						{
							throw new HadoopIllegalArgumentException("Input files cannot be merged as they" +
								 " have different Key and Value classes");
						}
					}
					inReaders[i] = reader;
				}
				if (comparator == null)
				{
					Type cls;
					cls = keyClass.AsSubclass<WritableComparable>();
					this.comparator = WritableComparator.Get(cls, conf);
				}
				else
				{
					if (comparator.GetKeyClass() != keyClass)
					{
						throw new HadoopIllegalArgumentException("Input files cannot be merged as they" +
							 " have different Key class compared to" + " specified comparator");
					}
				}
				outWriter = new MapFile.Writer(conf, outMapFile, MapFile.Writer.KeyClass(keyClass
					), MapFile.Writer.ValueClass(valueClass));
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
			private void MergePass()
			{
				// re-usable array
				WritableComparable[] keys = new WritableComparable[inReaders.Length];
				IWritable[] values = new IWritable[inReaders.Length];
				// Read first key/value from all inputs
				for (int i = 0; i < inReaders.Length; i++)
				{
					keys[i] = ReflectionUtils.NewInstance(keyClass, null);
					values[i] = ReflectionUtils.NewInstance(valueClass, null);
					if (!inReaders[i].Next(keys[i], values[i]))
					{
						// Handle empty files
						keys[i] = null;
						values[i] = null;
					}
				}
				do
				{
					int currentEntry = -1;
					WritableComparable currentKey = null;
					IWritable currentValue = null;
					for (int i_1 = 0; i_1 < keys.Length; i_1++)
					{
						if (keys[i_1] == null)
						{
							// Skip Readers reached EOF
							continue;
						}
						if (currentKey == null || comparator.Compare(currentKey, keys[i_1]) > 0)
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
					outWriter.Append(currentKey, currentValue);
					// Replace the already written key/value in keys/values arrays with the
					// next key/value from the selected input
					if (!inReaders[currentEntry].Next(keys[currentEntry], values[currentEntry]))
					{
						// EOF for this file
						keys[currentEntry] = null;
						values[currentEntry] = null;
					}
				}
				while (true);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Close()
			{
				for (int i = 0; i < inReaders.Length; i++)
				{
					IOUtils.CloseStream(inReaders[i]);
					inReaders[i] = null;
				}
				if (outWriter != null)
				{
					outWriter.Close();
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
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			MapFile.Reader reader = null;
			MapFile.Writer writer = null;
			try
			{
				reader = new MapFile.Reader(fs, @in, conf);
				writer = new MapFile.Writer(conf, fs, @out, reader.GetKeyClass().AsSubclass<WritableComparable
					>(), reader.GetValueClass());
				WritableComparable key = ReflectionUtils.NewInstance(reader.GetKeyClass().AsSubclass
					<WritableComparable>(), conf);
				IWritable value = ReflectionUtils.NewInstance(reader.GetValueClass().AsSubclass<IWritable
					>(), conf);
				while (reader.Next(key, value))
				{
					// copy all entries
					writer.Append(key, value);
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, writer, reader);
			}
		}
	}
}
