using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>A TFile is a container of key-value pairs.</summary>
	/// <remarks>
	/// A TFile is a container of key-value pairs. Both keys and values are type-less
	/// bytes. Keys are restricted to 64KB, value length is not restricted
	/// (practically limited to the available disk storage). TFile further provides
	/// the following features:
	/// <ul>
	/// <li>Block Compression.
	/// <li>Named meta data blocks.
	/// <li>Sorted or unsorted keys.
	/// <li>Seek by key or by file offset.
	/// </ul>
	/// The memory footprint of a TFile includes the following:
	/// <ul>
	/// <li>Some constant overhead of reading or writing a compressed block.
	/// <ul>
	/// <li>Each compressed block requires one compression/decompression codec for
	/// I/O.
	/// <li>Temporary space to buffer the key.
	/// <li>Temporary space to buffer the value (for TFile.Writer only). Values are
	/// chunk encoded, so that we buffer at most one chunk of user data. By default,
	/// the chunk buffer is 1MB. Reading chunked value does not require additional
	/// memory.
	/// </ul>
	/// <li>TFile index, which is proportional to the total number of Data Blocks.
	/// The total amount of memory needed to hold the index can be estimated as
	/// (56+AvgKeySize)*NumBlocks.
	/// <li>MetaBlock index, which is proportional to the total number of Meta
	/// Blocks.The total amount of memory needed to hold the index for Meta Blocks
	/// can be estimated as (40+AvgMetaBlockName)*NumMetaBlock.
	/// </ul>
	/// <p>
	/// The behavior of TFile can be customized by the following variables through
	/// Configuration:
	/// <ul>
	/// <li><b>tfile.io.chunk.size</b>: Value chunk size. Integer (in bytes). Default
	/// to 1MB. Values of the length less than the chunk size is guaranteed to have
	/// known value length in read time (See
	/// <see cref="Entry.IsValueLengthKnown()"/>
	/// ).
	/// <li><b>tfile.fs.output.buffer.size</b>: Buffer size used for
	/// FSDataOutputStream. Integer (in bytes). Default to 256KB.
	/// <li><b>tfile.fs.input.buffer.size</b>: Buffer size used for
	/// FSDataInputStream. Integer (in bytes). Default to 256KB.
	/// </ul>
	/// <p>
	/// Suggestions on performance optimization.
	/// <ul>
	/// <li>Minimum block size. We recommend a setting of minimum block size between
	/// 256KB to 1MB for general usage. Larger block size is preferred if files are
	/// primarily for sequential access. However, it would lead to inefficient random
	/// access (because there are more data to decompress). Smaller blocks are good
	/// for random access, but require more memory to hold the block index, and may
	/// be slower to create (because we must flush the compressor stream at the
	/// conclusion of each data block, which leads to an FS I/O flush). Further, due
	/// to the internal caching in Compression codec, the smallest possible block
	/// size would be around 20KB-30KB.
	/// <li>The current implementation does not offer true multi-threading for
	/// reading. The implementation uses FSDataInputStream seek()+read(), which is
	/// shown to be much faster than positioned-read call in single thread mode.
	/// However, it also means that if multiple threads attempt to access the same
	/// TFile (using multiple scanners) simultaneously, the actual I/O is carried out
	/// sequentially even if they access different DFS blocks.
	/// <li>Compression codec. Use "none" if the data is not very compressable (by
	/// compressable, I mean a compression ratio at least 2:1). Generally, use "lzo"
	/// as the starting point for experimenting. "gz" overs slightly better
	/// compression ratio over "lzo" but requires 4x CPU to compress and 2x CPU to
	/// decompress, comparing to "lzo".
	/// <li>File system buffering, if the underlying FSDataInputStream and
	/// FSDataOutputStream is already adequately buffered; or if applications
	/// reads/writes keys and values in large buffers, we can reduce the sizes of
	/// input/output buffering in TFile layer by setting the configuration parameters
	/// "tfile.fs.input.buffer.size" and "tfile.fs.output.buffer.size".
	/// </ul>
	/// Some design rationale behind TFile can be found at &lt;a
	/// href=https://issues.apache.org/jira/browse/HADOOP-3315&gt;Hadoop-3315</a>.
	/// </remarks>
	public class TFile
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.File.Tfile.TFile
			));

		private const string ChunkBufSizeAttr = "tfile.io.chunk.size";

		private const string FsInputBufSizeAttr = "tfile.fs.input.buffer.size";

		private const string FsOutputBufSizeAttr = "tfile.fs.output.buffer.size";

		internal static int GetChunkBufferSize(Configuration conf)
		{
			int ret = conf.GetInt(ChunkBufSizeAttr, 1024 * 1024);
			return (ret > 0) ? ret : 1024 * 1024;
		}

		internal static int GetFSInputBufferSize(Configuration conf)
		{
			return conf.GetInt(FsInputBufSizeAttr, 256 * 1024);
		}

		internal static int GetFSOutputBufferSize(Configuration conf)
		{
			return conf.GetInt(FsOutputBufSizeAttr, 256 * 1024);
		}

		private const int MaxKeySize = 64 * 1024;

		internal static readonly Utils.Version ApiVersion = new Utils.Version((short)1, (
			short)0);

		/// <summary>compression: gzip</summary>
		public const string CompressionGz = "gz";

		/// <summary>compression: lzo</summary>
		public const string CompressionLzo = "lzo";

		/// <summary>compression: none</summary>
		public const string CompressionNone = "none";

		/// <summary>comparator: memcmp</summary>
		public const string ComparatorMemcmp = "memcmp";

		/// <summary>comparator prefix: java class</summary>
		public const string ComparatorJclass = "jclass:";

		// 64KB
		/// <summary>Make a raw comparator from a string name.</summary>
		/// <param name="name">Comparator name</param>
		/// <returns>A RawComparable comparator.</returns>
		public static IComparer<RawComparable> MakeComparator(string name)
		{
			return TFile.TFileMeta.MakeComparator(name);
		}

		private TFile()
		{
		}

		// Prevent the instantiation of TFiles
		// nothing
		/// <summary>Get names of supported compression algorithms.</summary>
		/// <remarks>
		/// Get names of supported compression algorithms. The names are acceptable by
		/// TFile.Writer.
		/// </remarks>
		/// <returns>
		/// Array of strings, each represents a supported compression
		/// algorithm. Currently, the following compression algorithms are
		/// supported.
		/// <ul>
		/// <li>"none" - No compression.
		/// <li>"lzo" - LZO compression.
		/// <li>"gz" - GZIP compression.
		/// </ul>
		/// </returns>
		public static string[] GetSupportedCompressionAlgorithms()
		{
			return Compression.GetSupportedAlgorithms();
		}

		/// <summary>TFile Writer.</summary>
		public class Writer : IDisposable
		{
			private readonly int sizeMinBlock;

			internal readonly TFile.TFileIndex tfileIndex;

			internal readonly TFile.TFileMeta tfileMeta;

			private BCFile.Writer writerBCF;

			internal BCFile.Writer.BlockAppender blkAppender;

			internal long blkRecordCount;

			internal BoundedByteArrayOutputStream currentKeyBufferOS;

			internal BoundedByteArrayOutputStream lastKeyBufferOS;

			private byte[] valueBuffer;

			/// <summary>Writer states.</summary>
			/// <remarks>
			/// Writer states. The state always transits in circles: READY -&gt; IN_KEY -&gt;
			/// END_KEY -&gt; IN_VALUE -&gt; READY.
			/// </remarks>
			private enum State
			{
				Ready,
				InKey,
				EndKey,
				InValue,
				Closed
			}

			internal TFile.Writer.State state = TFile.Writer.State.Ready;

			internal Configuration conf;

			internal long errorCount = 0;

			/// <summary>Constructor</summary>
			/// <param name="fsdos">output stream for writing. Must be at position 0.</param>
			/// <param name="minBlockSize">
			/// Minimum compressed block size in bytes. A compression block will
			/// not be closed until it reaches this size except for the last
			/// block.
			/// </param>
			/// <param name="compressName">
			/// Name of the compression algorithm. Must be one of the strings
			/// returned by
			/// <see cref="TFile.GetSupportedCompressionAlgorithms()"/>
			/// .
			/// </param>
			/// <param name="comparator">
			/// Leave comparator as null or empty string if TFile is not sorted.
			/// Otherwise, provide the string name for the comparison algorithm
			/// for keys. Two kinds of comparators are supported.
			/// <ul>
			/// <li>Algorithmic comparator: binary comparators that is language
			/// independent. Currently, only "memcmp" is supported.
			/// <li>Language-specific comparator: binary comparators that can
			/// only be constructed in specific language. For Java, the syntax
			/// is "jclass:", followed by the class name of the RawComparator.
			/// Currently, we only support RawComparators that can be
			/// constructed through the default constructor (with no
			/// parameters). Parameterized RawComparators such as
			/// <see cref="Org.Apache.Hadoop.IO.WritableComparator"/>
			/// or
			/// <see cref="Org.Apache.Hadoop.IO.Serializer.JavaSerializationComparator{T}"/>
			/// may not be directly used.
			/// One should write a wrapper class that inherits from such classes
			/// and use its default constructor to perform proper
			/// initialization.
			/// </ul>
			/// </param>
			/// <param name="conf">The configuration object.</param>
			/// <exception cref="System.IO.IOException"/>
			public Writer(FSDataOutputStream fsdos, int minBlockSize, string compressName, string
				 comparator, Configuration conf)
			{
				// minimum compressed size for a block.
				// Meta blocks.
				// reference to the underlying BCFile.
				// current data block appender.
				// buffers for caching the key.
				// buffer used by chunk codec
				// Ready to start a new key-value pair insertion.
				// In the middle of key insertion.
				// Key insertion complete, ready to insert value.
				// In value insertion.
				// ERROR, // Error encountered, cannot continue.
				// TFile already closed.
				// current state of Writer.
				sizeMinBlock = minBlockSize;
				tfileMeta = new TFile.TFileMeta(comparator);
				tfileIndex = new TFile.TFileIndex(tfileMeta.GetComparator());
				writerBCF = new BCFile.Writer(fsdos, compressName, conf);
				currentKeyBufferOS = new BoundedByteArrayOutputStream(MaxKeySize);
				lastKeyBufferOS = new BoundedByteArrayOutputStream(MaxKeySize);
				this.conf = conf;
			}

			/// <summary>Close the Writer.</summary>
			/// <remarks>
			/// Close the Writer. Resources will be released regardless of the exceptions
			/// being thrown. Future close calls will have no effect.
			/// The underlying FSDataOutputStream is not closed.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				if ((state == TFile.Writer.State.Closed))
				{
					return;
				}
				try
				{
					// First try the normal finish.
					// Terminate upon the first Exception.
					if (errorCount == 0)
					{
						if (state != TFile.Writer.State.Ready)
						{
							throw new InvalidOperationException("Cannot close TFile in the middle of key-value insertion."
								);
						}
						FinishDataBlock(true);
						// first, write out data:TFile.meta
						BCFile.Writer.BlockAppender outMeta = writerBCF.PrepareMetaBlock(TFile.TFileMeta.
							BlockName, CompressionNone);
						try
						{
							tfileMeta.Write(outMeta);
						}
						finally
						{
							outMeta.Close();
						}
						// second, write out data:TFile.index
						BCFile.Writer.BlockAppender outIndex = writerBCF.PrepareMetaBlock(TFile.TFileIndex
							.BlockName);
						try
						{
							tfileIndex.Write(outIndex);
						}
						finally
						{
							outIndex.Close();
						}
						writerBCF.Close();
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, blkAppender, writerBCF);
					blkAppender = null;
					writerBCF = null;
					state = TFile.Writer.State.Closed;
				}
			}

			/// <summary>Adding a new key-value pair to the TFile.</summary>
			/// <remarks>
			/// Adding a new key-value pair to the TFile. This is synonymous to
			/// append(key, 0, key.length, value, 0, value.length)
			/// </remarks>
			/// <param name="key">Buffer for key.</param>
			/// <param name="value">Buffer for value.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(byte[] key, byte[] value)
			{
				Append(key, 0, key.Length, value, 0, value.Length);
			}

			/// <summary>Adding a new key-value pair to TFile.</summary>
			/// <param name="key">buffer for key.</param>
			/// <param name="koff">offset in key buffer.</param>
			/// <param name="klen">length of key.</param>
			/// <param name="value">buffer for value.</param>
			/// <param name="voff">offset in value buffer.</param>
			/// <param name="vlen">length of value.</param>
			/// <exception cref="System.IO.IOException">
			/// Upon IO errors.
			/// <p>
			/// If an exception is thrown, the TFile will be in an inconsistent
			/// state. The only legitimate call after that would be close
			/// </exception>
			public virtual void Append(byte[] key, int koff, int klen, byte[] value, int voff
				, int vlen)
			{
				if ((koff | klen | (koff + klen) | (key.Length - (koff + klen))) < 0)
				{
					throw new IndexOutOfRangeException("Bad key buffer offset-length combination.");
				}
				if ((voff | vlen | (voff + vlen) | (value.Length - (voff + vlen))) < 0)
				{
					throw new IndexOutOfRangeException("Bad value buffer offset-length combination.");
				}
				try
				{
					DataOutputStream dosKey = PrepareAppendKey(klen);
					try
					{
						++errorCount;
						dosKey.Write(key, koff, klen);
						--errorCount;
					}
					finally
					{
						dosKey.Close();
					}
					DataOutputStream dosValue = PrepareAppendValue(vlen);
					try
					{
						++errorCount;
						dosValue.Write(value, voff, vlen);
						--errorCount;
					}
					finally
					{
						dosValue.Close();
					}
				}
				finally
				{
					state = TFile.Writer.State.Ready;
				}
			}

			/// <summary>Helper class to register key after close call on key append stream.</summary>
			private class KeyRegister : DataOutputStream
			{
				private readonly int expectedLength;

				private bool closed = false;

				public KeyRegister(Writer _enclosing, int len)
					: base(this._enclosing.currentKeyBufferOS)
				{
					this._enclosing = _enclosing;
					if (len >= 0)
					{
						this._enclosing.currentKeyBufferOS.Reset(len);
					}
					else
					{
						this._enclosing.currentKeyBufferOS.Reset();
					}
					this.expectedLength = len;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					if (this.closed == true)
					{
						return;
					}
					try
					{
						++this._enclosing.errorCount;
						byte[] key = this._enclosing.currentKeyBufferOS.GetBuffer();
						int len = this._enclosing.currentKeyBufferOS.Size();
						if (this.expectedLength >= 0 && this.expectedLength != len)
						{
							throw new IOException("Incorrect key length: expected=" + this.expectedLength + " actual="
								 + len);
						}
						Utils.WriteVInt(this._enclosing.blkAppender, len);
						this._enclosing.blkAppender.Write(key, 0, len);
						if (this._enclosing.tfileIndex.GetFirstKey() == null)
						{
							this._enclosing.tfileIndex.SetFirstKey(key, 0, len);
						}
						if (this._enclosing.tfileMeta.IsSorted() && this._enclosing.tfileMeta.GetRecordCount
							() > 0)
						{
							byte[] lastKey = this._enclosing.lastKeyBufferOS.GetBuffer();
							int lastLen = this._enclosing.lastKeyBufferOS.Size();
							if (this._enclosing.tfileMeta.GetComparator().Compare(key, 0, len, lastKey, 0, lastLen
								) < 0)
							{
								throw new IOException("Keys are not added in sorted order");
							}
						}
						BoundedByteArrayOutputStream tmp = this._enclosing.currentKeyBufferOS;
						this._enclosing.currentKeyBufferOS = this._enclosing.lastKeyBufferOS;
						this._enclosing.lastKeyBufferOS = tmp;
						--this._enclosing.errorCount;
					}
					finally
					{
						this.closed = true;
						this._enclosing.state = TFile.Writer.State.EndKey;
					}
				}

				private readonly Writer _enclosing;
			}

			/// <summary>Helper class to register value after close call on value append stream.</summary>
			private class ValueRegister : DataOutputStream
			{
				private bool closed = false;

				public ValueRegister(Writer _enclosing, OutputStream os)
					: base(os)
				{
					this._enclosing = _enclosing;
				}

				// Avoiding flushing call to down stream.
				public override void Flush()
				{
				}

				// do nothing
				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					if (this.closed == true)
					{
						return;
					}
					try
					{
						++this._enclosing.errorCount;
						base.Close();
						this._enclosing.blkRecordCount++;
						// bump up the total record count in the whole file
						this._enclosing.tfileMeta.IncRecordCount();
						this._enclosing.FinishDataBlock(false);
						--this._enclosing.errorCount;
					}
					finally
					{
						this.closed = true;
						this._enclosing.state = TFile.Writer.State.Ready;
					}
				}

				private readonly Writer _enclosing;
			}

			/// <summary>Obtain an output stream for writing a key into TFile.</summary>
			/// <remarks>
			/// Obtain an output stream for writing a key into TFile. This may only be
			/// called when there is no active Key appending stream or value appending
			/// stream.
			/// </remarks>
			/// <param name="length">
			/// The expected length of the key. If length of the key is not
			/// known, set length = -1. Otherwise, the application must write
			/// exactly as many bytes as specified here before calling close on
			/// the returned output stream.
			/// </param>
			/// <returns>The key appending output stream.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual DataOutputStream PrepareAppendKey(int length)
			{
				if (state != TFile.Writer.State.Ready)
				{
					throw new InvalidOperationException("Incorrect state to start a new key: " + state
						.ToString());
				}
				InitDataBlock();
				DataOutputStream ret = new TFile.Writer.KeyRegister(this, length);
				state = TFile.Writer.State.InKey;
				return ret;
			}

			/// <summary>Obtain an output stream for writing a value into TFile.</summary>
			/// <remarks>
			/// Obtain an output stream for writing a value into TFile. This may only be
			/// called right after a key appending operation (the key append stream must
			/// be closed).
			/// </remarks>
			/// <param name="length">
			/// The expected length of the value. If length of the value is not
			/// known, set length = -1. Otherwise, the application must write
			/// exactly as many bytes as specified here before calling close on
			/// the returned output stream. Advertising the value size up-front
			/// guarantees that the value is encoded in one chunk, and avoids
			/// intermediate chunk buffering.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			public virtual DataOutputStream PrepareAppendValue(int length)
			{
				if (state != TFile.Writer.State.EndKey)
				{
					throw new InvalidOperationException("Incorrect state to start a new value: " + state
						.ToString());
				}
				DataOutputStream ret;
				// unknown length
				if (length < 0)
				{
					if (valueBuffer == null)
					{
						valueBuffer = new byte[GetChunkBufferSize(conf)];
					}
					ret = new TFile.Writer.ValueRegister(this, new Chunk.ChunkEncoder(blkAppender, valueBuffer
						));
				}
				else
				{
					ret = new TFile.Writer.ValueRegister(this, new Chunk.SingleChunkEncoder(blkAppender
						, length));
				}
				state = TFile.Writer.State.InValue;
				return ret;
			}

			/// <summary>Obtain an output stream for creating a meta block.</summary>
			/// <remarks>
			/// Obtain an output stream for creating a meta block. This function may not
			/// be called when there is a key append stream or value append stream
			/// active. No more key-value insertion is allowed after a meta data block
			/// has been added to TFile.
			/// </remarks>
			/// <param name="name">Name of the meta block.</param>
			/// <param name="compressName">
			/// Name of the compression algorithm to be used. Must be one of the
			/// strings returned by
			/// <see cref="TFile.GetSupportedCompressionAlgorithms()"/>
			/// .
			/// </param>
			/// <returns>
			/// A DataOutputStream that can be used to write Meta Block data.
			/// Closing the stream would signal the ending of the block.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockAlreadyExists">the Meta Block with the same name already exists.
			/// 	</exception>
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockAlreadyExists"/>
			public virtual DataOutputStream PrepareMetaBlock(string name, string compressName
				)
			{
				if (state != TFile.Writer.State.Ready)
				{
					throw new InvalidOperationException("Incorrect state to start a Meta Block: " + state
						.ToString());
				}
				FinishDataBlock(true);
				DataOutputStream outputStream = writerBCF.PrepareMetaBlock(name, compressName);
				return outputStream;
			}

			/// <summary>Obtain an output stream for creating a meta block.</summary>
			/// <remarks>
			/// Obtain an output stream for creating a meta block. This function may not
			/// be called when there is a key append stream or value append stream
			/// active. No more key-value insertion is allowed after a meta data block
			/// has been added to TFile. Data will be compressed using the default
			/// compressor as defined in Writer's constructor.
			/// </remarks>
			/// <param name="name">Name of the meta block.</param>
			/// <returns>
			/// A DataOutputStream that can be used to write Meta Block data.
			/// Closing the stream would signal the ending of the block.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockAlreadyExists">the Meta Block with the same name already exists.
			/// 	</exception>
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockAlreadyExists"/>
			public virtual DataOutputStream PrepareMetaBlock(string name)
			{
				if (state != TFile.Writer.State.Ready)
				{
					throw new InvalidOperationException("Incorrect state to start a Meta Block: " + state
						.ToString());
				}
				FinishDataBlock(true);
				return writerBCF.PrepareMetaBlock(name);
			}

			/// <summary>Check if we need to start a new data block.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void InitDataBlock()
			{
				// for each new block, get a new appender
				if (blkAppender == null)
				{
					blkAppender = writerBCF.PrepareDataBlock();
				}
			}

			/// <summary>Close the current data block if necessary.</summary>
			/// <param name="bForceFinish">Force the closure regardless of the block size.</param>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void FinishDataBlock(bool bForceFinish)
			{
				if (blkAppender == null)
				{
					return;
				}
				// exceeded the size limit, do the compression and finish the block
				if (bForceFinish || blkAppender.GetCompressedSize() >= sizeMinBlock)
				{
					// keep tracks of the last key of each data block, no padding
					// for now
					TFile.TFileIndexEntry keyLast = new TFile.TFileIndexEntry(lastKeyBufferOS.GetBuffer
						(), 0, lastKeyBufferOS.Size(), blkRecordCount);
					tfileIndex.AddEntry(keyLast);
					// close the appender
					blkAppender.Close();
					blkAppender = null;
					blkRecordCount = 0;
				}
			}
		}

		/// <summary>TFile Reader.</summary>
		/// <remarks>
		/// TFile Reader. Users may only read TFiles by creating TFile.Reader.Scanner.
		/// objects. A scanner may scan the whole TFile (
		/// <see cref="CreateScanner()"/>
		/// ) , a portion of TFile based on byte offsets (
		/// <see cref="CreateScannerByByteRange(long, long)"/>
		/// ), or a portion of TFile with keys
		/// fall in a certain key range (for sorted TFile only,
		/// <see cref="CreateScannerByKey(byte[], byte[])"/>
		/// or
		/// <see cref="CreateScannerByKey(RawComparable, RawComparable)"/>
		/// ).
		/// </remarks>
		public class Reader : IDisposable
		{
			internal readonly BCFile.Reader readerBCF;

			internal TFile.TFileIndex tfileIndex = null;

			internal readonly TFile.TFileMeta tfileMeta;

			internal readonly CompareUtils.BytesComparator comparator;

			private readonly TFile.Reader.Location begin;

			private readonly TFile.Reader.Location end;

			/// <summary>Location representing a virtual position in the TFile.</summary>
			internal sealed class Location : Comparable<TFile.Reader.Location>, ICloneable
			{
				private int blockIndex;

				private long recordIndex;

				internal Location(int blockIndex, long recordIndex)
				{
					// The underlying BCFile reader.
					// TFile index, it is loaded lazily.
					// global begin and end locations.
					// distance/offset from the beginning of the block
					Set(blockIndex, recordIndex);
				}

				internal void IncRecordIndex()
				{
					++recordIndex;
				}

				internal Location(TFile.Reader.Location other)
				{
					Set(other);
				}

				internal int GetBlockIndex()
				{
					return blockIndex;
				}

				internal long GetRecordIndex()
				{
					return recordIndex;
				}

				internal void Set(int blockIndex, long recordIndex)
				{
					if ((blockIndex | recordIndex) < 0)
					{
						throw new ArgumentException("Illegal parameter for BlockLocation.");
					}
					this.blockIndex = blockIndex;
					this.recordIndex = recordIndex;
				}

				internal void Set(TFile.Reader.Location other)
				{
					Set(other.blockIndex, other.recordIndex);
				}

				/// <seealso cref="System.IComparable{T}.CompareTo(object)"/>
				public int CompareTo(TFile.Reader.Location other)
				{
					return CompareTo(other.blockIndex, other.recordIndex);
				}

				internal int CompareTo(int bid, long rid)
				{
					if (this.blockIndex == bid)
					{
						long ret = this.recordIndex - rid;
						if (ret > 0)
						{
							return 1;
						}
						if (ret < 0)
						{
							return -1;
						}
						return 0;
					}
					return this.blockIndex - bid;
				}

				/// <seealso cref="object.MemberwiseClone()"/>
				protected internal TFile.Reader.Location Clone()
				{
					return new TFile.Reader.Location(blockIndex, recordIndex);
				}

				/// <seealso cref="object.GetHashCode()"/>
				public override int GetHashCode()
				{
					int prime = 31;
					int result = prime + blockIndex;
					result = (int)(prime * result + recordIndex);
					return result;
				}

				/// <seealso cref="object.Equals(object)"/>
				public override bool Equals(object obj)
				{
					if (this == obj)
					{
						return true;
					}
					if (obj == null)
					{
						return false;
					}
					if (GetType() != obj.GetType())
					{
						return false;
					}
					TFile.Reader.Location other = (TFile.Reader.Location)obj;
					if (blockIndex != other.blockIndex)
					{
						return false;
					}
					if (recordIndex != other.recordIndex)
					{
						return false;
					}
					return true;
				}
			}

			/// <summary>Constructor</summary>
			/// <param name="fsdis">FS input stream of the TFile.</param>
			/// <param name="fileLength">
			/// The length of TFile. This is required because we have no easy
			/// way of knowing the actual size of the input file through the
			/// File input stream.
			/// </param>
			/// <param name="conf"/>
			/// <exception cref="System.IO.IOException"/>
			public Reader(FSDataInputStream fsdis, long fileLength, Configuration conf)
			{
				readerBCF = new BCFile.Reader(fsdis, fileLength, conf);
				// first, read TFile meta
				BCFile.Reader.BlockReader brMeta = readerBCF.GetMetaBlock(TFile.TFileMeta.BlockName
					);
				try
				{
					tfileMeta = new TFile.TFileMeta(brMeta);
				}
				finally
				{
					brMeta.Close();
				}
				comparator = tfileMeta.GetComparator();
				// Set begin and end locations.
				begin = new TFile.Reader.Location(0, 0);
				end = new TFile.Reader.Location(readerBCF.GetBlockCount(), 0);
			}

			/// <summary>Close the reader.</summary>
			/// <remarks>
			/// Close the reader. The state of the Reader object is undefined after
			/// close. Calling close() for multiple times has no effect.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				readerBCF.Close();
			}

			/// <summary>Get the begin location of the TFile.</summary>
			/// <returns>
			/// If TFile is not empty, the location of the first key-value pair.
			/// Otherwise, it returns end().
			/// </returns>
			internal virtual TFile.Reader.Location Begin()
			{
				return begin;
			}

			/// <summary>Get the end location of the TFile.</summary>
			/// <returns>The location right after the last key-value pair in TFile.</returns>
			internal virtual TFile.Reader.Location End()
			{
				return end;
			}

			/// <summary>Get the string representation of the comparator.</summary>
			/// <returns>
			/// If the TFile is not sorted by keys, an empty string will be
			/// returned. Otherwise, the actual comparator string that is
			/// provided during the TFile creation time will be returned.
			/// </returns>
			public virtual string GetComparatorName()
			{
				return tfileMeta.GetComparatorString();
			}

			/// <summary>Is the TFile sorted?</summary>
			/// <returns>true if TFile is sorted.</returns>
			public virtual bool IsSorted()
			{
				return tfileMeta.IsSorted();
			}

			/// <summary>Get the number of key-value pair entries in TFile.</summary>
			/// <returns>the number of key-value pairs in TFile</returns>
			public virtual long GetEntryCount()
			{
				return tfileMeta.GetRecordCount();
			}

			/// <summary>Lazily loading the TFile index.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void CheckTFileDataIndex()
			{
				lock (this)
				{
					if (tfileIndex == null)
					{
						BCFile.Reader.BlockReader brIndex = readerBCF.GetMetaBlock(TFile.TFileIndex.BlockName
							);
						try
						{
							tfileIndex = new TFile.TFileIndex(readerBCF.GetBlockCount(), brIndex, tfileMeta.GetComparator
								());
						}
						finally
						{
							brIndex.Close();
						}
					}
				}
			}

			/// <summary>Get the first key in the TFile.</summary>
			/// <returns>The first key in the TFile.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual RawComparable GetFirstKey()
			{
				CheckTFileDataIndex();
				return tfileIndex.GetFirstKey();
			}

			/// <summary>Get the last key in the TFile.</summary>
			/// <returns>The last key in the TFile.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual RawComparable GetLastKey()
			{
				CheckTFileDataIndex();
				return tfileIndex.GetLastKey();
			}

			/// <summary>Get a Comparator object to compare Entries.</summary>
			/// <remarks>
			/// Get a Comparator object to compare Entries. It is useful when you want
			/// stores the entries in a collection (such as PriorityQueue) and perform
			/// sorting or comparison among entries based on the keys without copying out
			/// the key.
			/// </remarks>
			/// <returns>An Entry Comparator..</returns>
			public virtual IComparer<TFile.Reader.Scanner.Entry> GetEntryComparator()
			{
				if (!IsSorted())
				{
					throw new RuntimeException("Entries are not comparable for unsorted TFiles");
				}
				return new _IComparer_931(this);
			}

			private sealed class _IComparer_931 : IComparer<TFile.Reader.Scanner.Entry>
			{
				public _IComparer_931(Reader _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <summary>Provide a customized comparator for Entries.</summary>
				/// <remarks>
				/// Provide a customized comparator for Entries. This is useful if we
				/// have a collection of Entry objects. However, if the Entry objects
				/// come from different TFiles, users must ensure that those TFiles share
				/// the same RawComparator.
				/// </remarks>
				public int Compare(TFile.Reader.Scanner.Entry o1, TFile.Reader.Scanner.Entry o2)
				{
					return this._enclosing.comparator.Compare(o1.GetKeyBuffer(), 0, o1.GetKeyLength()
						, o2.GetKeyBuffer(), 0, o2.GetKeyLength());
				}

				private readonly Reader _enclosing;
			}

			/// <summary>
			/// Get an instance of the RawComparator that is constructed based on the
			/// string comparator representation.
			/// </summary>
			/// <returns>a Comparator that can compare RawComparable's.</returns>
			public virtual IComparer<RawComparable> GetComparator()
			{
				return comparator;
			}

			/// <summary>Stream access to a meta block.``</summary>
			/// <param name="name">The name of the meta block.</param>
			/// <returns>The input stream.</returns>
			/// <exception cref="System.IO.IOException">on I/O error.</exception>
			/// <exception cref="MetaBlockDoesNotExist">If the meta block with the name does not exist.
			/// 	</exception>
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockDoesNotExist"/>
			public virtual DataInputStream GetMetaBlock(string name)
			{
				return readerBCF.GetMetaBlock(name);
			}

			/// <summary>
			/// if greater is true then returns the beginning location of the block
			/// containing the key strictly greater than input key.
			/// </summary>
			/// <remarks>
			/// if greater is true then returns the beginning location of the block
			/// containing the key strictly greater than input key. if greater is false
			/// then returns the beginning location of the block greater than equal to
			/// the input key
			/// </remarks>
			/// <param name="key">the input key</param>
			/// <param name="greater">boolean flag</param>
			/// <returns/>
			/// <exception cref="System.IO.IOException"/>
			internal virtual TFile.Reader.Location GetBlockContainsKey(RawComparable key, bool
				 greater)
			{
				if (!IsSorted())
				{
					throw new RuntimeException("Seeking in unsorted TFile");
				}
				CheckTFileDataIndex();
				int blkIndex = (greater) ? tfileIndex.UpperBound(key) : tfileIndex.LowerBound(key
					);
				if (blkIndex < 0)
				{
					return end;
				}
				return new TFile.Reader.Location(blkIndex, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual TFile.Reader.Location GetLocationByRecordNum(long recNum)
			{
				CheckTFileDataIndex();
				return tfileIndex.GetLocationByRecordNum(recNum);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual long GetRecordNumByLocation(TFile.Reader.Location location)
			{
				CheckTFileDataIndex();
				return tfileIndex.GetRecordNumByLocation(location);
			}

			internal virtual int CompareKeys(byte[] a, int o1, int l1, byte[] b, int o2, int 
				l2)
			{
				if (!IsSorted())
				{
					throw new RuntimeException("Cannot compare keys for unsorted TFiles.");
				}
				return comparator.Compare(a, o1, l1, b, o2, l2);
			}

			internal virtual int CompareKeys(RawComparable a, RawComparable b)
			{
				if (!IsSorted())
				{
					throw new RuntimeException("Cannot compare keys for unsorted TFiles.");
				}
				return comparator.Compare(a, b);
			}

			/// <summary>
			/// Get the location pointing to the beginning of the first key-value pair in
			/// a compressed block whose byte offset in the TFile is greater than or
			/// equal to the specified offset.
			/// </summary>
			/// <param name="offset">the user supplied offset.</param>
			/// <returns>
			/// the location to the corresponding entry; or end() if no such
			/// entry exists.
			/// </returns>
			internal virtual TFile.Reader.Location GetLocationNear(long offset)
			{
				int blockIndex = readerBCF.GetBlockIndexNear(offset);
				if (blockIndex == -1)
				{
					return end;
				}
				return new TFile.Reader.Location(blockIndex, 0);
			}

			/// <summary>
			/// Get the RecordNum for the first key-value pair in a compressed block
			/// whose byte offset in the TFile is greater than or equal to the specified
			/// offset.
			/// </summary>
			/// <param name="offset">the user supplied offset.</param>
			/// <returns>
			/// the RecordNum to the corresponding entry. If no such entry
			/// exists, it returns the total entry count.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetRecordNumNear(long offset)
			{
				return GetRecordNumByLocation(GetLocationNear(offset));
			}

			/// <summary>
			/// Get a sample key that is within a block whose starting offset is greater
			/// than or equal to the specified offset.
			/// </summary>
			/// <param name="offset">The file offset.</param>
			/// <returns>
			/// the key that fits the requirement; or null if no such key exists
			/// (which could happen if the offset is close to the end of the
			/// TFile).
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual RawComparable GetKeyNear(long offset)
			{
				int blockIndex = readerBCF.GetBlockIndexNear(offset);
				if (blockIndex == -1)
				{
					return null;
				}
				CheckTFileDataIndex();
				return new ByteArray(tfileIndex.GetEntry(blockIndex).key);
			}

			/// <summary>Get a scanner than can scan the whole TFile.</summary>
			/// <returns>
			/// The scanner object. A valid Scanner is always returned even if
			/// the TFile is empty.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual TFile.Reader.Scanner CreateScanner()
			{
				return new TFile.Reader.Scanner(this, begin, end);
			}

			/// <summary>Get a scanner that covers a portion of TFile based on byte offsets.</summary>
			/// <param name="offset">The beginning byte offset in the TFile.</param>
			/// <param name="length">The length of the region.</param>
			/// <returns>
			/// The actual coverage of the returned scanner tries to match the
			/// specified byte-region but always round up to the compression
			/// block boundaries. It is possible that the returned scanner
			/// contains zero key-value pairs even if length is positive.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual TFile.Reader.Scanner CreateScannerByByteRange(long offset, long length
				)
			{
				return new TFile.Reader.Scanner(this, offset, offset + length);
			}

			/// <summary>Get a scanner that covers a portion of TFile based on keys.</summary>
			/// <param name="beginKey">
			/// Begin key of the scan (inclusive). If null, scan from the first
			/// key-value entry of the TFile.
			/// </param>
			/// <param name="endKey">
			/// End key of the scan (exclusive). If null, scan up to the last
			/// key-value entry of the TFile.
			/// </param>
			/// <returns>
			/// The actual coverage of the returned scanner will cover all keys
			/// greater than or equal to the beginKey and less than the endKey.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use CreateScannerByKey(byte[], byte[]) instead.")]
			public virtual TFile.Reader.Scanner CreateScanner(byte[] beginKey, byte[] endKey)
			{
				return CreateScannerByKey(beginKey, endKey);
			}

			/// <summary>Get a scanner that covers a portion of TFile based on keys.</summary>
			/// <param name="beginKey">
			/// Begin key of the scan (inclusive). If null, scan from the first
			/// key-value entry of the TFile.
			/// </param>
			/// <param name="endKey">
			/// End key of the scan (exclusive). If null, scan up to the last
			/// key-value entry of the TFile.
			/// </param>
			/// <returns>
			/// The actual coverage of the returned scanner will cover all keys
			/// greater than or equal to the beginKey and less than the endKey.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual TFile.Reader.Scanner CreateScannerByKey(byte[] beginKey, byte[] endKey
				)
			{
				return CreateScannerByKey((beginKey == null) ? null : new ByteArray(beginKey, 0, 
					beginKey.Length), (endKey == null) ? null : new ByteArray(endKey, 0, endKey.Length
					));
			}

			/// <summary>Get a scanner that covers a specific key range.</summary>
			/// <param name="beginKey">
			/// Begin key of the scan (inclusive). If null, scan from the first
			/// key-value entry of the TFile.
			/// </param>
			/// <param name="endKey">
			/// End key of the scan (exclusive). If null, scan up to the last
			/// key-value entry of the TFile.
			/// </param>
			/// <returns>
			/// The actual coverage of the returned scanner will cover all keys
			/// greater than or equal to the beginKey and less than the endKey.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use CreateScannerByKey(RawComparable, RawComparable) instead."
				)]
			public virtual TFile.Reader.Scanner CreateScanner(RawComparable beginKey, RawComparable
				 endKey)
			{
				return CreateScannerByKey(beginKey, endKey);
			}

			/// <summary>Get a scanner that covers a specific key range.</summary>
			/// <param name="beginKey">
			/// Begin key of the scan (inclusive). If null, scan from the first
			/// key-value entry of the TFile.
			/// </param>
			/// <param name="endKey">
			/// End key of the scan (exclusive). If null, scan up to the last
			/// key-value entry of the TFile.
			/// </param>
			/// <returns>
			/// The actual coverage of the returned scanner will cover all keys
			/// greater than or equal to the beginKey and less than the endKey.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual TFile.Reader.Scanner CreateScannerByKey(RawComparable beginKey, RawComparable
				 endKey)
			{
				if ((beginKey != null) && (endKey != null) && (CompareKeys(beginKey, endKey) >= 0
					))
				{
					return new TFile.Reader.Scanner(this, beginKey, beginKey);
				}
				return new TFile.Reader.Scanner(this, beginKey, endKey);
			}

			/// <summary>Create a scanner that covers a range of records.</summary>
			/// <param name="beginRecNum">The RecordNum for the first record (inclusive).</param>
			/// <param name="endRecNum">
			/// The RecordNum for the last record (exclusive). To scan the whole
			/// file, either specify endRecNum==-1 or endRecNum==getEntryCount().
			/// </param>
			/// <returns>The TFile scanner that covers the specified range of records.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual TFile.Reader.Scanner CreateScannerByRecordNum(long beginRecNum, long
				 endRecNum)
			{
				if (beginRecNum < 0)
				{
					beginRecNum = 0;
				}
				if (endRecNum < 0 || endRecNum > GetEntryCount())
				{
					endRecNum = GetEntryCount();
				}
				return new TFile.Reader.Scanner(this, GetLocationByRecordNum(beginRecNum), GetLocationByRecordNum
					(endRecNum));
			}

			/// <summary>The TFile Scanner.</summary>
			/// <remarks>
			/// The TFile Scanner. The Scanner has an implicit cursor, which, upon
			/// creation, points to the first key-value pair in the scan range. If the
			/// scan range is empty, the cursor will point to the end of the scan range.
			/// <p>
			/// Use
			/// <see cref="AtEnd()"/>
			/// to test whether the cursor is at the end
			/// location of the scanner.
			/// <p>
			/// Use
			/// <see cref="Advance()"/>
			/// to move the cursor to the next key-value
			/// pair (or end if none exists). Use seekTo methods (
			/// <see cref="SeekTo(byte[])"/>
			/// or
			/// <see cref="SeekTo(byte[], int, int)"/>
			/// ) to seek to any arbitrary
			/// location in the covered range (including backward seeking). Use
			/// <see cref="Rewind()"/>
			/// to seek back to the beginning of the scanner.
			/// Use
			/// <see cref="SeekToEnd()"/>
			/// to seek to the end of the scanner.
			/// <p>
			/// Actual keys and values may be obtained through
			/// <see cref="Entry"/>
			/// object, which is obtained through
			/// <see cref="Entry()"/>
			/// .
			/// </remarks>
			public class Scanner : IDisposable
			{
				internal readonly TFile.Reader reader;

				private BCFile.Reader.BlockReader blkReader;

				internal TFile.Reader.Location beginLocation;

				internal TFile.Reader.Location endLocation;

				internal TFile.Reader.Location currentLocation;

				internal bool valueChecked = false;

				internal readonly byte[] keyBuffer;

				internal int klen = -1;

				internal const int MaxValTransferBufSize = 128 * 1024;

				internal BytesWritable valTransferBuffer;

				internal DataInputBuffer keyDataInputStream;

				internal Chunk.ChunkDecoder valueBufferInputStream;

				internal DataInputStream valueDataInputStream;

				internal int vlen;

				/// <summary>Constructor</summary>
				/// <param name="reader">The TFile reader object.</param>
				/// <param name="offBegin">Begin byte-offset of the scan.</param>
				/// <param name="offEnd">End byte-offset of the scan.</param>
				/// <exception cref="System.IO.IOException">
				/// The offsets will be rounded to the beginning of a compressed
				/// block whose offset is greater than or equal to the specified
				/// offset.
				/// </exception>
				protected internal Scanner(TFile.Reader reader, long offBegin, long offEnd)
					: this(reader, reader.GetLocationNear(offBegin), reader.GetLocationNear(offEnd))
				{
				}

				/// <summary>Constructor</summary>
				/// <param name="reader">The TFile reader object.</param>
				/// <param name="begin">Begin location of the scan.</param>
				/// <param name="end">End location of the scan.</param>
				/// <exception cref="System.IO.IOException"/>
				internal Scanner(TFile.Reader reader, TFile.Reader.Location begin, TFile.Reader.Location
					 end)
				{
					// The underlying TFile reader.
					// current block (null if reaching end)
					// flag to ensure value is only examined once.
					// reusable buffer for keys.
					// length of key, -1 means key is invalid.
					// vlen == -1 if unknown.
					this.reader = reader;
					// ensure the TFile index is loaded throughout the life of scanner.
					reader.CheckTFileDataIndex();
					beginLocation = begin;
					endLocation = end;
					valTransferBuffer = new BytesWritable();
					// TODO: remember the longest key in a TFile, and use it to replace
					// MAX_KEY_SIZE.
					keyBuffer = new byte[MaxKeySize];
					keyDataInputStream = new DataInputBuffer();
					valueBufferInputStream = new Chunk.ChunkDecoder();
					valueDataInputStream = new DataInputStream(valueBufferInputStream);
					if (beginLocation.CompareTo(endLocation) >= 0)
					{
						currentLocation = new TFile.Reader.Location(endLocation);
					}
					else
					{
						currentLocation = new TFile.Reader.Location(0, 0);
						InitBlock(beginLocation.GetBlockIndex());
						InBlockAdvance(beginLocation.GetRecordIndex());
					}
				}

				/// <summary>Constructor</summary>
				/// <param name="reader">The TFile reader object.</param>
				/// <param name="beginKey">
				/// Begin key of the scan. If null, scan from the first <K,V>
				/// entry of the TFile.
				/// </param>
				/// <param name="endKey">
				/// End key of the scan. If null, scan up to the last <K, V> entry
				/// of the TFile.
				/// </param>
				/// <exception cref="System.IO.IOException"/>
				protected internal Scanner(TFile.Reader reader, RawComparable beginKey, RawComparable
					 endKey)
					: this(reader, (beginKey == null) ? reader.Begin() : reader.GetBlockContainsKey(beginKey
						, false), reader.End())
				{
					if (beginKey != null)
					{
						InBlockAdvance(beginKey, false);
						beginLocation.Set(currentLocation);
					}
					if (endKey != null)
					{
						SeekTo(endKey, false);
						endLocation.Set(currentLocation);
						SeekTo(beginLocation);
					}
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key. Synonymous to seekTo(key, 0, key.length). The entry
				/// returned by the previous entry() call will be invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <returns>true if we find an equal key.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual bool SeekTo(byte[] key)
				{
					return SeekTo(key, 0, key.Length);
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key. The entry returned by the previous entry() call will
				/// be invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <param name="keyOffset">offset in the key buffer.</param>
				/// <param name="keyLen">key buffer length.</param>
				/// <returns>true if we find an equal key; false otherwise.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual bool SeekTo(byte[] key, int keyOffset, int keyLen)
				{
					return SeekTo(new ByteArray(key, keyOffset, keyLen), false);
				}

				/// <exception cref="System.IO.IOException"/>
				private bool SeekTo(RawComparable key, bool beyond)
				{
					TFile.Reader.Location l = reader.GetBlockContainsKey(key, beyond);
					if (l.CompareTo(beginLocation) < 0)
					{
						l = beginLocation;
					}
					else
					{
						if (l.CompareTo(endLocation) >= 0)
						{
							SeekTo(endLocation);
							return false;
						}
					}
					// check if what we are seeking is in the later part of the current
					// block.
					if (AtEnd() || (l.GetBlockIndex() != currentLocation.GetBlockIndex()) || (CompareCursorKeyTo
						(key) >= 0))
					{
						// sorry, we must seek to a different location first.
						SeekTo(l);
					}
					return InBlockAdvance(key, beyond);
				}

				/// <summary>Move the cursor to the new location.</summary>
				/// <remarks>
				/// Move the cursor to the new location. The entry returned by the previous
				/// entry() call will be invalid.
				/// </remarks>
				/// <param name="l">
				/// new cursor location. It must fall between the begin and end
				/// location of the scanner.
				/// </param>
				/// <exception cref="System.IO.IOException"/>
				private void SeekTo(TFile.Reader.Location l)
				{
					if (l.CompareTo(beginLocation) < 0)
					{
						throw new ArgumentException("Attempt to seek before the begin location.");
					}
					if (l.CompareTo(endLocation) > 0)
					{
						throw new ArgumentException("Attempt to seek after the end location.");
					}
					if (l.CompareTo(endLocation) == 0)
					{
						ParkCursorAtEnd();
						return;
					}
					if (l.GetBlockIndex() != currentLocation.GetBlockIndex())
					{
						// going to a totally different block
						InitBlock(l.GetBlockIndex());
					}
					else
					{
						if (valueChecked)
						{
							// may temporarily go beyond the last record in the block (in which
							// case the next if loop will always be true).
							InBlockAdvance(1);
						}
						if (l.GetRecordIndex() < currentLocation.GetRecordIndex())
						{
							InitBlock(l.GetBlockIndex());
						}
					}
					InBlockAdvance(l.GetRecordIndex() - currentLocation.GetRecordIndex());
					return;
				}

				/// <summary>Rewind to the first entry in the scanner.</summary>
				/// <remarks>
				/// Rewind to the first entry in the scanner. The entry returned by the
				/// previous entry() call will be invalid.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void Rewind()
				{
					SeekTo(beginLocation);
				}

				/// <summary>Seek to the end of the scanner.</summary>
				/// <remarks>
				/// Seek to the end of the scanner. The entry returned by the previous
				/// entry() call will be invalid.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void SeekToEnd()
				{
					ParkCursorAtEnd();
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key. Synonymous to lowerBound(key, 0, key.length). The
				/// entry returned by the previous entry() call will be invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <exception cref="System.IO.IOException"/>
				public virtual void LowerBound(byte[] key)
				{
					LowerBound(key, 0, key.Length);
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is greater than or equal
				/// to the input key. The entry returned by the previous entry() call will
				/// be invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <param name="keyOffset">offset in the key buffer.</param>
				/// <param name="keyLen">key buffer length.</param>
				/// <exception cref="System.IO.IOException"/>
				public virtual void LowerBound(byte[] key, int keyOffset, int keyLen)
				{
					SeekTo(new ByteArray(key, keyOffset, keyLen), false);
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is strictly greater than
				/// the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is strictly greater than
				/// the input key. Synonymous to upperBound(key, 0, key.length). The entry
				/// returned by the previous entry() call will be invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <exception cref="System.IO.IOException"/>
				public virtual void UpperBound(byte[] key)
				{
					UpperBound(key, 0, key.Length);
				}

				/// <summary>
				/// Move the cursor to the first entry whose key is strictly greater than
				/// the input key.
				/// </summary>
				/// <remarks>
				/// Move the cursor to the first entry whose key is strictly greater than
				/// the input key. The entry returned by the previous entry() call will be
				/// invalid.
				/// </remarks>
				/// <param name="key">The input key</param>
				/// <param name="keyOffset">offset in the key buffer.</param>
				/// <param name="keyLen">key buffer length.</param>
				/// <exception cref="System.IO.IOException"/>
				public virtual void UpperBound(byte[] key, int keyOffset, int keyLen)
				{
					SeekTo(new ByteArray(key, keyOffset, keyLen), true);
				}

				/// <summary>Move the cursor to the next key-value pair.</summary>
				/// <remarks>
				/// Move the cursor to the next key-value pair. The entry returned by the
				/// previous entry() call will be invalid.
				/// </remarks>
				/// <returns>
				/// true if the cursor successfully moves. False when cursor is
				/// already at the end location and cannot be advanced.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual bool Advance()
				{
					if (AtEnd())
					{
						return false;
					}
					int curBid = currentLocation.GetBlockIndex();
					long curRid = currentLocation.GetRecordIndex();
					long entriesInBlock = reader.GetBlockEntryCount(curBid);
					if (curRid + 1 >= entriesInBlock)
					{
						if (endLocation.CompareTo(curBid + 1, 0) <= 0)
						{
							// last entry in TFile.
							ParkCursorAtEnd();
						}
						else
						{
							// last entry in Block.
							InitBlock(curBid + 1);
						}
					}
					else
					{
						InBlockAdvance(1);
					}
					return true;
				}

				/// <summary>Load a compressed block for reading.</summary>
				/// <remarks>Load a compressed block for reading. Expecting blockIndex is valid.</remarks>
				/// <exception cref="System.IO.IOException"/>
				private void InitBlock(int blockIndex)
				{
					klen = -1;
					if (blkReader != null)
					{
						try
						{
							blkReader.Close();
						}
						finally
						{
							blkReader = null;
						}
					}
					blkReader = reader.GetBlockReader(blockIndex);
					currentLocation.Set(blockIndex, 0);
				}

				/// <exception cref="System.IO.IOException"/>
				private void ParkCursorAtEnd()
				{
					klen = -1;
					currentLocation.Set(endLocation);
					if (blkReader != null)
					{
						try
						{
							blkReader.Close();
						}
						finally
						{
							blkReader = null;
						}
					}
				}

				/// <summary>Close the scanner.</summary>
				/// <remarks>
				/// Close the scanner. Release all resources. The behavior of using the
				/// scanner after calling close is not defined. The entry returned by the
				/// previous entry() call will be invalid.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void Close()
				{
					ParkCursorAtEnd();
				}

				/// <summary>Is cursor at the end location?</summary>
				/// <returns>true if the cursor is at the end location.</returns>
				public virtual bool AtEnd()
				{
					return (currentLocation.CompareTo(endLocation) >= 0);
				}

				/// <summary>check whether we have already successfully obtained the key.</summary>
				/// <remarks>
				/// check whether we have already successfully obtained the key. It also
				/// initializes the valueInputStream.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				internal virtual void CheckKey()
				{
					if (klen >= 0)
					{
						return;
					}
					if (AtEnd())
					{
						throw new EOFException("No key-value to read");
					}
					klen = -1;
					vlen = -1;
					valueChecked = false;
					klen = Utils.ReadVInt(blkReader);
					blkReader.ReadFully(keyBuffer, 0, klen);
					valueBufferInputStream.Reset(blkReader);
					if (valueBufferInputStream.IsLastChunk())
					{
						vlen = valueBufferInputStream.GetRemain();
					}
				}

				/// <summary>Get an entry to access the key and value.</summary>
				/// <returns>The Entry object to access the key and value.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual TFile.Reader.Scanner.Entry Entry()
				{
					CheckKey();
					return new TFile.Reader.Scanner.Entry(this);
				}

				/// <summary>Get the RecordNum corresponding to the entry pointed by the cursor.</summary>
				/// <returns>The RecordNum corresponding to the entry pointed by the cursor.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual long GetRecordNum()
				{
					return reader.GetRecordNumByLocation(currentLocation);
				}

				/// <summary>Internal API.</summary>
				/// <remarks>Internal API. Comparing the key at cursor to user-specified key.</remarks>
				/// <param name="other">user-specified key.</param>
				/// <returns>
				/// negative if key at cursor is smaller than user key; 0 if equal;
				/// and positive if key at cursor greater than user key.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				internal virtual int CompareCursorKeyTo(RawComparable other)
				{
					CheckKey();
					return reader.CompareKeys(keyBuffer, 0, klen, other.Buffer(), other.Offset(), other
						.Size());
				}

				/// <summary>Entry to a &lt;Key, Value&gt; pair.</summary>
				public class Entry : Comparable<RawComparable>
				{
					/// <summary>Get the length of the key.</summary>
					/// <returns>the length of the key.</returns>
					public virtual int GetKeyLength()
					{
						return this._enclosing.klen;
					}

					internal virtual byte[] GetKeyBuffer()
					{
						return this._enclosing.keyBuffer;
					}

					/// <summary>Copy the key and value in one shot into BytesWritables.</summary>
					/// <remarks>
					/// Copy the key and value in one shot into BytesWritables. This is
					/// equivalent to getKey(key); getValue(value);
					/// </remarks>
					/// <param name="key">BytesWritable to hold key.</param>
					/// <param name="value">BytesWritable to hold value</param>
					/// <exception cref="System.IO.IOException"/>
					public virtual void Get(BytesWritable key, BytesWritable value)
					{
						this.GetKey(key);
						this.GetValue(value);
					}

					/// <summary>Copy the key into BytesWritable.</summary>
					/// <remarks>
					/// Copy the key into BytesWritable. The input BytesWritable will be
					/// automatically resized to the actual key size.
					/// </remarks>
					/// <param name="key">BytesWritable to hold the key.</param>
					/// <exception cref="System.IO.IOException"/>
					public virtual int GetKey(BytesWritable key)
					{
						key.SetSize(this.GetKeyLength());
						this.GetKey(key.GetBytes());
						return key.GetLength();
					}

					/// <summary>Copy the value into BytesWritable.</summary>
					/// <remarks>
					/// Copy the value into BytesWritable. The input BytesWritable will be
					/// automatically resized to the actual value size. The implementation
					/// directly uses the buffer inside BytesWritable for storing the value.
					/// The call does not require the value length to be known.
					/// </remarks>
					/// <param name="value"/>
					/// <exception cref="System.IO.IOException"/>
					public virtual long GetValue(BytesWritable value)
					{
						DataInputStream dis = this.GetValueStream();
						int size = 0;
						try
						{
							int remain;
							while ((remain = this._enclosing.valueBufferInputStream.GetRemain()) > 0)
							{
								value.SetSize(size + remain);
								dis.ReadFully(value.GetBytes(), size, remain);
								size += remain;
							}
							return value.GetLength();
						}
						finally
						{
							dis.Close();
						}
					}

					/// <summary>Writing the key to the output stream.</summary>
					/// <remarks>
					/// Writing the key to the output stream. This method avoids copying key
					/// buffer from Scanner into user buffer, then writing to the output
					/// stream.
					/// </remarks>
					/// <param name="out">The output stream</param>
					/// <returns>the length of the key.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int WriteKey(OutputStream @out)
					{
						@out.Write(this._enclosing.keyBuffer, 0, this._enclosing.klen);
						return this._enclosing.klen;
					}

					/// <summary>Writing the value to the output stream.</summary>
					/// <remarks>
					/// Writing the value to the output stream. This method avoids copying
					/// value data from Scanner into user buffer, then writing to the output
					/// stream. It does not require the value length to be known.
					/// </remarks>
					/// <param name="out">The output stream</param>
					/// <returns>the length of the value</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual long WriteValue(OutputStream @out)
					{
						DataInputStream dis = this.GetValueStream();
						long size = 0;
						try
						{
							int chunkSize;
							while ((chunkSize = this._enclosing.valueBufferInputStream.GetRemain()) > 0)
							{
								chunkSize = Math.Min(chunkSize, TFile.Reader.Scanner.MaxValTransferBufSize);
								this._enclosing.valTransferBuffer.SetSize(chunkSize);
								dis.ReadFully(this._enclosing.valTransferBuffer.GetBytes(), 0, chunkSize);
								@out.Write(this._enclosing.valTransferBuffer.GetBytes(), 0, chunkSize);
								size += chunkSize;
							}
							return size;
						}
						finally
						{
							dis.Close();
						}
					}

					/// <summary>Copy the key into user supplied buffer.</summary>
					/// <param name="buf">
					/// The buffer supplied by user. The length of the buffer must
					/// not be shorter than the key length.
					/// </param>
					/// <returns>The length of the key.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int GetKey(byte[] buf)
					{
						return this.GetKey(buf, 0);
					}

					/// <summary>Copy the key into user supplied buffer.</summary>
					/// <param name="buf">The buffer supplied by user.</param>
					/// <param name="offset">
					/// The starting offset of the user buffer where we should copy
					/// the key into. Requiring the key-length + offset no greater
					/// than the buffer length.
					/// </param>
					/// <returns>The length of the key.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int GetKey(byte[] buf, int offset)
					{
						if ((offset | (buf.Length - offset - this._enclosing.klen)) < 0)
						{
							throw new IndexOutOfRangeException("Bufer not enough to store the key");
						}
						System.Array.Copy(this._enclosing.keyBuffer, 0, buf, offset, this._enclosing.klen
							);
						return this._enclosing.klen;
					}

					/// <summary>Streaming access to the key.</summary>
					/// <remarks>
					/// Streaming access to the key. Useful for desrializing the key into
					/// user objects.
					/// </remarks>
					/// <returns>The input stream.</returns>
					public virtual DataInputStream GetKeyStream()
					{
						this._enclosing.keyDataInputStream.Reset(this._enclosing.keyBuffer, this._enclosing
							.klen);
						return this._enclosing.keyDataInputStream;
					}

					/// <summary>Get the length of the value.</summary>
					/// <remarks>
					/// Get the length of the value. isValueLengthKnown() must be tested
					/// true.
					/// </remarks>
					/// <returns>the length of the value.</returns>
					public virtual int GetValueLength()
					{
						if (this._enclosing.vlen >= 0)
						{
							return this._enclosing.vlen;
						}
						throw new RuntimeException("Value length unknown.");
					}

					/// <summary>Copy value into user-supplied buffer.</summary>
					/// <remarks>
					/// Copy value into user-supplied buffer. User supplied buffer must be
					/// large enough to hold the whole value. The value part of the key-value
					/// pair pointed by the current cursor is not cached and can only be
					/// examined once. Calling any of the following functions more than once
					/// without moving the cursor will result in exception:
					/// <see cref="GetValue(byte[])"/>
					/// ,
					/// <see cref="GetValue(byte[], int)"/>
					/// ,
					/// <see cref="GetValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>
					/// the length of the value. Does not require
					/// isValueLengthKnown() to be true.
					/// </returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int GetValue(byte[] buf)
					{
						return this.GetValue(buf, 0);
					}

					/// <summary>Copy value into user-supplied buffer.</summary>
					/// <remarks>
					/// Copy value into user-supplied buffer. User supplied buffer must be
					/// large enough to hold the whole value (starting from the offset). The
					/// value part of the key-value pair pointed by the current cursor is not
					/// cached and can only be examined once. Calling any of the following
					/// functions more than once without moving the cursor will result in
					/// exception:
					/// <see cref="GetValue(byte[])"/>
					/// ,
					/// <see cref="GetValue(byte[], int)"/>
					/// ,
					/// <see cref="GetValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>
					/// the length of the value. Does not require
					/// isValueLengthKnown() to be true.
					/// </returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int GetValue(byte[] buf, int offset)
					{
						DataInputStream dis = this.GetValueStream();
						try
						{
							if (this.IsValueLengthKnown())
							{
								if ((offset | (buf.Length - offset - this._enclosing.vlen)) < 0)
								{
									throw new IndexOutOfRangeException("Buffer too small to hold value");
								}
								dis.ReadFully(buf, offset, this._enclosing.vlen);
								return this._enclosing.vlen;
							}
							int nextOffset = offset;
							while (nextOffset < buf.Length)
							{
								int n = dis.Read(buf, nextOffset, buf.Length - nextOffset);
								if (n < 0)
								{
									break;
								}
								nextOffset += n;
							}
							if (dis.Read() >= 0)
							{
								// attempt to read one more byte to determine whether we reached
								// the
								// end or not.
								throw new IndexOutOfRangeException("Buffer too small to hold value");
							}
							return nextOffset - offset;
						}
						finally
						{
							dis.Close();
						}
					}

					/// <summary>Stream access to value.</summary>
					/// <remarks>
					/// Stream access to value. The value part of the key-value pair pointed
					/// by the current cursor is not cached and can only be examined once.
					/// Calling any of the following functions more than once without moving
					/// the cursor will result in exception:
					/// <see cref="GetValue(byte[])"/>
					/// ,
					/// <see cref="GetValue(byte[], int)"/>
					/// ,
					/// <see cref="GetValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>The input stream for reading the value.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual DataInputStream GetValueStream()
					{
						if (this._enclosing.valueChecked == true)
						{
							throw new InvalidOperationException("Attempt to examine value multiple times.");
						}
						this._enclosing.valueChecked = true;
						return this._enclosing.valueDataInputStream;
					}

					/// <summary>Check whether it is safe to call getValueLength().</summary>
					/// <returns>
					/// true if value length is known before hand. Values less than
					/// the chunk size will always have their lengths known before
					/// hand. Values that are written out as a whole (with advertised
					/// length up-front) will always have their lengths known in
					/// read.
					/// </returns>
					public virtual bool IsValueLengthKnown()
					{
						return (this._enclosing.vlen >= 0);
					}

					/// <summary>Compare the entry key to another key.</summary>
					/// <remarks>
					/// Compare the entry key to another key. Synonymous to compareTo(key, 0,
					/// key.length).
					/// </remarks>
					/// <param name="buf">The key buffer.</param>
					/// <returns>comparison result between the entry key with the input key.</returns>
					public virtual int CompareTo(byte[] buf)
					{
						return this.CompareTo(buf, 0, buf.Length);
					}

					/// <summary>Compare the entry key to another key.</summary>
					/// <remarks>
					/// Compare the entry key to another key. Synonymous to compareTo(new
					/// ByteArray(buf, offset, length)
					/// </remarks>
					/// <param name="buf">The key buffer</param>
					/// <param name="offset">offset into the key buffer.</param>
					/// <param name="length">the length of the key.</param>
					/// <returns>comparison result between the entry key with the input key.</returns>
					public virtual int CompareTo(byte[] buf, int offset, int length)
					{
						return this.CompareTo(new ByteArray(buf, offset, length));
					}

					/// <summary>Compare an entry with a RawComparable object.</summary>
					/// <remarks>
					/// Compare an entry with a RawComparable object. This is useful when
					/// Entries are stored in a collection, and we want to compare a user
					/// supplied key.
					/// </remarks>
					public virtual int CompareTo(RawComparable key)
					{
						return this._enclosing.reader.CompareKeys(this._enclosing.keyBuffer, 0, this.GetKeyLength
							(), key.Buffer(), key.Offset(), key.Size());
					}

					/// <summary>Compare whether this and other points to the same key value.</summary>
					public override bool Equals(object other)
					{
						if (this == other)
						{
							return true;
						}
						if (!(other is TFile.Reader.Scanner.Entry))
						{
							return false;
						}
						return ((TFile.Reader.Scanner.Entry)other).CompareTo(this._enclosing.keyBuffer, 0
							, this.GetKeyLength()) == 0;
					}

					public override int GetHashCode()
					{
						return WritableComparator.HashBytes(this._enclosing.keyBuffer, 0, this.GetKeyLength
							());
					}

					internal Entry(Scanner _enclosing)
					{
						this._enclosing = _enclosing;
					}

					private readonly Scanner _enclosing;
				}

				/// <summary>Advance cursor by n positions within the block.</summary>
				/// <param name="n">Number of key-value pairs to skip in block.</param>
				/// <exception cref="System.IO.IOException"/>
				private void InBlockAdvance(long n)
				{
					for (long i = 0; i < n; ++i)
					{
						CheckKey();
						if (!valueBufferInputStream.IsClosed())
						{
							valueBufferInputStream.Close();
						}
						klen = -1;
						currentLocation.IncRecordIndex();
					}
				}

				/// <summary>
				/// Advance cursor in block until we find a key that is greater than or
				/// equal to the input key.
				/// </summary>
				/// <param name="key">Key to compare.</param>
				/// <param name="greater">advance until we find a key greater than the input key.</param>
				/// <returns>true if we find a equal key.</returns>
				/// <exception cref="System.IO.IOException"/>
				private bool InBlockAdvance(RawComparable key, bool greater)
				{
					int curBid = currentLocation.GetBlockIndex();
					long entryInBlock = reader.GetBlockEntryCount(curBid);
					if (curBid == endLocation.GetBlockIndex())
					{
						entryInBlock = endLocation.GetRecordIndex();
					}
					while (currentLocation.GetRecordIndex() < entryInBlock)
					{
						int cmp = CompareCursorKeyTo(key);
						if (cmp > 0)
						{
							return false;
						}
						if (cmp == 0 && !greater)
						{
							return true;
						}
						if (!valueBufferInputStream.IsClosed())
						{
							valueBufferInputStream.Close();
						}
						klen = -1;
						currentLocation.IncRecordIndex();
					}
					throw new RuntimeException("Cannot find matching key in block.");
				}
			}

			internal virtual long GetBlockEntryCount(int curBid)
			{
				return tfileIndex.GetEntry(curBid).Entries();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual BCFile.Reader.BlockReader GetBlockReader(int blockIndex)
			{
				return readerBCF.GetDataBlock(blockIndex);
			}
		}

		/// <summary>Data structure representing "TFile.meta" meta block.</summary>
		internal sealed class TFileMeta
		{
			internal const string BlockName = "TFile.meta";

			internal readonly Utils.Version version;

			private long recordCount;

			private readonly string strComparator;

			private readonly CompareUtils.BytesComparator comparator;

			public TFileMeta(string comparator)
			{
				// ctor for writes
				// set fileVersion to API version when we create it.
				version = TFile.ApiVersion;
				recordCount = 0;
				strComparator = (comparator == null) ? string.Empty : comparator;
				this.comparator = MakeComparator(strComparator);
			}

			/// <exception cref="System.IO.IOException"/>
			public TFileMeta(DataInput @in)
			{
				// ctor for reads
				version = new Utils.Version(@in);
				if (!version.CompatibleWith(TFile.ApiVersion))
				{
					throw new RuntimeException("Incompatible TFile fileVersion.");
				}
				recordCount = Utils.ReadVLong(@in);
				strComparator = Utils.ReadString(@in);
				comparator = MakeComparator(strComparator);
			}

			internal static CompareUtils.BytesComparator MakeComparator(string comparator)
			{
				if (comparator.Length == 0)
				{
					// unsorted keys
					return null;
				}
				if (comparator.Equals(ComparatorMemcmp))
				{
					// default comparator
					return new CompareUtils.BytesComparator(new CompareUtils.MemcmpRawComparator());
				}
				else
				{
					if (comparator.StartsWith(ComparatorJclass))
					{
						string compClassName = Sharpen.Runtime.Substring(comparator, ComparatorJclass.Length
							).Trim();
						try
						{
							Type compClass = Sharpen.Runtime.GetType(compClassName);
							// use its default ctor to create an instance
							return new CompareUtils.BytesComparator((RawComparator<object>)System.Activator.CreateInstance
								(compClass));
						}
						catch (Exception e)
						{
							throw new ArgumentException("Failed to instantiate comparator: " + comparator + "("
								 + e.ToString() + ")");
						}
					}
					else
					{
						throw new ArgumentException("Unsupported comparator: " + comparator);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(DataOutput @out)
			{
				TFile.ApiVersion.Write(@out);
				Utils.WriteVLong(@out, recordCount);
				Utils.WriteString(@out, strComparator);
			}

			public long GetRecordCount()
			{
				return recordCount;
			}

			public void IncRecordCount()
			{
				++recordCount;
			}

			public bool IsSorted()
			{
				return !strComparator.IsEmpty();
			}

			public string GetComparatorString()
			{
				return strComparator;
			}

			public CompareUtils.BytesComparator GetComparator()
			{
				return comparator;
			}

			public Utils.Version GetVersion()
			{
				return version;
			}
		}

		/// <summary>Data structure representing "TFile.index" meta block.</summary>
		internal class TFileIndex
		{
			internal const string BlockName = "TFile.index";

			private ByteArray firstKey;

			private readonly AList<TFile.TFileIndexEntry> index;

			private readonly AList<long> recordNumIndex;

			private readonly CompareUtils.BytesComparator comparator;

			private long sum = 0;

			/// <summary>For reading from file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public TFileIndex(int entryCount, DataInput @in, CompareUtils.BytesComparator comparator
				)
			{
				// END: class MetaTFileMeta
				index = new AList<TFile.TFileIndexEntry>(entryCount);
				recordNumIndex = new AList<long>(entryCount);
				int size = Utils.ReadVInt(@in);
				// size for the first key entry.
				if (size > 0)
				{
					byte[] buffer = new byte[size];
					@in.ReadFully(buffer);
					DataInputStream firstKeyInputStream = new DataInputStream(new ByteArrayInputStream
						(buffer, 0, size));
					int firstKeyLength = Utils.ReadVInt(firstKeyInputStream);
					firstKey = new ByteArray(new byte[firstKeyLength]);
					firstKeyInputStream.ReadFully(firstKey.Buffer());
					for (int i = 0; i < entryCount; i++)
					{
						size = Utils.ReadVInt(@in);
						if (buffer.Length < size)
						{
							buffer = new byte[size];
						}
						@in.ReadFully(buffer, 0, size);
						TFile.TFileIndexEntry idx = new TFile.TFileIndexEntry(new DataInputStream(new ByteArrayInputStream
							(buffer, 0, size)));
						index.AddItem(idx);
						sum += idx.Entries();
						recordNumIndex.AddItem(sum);
					}
				}
				else
				{
					if (entryCount != 0)
					{
						throw new RuntimeException("Internal error");
					}
				}
				this.comparator = comparator;
			}

			/// <param name="key">input key.</param>
			/// <returns>
			/// the ID of the first block that contains key &gt;= input key. Or -1
			/// if no such block exists.
			/// </returns>
			public virtual int LowerBound(RawComparable key)
			{
				if (comparator == null)
				{
					throw new RuntimeException("Cannot search in unsorted TFile");
				}
				if (firstKey == null)
				{
					return -1;
				}
				// not found
				int ret = Utils.LowerBound(index, key, comparator);
				if (ret == index.Count)
				{
					return -1;
				}
				return ret;
			}

			/// <param name="key">input key.</param>
			/// <returns>
			/// the ID of the first block that contains key &gt; input key. Or -1
			/// if no such block exists.
			/// </returns>
			public virtual int UpperBound(RawComparable key)
			{
				if (comparator == null)
				{
					throw new RuntimeException("Cannot search in unsorted TFile");
				}
				if (firstKey == null)
				{
					return -1;
				}
				// not found
				int ret = Utils.UpperBound(index, key, comparator);
				if (ret == index.Count)
				{
					return -1;
				}
				return ret;
			}

			/// <summary>For writing to file.</summary>
			public TFileIndex(CompareUtils.BytesComparator comparator)
			{
				index = new AList<TFile.TFileIndexEntry>();
				recordNumIndex = new AList<long>();
				this.comparator = comparator;
			}

			public virtual RawComparable GetFirstKey()
			{
				return firstKey;
			}

			public virtual TFile.Reader.Location GetLocationByRecordNum(long recNum)
			{
				int idx = Utils.UpperBound(recordNumIndex, recNum);
				long lastRecNum = (idx == 0) ? 0 : recordNumIndex[idx - 1];
				return new TFile.Reader.Location(idx, recNum - lastRecNum);
			}

			public virtual long GetRecordNumByLocation(TFile.Reader.Location location)
			{
				int blkIndex = location.GetBlockIndex();
				long lastRecNum = (blkIndex == 0) ? 0 : recordNumIndex[blkIndex - 1];
				return lastRecNum + location.GetRecordIndex();
			}

			public virtual void SetFirstKey(byte[] key, int offset, int length)
			{
				firstKey = new ByteArray(new byte[length]);
				System.Array.Copy(key, offset, firstKey.Buffer(), 0, length);
			}

			public virtual RawComparable GetLastKey()
			{
				if (index.Count == 0)
				{
					return null;
				}
				return new ByteArray(index[index.Count - 1].Buffer());
			}

			public virtual void AddEntry(TFile.TFileIndexEntry keyEntry)
			{
				index.AddItem(keyEntry);
				sum += keyEntry.Entries();
				recordNumIndex.AddItem(sum);
			}

			public virtual TFile.TFileIndexEntry GetEntry(int bid)
			{
				return index[bid];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				if (firstKey == null)
				{
					Utils.WriteVInt(@out, 0);
					return;
				}
				DataOutputBuffer dob = new DataOutputBuffer();
				Utils.WriteVInt(dob, firstKey.Size());
				dob.Write(firstKey.Buffer());
				Utils.WriteVInt(@out, dob.Size());
				@out.Write(dob.GetData(), 0, dob.GetLength());
				foreach (TFile.TFileIndexEntry entry in index)
				{
					dob.Reset();
					entry.Write(dob);
					Utils.WriteVInt(@out, dob.GetLength());
					@out.Write(dob.GetData(), 0, dob.GetLength());
				}
			}
		}

		/// <summary>TFile Data Index entry.</summary>
		/// <remarks>
		/// TFile Data Index entry. We should try to make the memory footprint of each
		/// index entry as small as possible.
		/// </remarks>
		internal sealed class TFileIndexEntry : RawComparable
		{
			internal readonly byte[] key;

			internal readonly long kvEntries;

			/// <exception cref="System.IO.IOException"/>
			public TFileIndexEntry(DataInput @in)
			{
				// count of <key, value> entries in the block.
				int len = Utils.ReadVInt(@in);
				key = new byte[len];
				@in.ReadFully(key, 0, len);
				kvEntries = Utils.ReadVLong(@in);
			}

			public TFileIndexEntry(byte[] newkey, int offset, int len, long entries)
			{
				// default entry, without any padding
				key = new byte[len];
				System.Array.Copy(newkey, offset, key, 0, len);
				this.kvEntries = entries;
			}

			public byte[] Buffer()
			{
				return key;
			}

			public int Offset()
			{
				return 0;
			}

			public int Size()
			{
				return key.Length;
			}

			internal long Entries()
			{
				return kvEntries;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(DataOutput @out)
			{
				Utils.WriteVInt(@out, key.Length);
				@out.Write(key, 0, key.Length);
				Utils.WriteVLong(@out, kvEntries);
			}
		}

		/// <summary>Dumping the TFile information.</summary>
		/// <param name="args">A list of TFile paths.</param>
		public static void Main(string[] args)
		{
			System.Console.Out.Printf("TFile Dumper (TFile %s, BCFile %s)%n", TFile.ApiVersion
				.ToString(), BCFile.ApiVersion.ToString());
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("Usage: java ... org.apache.hadoop.io.file.tfile.TFile tfile-path [tfile-path ...]"
					);
				System.Environment.Exit(0);
			}
			Configuration conf = new Configuration();
			foreach (string file in args)
			{
				System.Console.Out.WriteLine("===" + file + "===");
				try
				{
					TFileDumper.DumpInfo(file, System.Console.Out, conf);
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e, System.Console.Error);
				}
			}
		}
	}
}
