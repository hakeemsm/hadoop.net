using Sharpen;

namespace org.apache.hadoop.io.file.tfile
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
	/// <see cref="Entry.isValueLengthKnown()"/>
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
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.file.tfile.TFile
			)));

		private const string CHUNK_BUF_SIZE_ATTR = "tfile.io.chunk.size";

		private const string FS_INPUT_BUF_SIZE_ATTR = "tfile.fs.input.buffer.size";

		private const string FS_OUTPUT_BUF_SIZE_ATTR = "tfile.fs.output.buffer.size";

		internal static int getChunkBufferSize(org.apache.hadoop.conf.Configuration conf)
		{
			int ret = conf.getInt(CHUNK_BUF_SIZE_ATTR, 1024 * 1024);
			return (ret > 0) ? ret : 1024 * 1024;
		}

		internal static int getFSInputBufferSize(org.apache.hadoop.conf.Configuration conf
			)
		{
			return conf.getInt(FS_INPUT_BUF_SIZE_ATTR, 256 * 1024);
		}

		internal static int getFSOutputBufferSize(org.apache.hadoop.conf.Configuration conf
			)
		{
			return conf.getInt(FS_OUTPUT_BUF_SIZE_ATTR, 256 * 1024);
		}

		private const int MAX_KEY_SIZE = 64 * 1024;

		internal static readonly org.apache.hadoop.io.file.tfile.Utils.Version API_VERSION
			 = new org.apache.hadoop.io.file.tfile.Utils.Version((short)1, (short)0);

		/// <summary>compression: gzip</summary>
		public const string COMPRESSION_GZ = "gz";

		/// <summary>compression: lzo</summary>
		public const string COMPRESSION_LZO = "lzo";

		/// <summary>compression: none</summary>
		public const string COMPRESSION_NONE = "none";

		/// <summary>comparator: memcmp</summary>
		public const string COMPARATOR_MEMCMP = "memcmp";

		/// <summary>comparator prefix: java class</summary>
		public const string COMPARATOR_JCLASS = "jclass:";

		// 64KB
		/// <summary>Make a raw comparator from a string name.</summary>
		/// <param name="name">Comparator name</param>
		/// <returns>A RawComparable comparator.</returns>
		public static java.util.Comparator<org.apache.hadoop.io.file.tfile.RawComparable>
			 makeComparator(string name)
		{
			return org.apache.hadoop.io.file.tfile.TFile.TFileMeta.makeComparator(name);
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
		public static string[] getSupportedCompressionAlgorithms()
		{
			return org.apache.hadoop.io.file.tfile.Compression.getSupportedAlgorithms();
		}

		/// <summary>TFile Writer.</summary>
		public class Writer : java.io.Closeable
		{
			private readonly int sizeMinBlock;

			internal readonly org.apache.hadoop.io.file.tfile.TFile.TFileIndex tfileIndex;

			internal readonly org.apache.hadoop.io.file.tfile.TFile.TFileMeta tfileMeta;

			private org.apache.hadoop.io.file.tfile.BCFile.Writer writerBCF;

			internal org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender blkAppender;

			internal long blkRecordCount;

			internal org.apache.hadoop.io.BoundedByteArrayOutputStream currentKeyBufferOS;

			internal org.apache.hadoop.io.BoundedByteArrayOutputStream lastKeyBufferOS;

			private byte[] valueBuffer;

			/// <summary>Writer states.</summary>
			/// <remarks>
			/// Writer states. The state always transits in circles: READY -&gt; IN_KEY -&gt;
			/// END_KEY -&gt; IN_VALUE -&gt; READY.
			/// </remarks>
			private enum State
			{
				READY,
				IN_KEY,
				END_KEY,
				IN_VALUE,
				CLOSED
			}

			internal org.apache.hadoop.io.file.tfile.TFile.Writer.State state = org.apache.hadoop.io.file.tfile.TFile.Writer.State
				.READY;

			internal org.apache.hadoop.conf.Configuration conf;

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
			/// <see cref="TFile.getSupportedCompressionAlgorithms()"/>
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
			/// <see cref="org.apache.hadoop.io.WritableComparator"/>
			/// or
			/// <see cref="org.apache.hadoop.io.serializer.JavaSerializationComparator{T}"/>
			/// may not be directly used.
			/// One should write a wrapper class that inherits from such classes
			/// and use its default constructor to perform proper
			/// initialization.
			/// </ul>
			/// </param>
			/// <param name="conf">The configuration object.</param>
			/// <exception cref="System.IO.IOException"/>
			public Writer(org.apache.hadoop.fs.FSDataOutputStream fsdos, int minBlockSize, string
				 compressName, string comparator, org.apache.hadoop.conf.Configuration conf)
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
				tfileMeta = new org.apache.hadoop.io.file.tfile.TFile.TFileMeta(comparator);
				tfileIndex = new org.apache.hadoop.io.file.tfile.TFile.TFileIndex(tfileMeta.getComparator
					());
				writerBCF = new org.apache.hadoop.io.file.tfile.BCFile.Writer(fsdos, compressName
					, conf);
				currentKeyBufferOS = new org.apache.hadoop.io.BoundedByteArrayOutputStream(MAX_KEY_SIZE
					);
				lastKeyBufferOS = new org.apache.hadoop.io.BoundedByteArrayOutputStream(MAX_KEY_SIZE
					);
				this.conf = conf;
			}

			/// <summary>Close the Writer.</summary>
			/// <remarks>
			/// Close the Writer. Resources will be released regardless of the exceptions
			/// being thrown. Future close calls will have no effect.
			/// The underlying FSDataOutputStream is not closed.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				if ((state == org.apache.hadoop.io.file.tfile.TFile.Writer.State.CLOSED))
				{
					return;
				}
				try
				{
					// First try the normal finish.
					// Terminate upon the first Exception.
					if (errorCount == 0)
					{
						if (state != org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY)
						{
							throw new System.InvalidOperationException("Cannot close TFile in the middle of key-value insertion."
								);
						}
						finishDataBlock(true);
						// first, write out data:TFile.meta
						org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender outMeta = writerBCF.prepareMetaBlock
							(org.apache.hadoop.io.file.tfile.TFile.TFileMeta.BLOCK_NAME, COMPRESSION_NONE);
						try
						{
							tfileMeta.write(outMeta);
						}
						finally
						{
							outMeta.close();
						}
						// second, write out data:TFile.index
						org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender outIndex = writerBCF.
							prepareMetaBlock(org.apache.hadoop.io.file.tfile.TFile.TFileIndex.BLOCK_NAME);
						try
						{
							tfileIndex.write(outIndex);
						}
						finally
						{
							outIndex.close();
						}
						writerBCF.close();
					}
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.cleanup(LOG, blkAppender, writerBCF);
					blkAppender = null;
					writerBCF = null;
					state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.CLOSED;
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
			public virtual void append(byte[] key, byte[] value)
			{
				append(key, 0, key.Length, value, 0, value.Length);
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
			public virtual void append(byte[] key, int koff, int klen, byte[] value, int voff
				, int vlen)
			{
				if ((koff | klen | (koff + klen) | (key.Length - (koff + klen))) < 0)
				{
					throw new System.IndexOutOfRangeException("Bad key buffer offset-length combination."
						);
				}
				if ((voff | vlen | (voff + vlen) | (value.Length - (voff + vlen))) < 0)
				{
					throw new System.IndexOutOfRangeException("Bad value buffer offset-length combination."
						);
				}
				try
				{
					java.io.DataOutputStream dosKey = prepareAppendKey(klen);
					try
					{
						++errorCount;
						dosKey.write(key, koff, klen);
						--errorCount;
					}
					finally
					{
						dosKey.close();
					}
					java.io.DataOutputStream dosValue = prepareAppendValue(vlen);
					try
					{
						++errorCount;
						dosValue.write(value, voff, vlen);
						--errorCount;
					}
					finally
					{
						dosValue.close();
					}
				}
				finally
				{
					state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY;
				}
			}

			/// <summary>Helper class to register key after close call on key append stream.</summary>
			private class KeyRegister : java.io.DataOutputStream
			{
				private readonly int expectedLength;

				private bool closed = false;

				public KeyRegister(Writer _enclosing, int len)
					: base(this._enclosing.currentKeyBufferOS)
				{
					this._enclosing = _enclosing;
					if (len >= 0)
					{
						this._enclosing.currentKeyBufferOS.reset(len);
					}
					else
					{
						this._enclosing.currentKeyBufferOS.reset();
					}
					this.expectedLength = len;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void close()
				{
					if (this.closed == true)
					{
						return;
					}
					try
					{
						++this._enclosing.errorCount;
						byte[] key = this._enclosing.currentKeyBufferOS.getBuffer();
						int len = this._enclosing.currentKeyBufferOS.size();
						if (this.expectedLength >= 0 && this.expectedLength != len)
						{
							throw new System.IO.IOException("Incorrect key length: expected=" + this.expectedLength
								 + " actual=" + len);
						}
						org.apache.hadoop.io.file.tfile.Utils.writeVInt(this._enclosing.blkAppender, len);
						this._enclosing.blkAppender.write(key, 0, len);
						if (this._enclosing.tfileIndex.getFirstKey() == null)
						{
							this._enclosing.tfileIndex.setFirstKey(key, 0, len);
						}
						if (this._enclosing.tfileMeta.isSorted() && this._enclosing.tfileMeta.getRecordCount
							() > 0)
						{
							byte[] lastKey = this._enclosing.lastKeyBufferOS.getBuffer();
							int lastLen = this._enclosing.lastKeyBufferOS.size();
							if (this._enclosing.tfileMeta.getComparator().compare(key, 0, len, lastKey, 0, lastLen
								) < 0)
							{
								throw new System.IO.IOException("Keys are not added in sorted order");
							}
						}
						org.apache.hadoop.io.BoundedByteArrayOutputStream tmp = this._enclosing.currentKeyBufferOS;
						this._enclosing.currentKeyBufferOS = this._enclosing.lastKeyBufferOS;
						this._enclosing.lastKeyBufferOS = tmp;
						--this._enclosing.errorCount;
					}
					finally
					{
						this.closed = true;
						this._enclosing.state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.END_KEY;
					}
				}

				private readonly Writer _enclosing;
			}

			/// <summary>Helper class to register value after close call on value append stream.</summary>
			private class ValueRegister : java.io.DataOutputStream
			{
				private bool closed = false;

				public ValueRegister(Writer _enclosing, java.io.OutputStream os)
					: base(os)
				{
					this._enclosing = _enclosing;
				}

				// Avoiding flushing call to down stream.
				public override void flush()
				{
				}

				// do nothing
				/// <exception cref="System.IO.IOException"/>
				public override void close()
				{
					if (this.closed == true)
					{
						return;
					}
					try
					{
						++this._enclosing.errorCount;
						base.close();
						this._enclosing.blkRecordCount++;
						// bump up the total record count in the whole file
						this._enclosing.tfileMeta.incRecordCount();
						this._enclosing.finishDataBlock(false);
						--this._enclosing.errorCount;
					}
					finally
					{
						this.closed = true;
						this._enclosing.state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY;
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
			public virtual java.io.DataOutputStream prepareAppendKey(int length)
			{
				if (state != org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY)
				{
					throw new System.InvalidOperationException("Incorrect state to start a new key: "
						 + state.ToString());
				}
				initDataBlock();
				java.io.DataOutputStream ret = new org.apache.hadoop.io.file.tfile.TFile.Writer.KeyRegister
					(this, length);
				state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.IN_KEY;
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
			public virtual java.io.DataOutputStream prepareAppendValue(int length)
			{
				if (state != org.apache.hadoop.io.file.tfile.TFile.Writer.State.END_KEY)
				{
					throw new System.InvalidOperationException("Incorrect state to start a new value: "
						 + state.ToString());
				}
				java.io.DataOutputStream ret;
				// unknown length
				if (length < 0)
				{
					if (valueBuffer == null)
					{
						valueBuffer = new byte[getChunkBufferSize(conf)];
					}
					ret = new org.apache.hadoop.io.file.tfile.TFile.Writer.ValueRegister(this, new org.apache.hadoop.io.file.tfile.Chunk.ChunkEncoder
						(blkAppender, valueBuffer));
				}
				else
				{
					ret = new org.apache.hadoop.io.file.tfile.TFile.Writer.ValueRegister(this, new org.apache.hadoop.io.file.tfile.Chunk.SingleChunkEncoder
						(blkAppender, length));
				}
				state = org.apache.hadoop.io.file.tfile.TFile.Writer.State.IN_VALUE;
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
			/// <see cref="TFile.getSupportedCompressionAlgorithms()"/>
			/// .
			/// </param>
			/// <returns>
			/// A DataOutputStream that can be used to write Meta Block data.
			/// Closing the stream would signal the ending of the block.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockAlreadyExists">the Meta Block with the same name already exists.
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists"/>
			public virtual java.io.DataOutputStream prepareMetaBlock(string name, string compressName
				)
			{
				if (state != org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY)
				{
					throw new System.InvalidOperationException("Incorrect state to start a Meta Block: "
						 + state.ToString());
				}
				finishDataBlock(true);
				java.io.DataOutputStream outputStream = writerBCF.prepareMetaBlock(name, compressName
					);
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
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists"/>
			public virtual java.io.DataOutputStream prepareMetaBlock(string name)
			{
				if (state != org.apache.hadoop.io.file.tfile.TFile.Writer.State.READY)
				{
					throw new System.InvalidOperationException("Incorrect state to start a Meta Block: "
						 + state.ToString());
				}
				finishDataBlock(true);
				return writerBCF.prepareMetaBlock(name);
			}

			/// <summary>Check if we need to start a new data block.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void initDataBlock()
			{
				// for each new block, get a new appender
				if (blkAppender == null)
				{
					blkAppender = writerBCF.prepareDataBlock();
				}
			}

			/// <summary>Close the current data block if necessary.</summary>
			/// <param name="bForceFinish">Force the closure regardless of the block size.</param>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void finishDataBlock(bool bForceFinish)
			{
				if (blkAppender == null)
				{
					return;
				}
				// exceeded the size limit, do the compression and finish the block
				if (bForceFinish || blkAppender.getCompressedSize() >= sizeMinBlock)
				{
					// keep tracks of the last key of each data block, no padding
					// for now
					org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry keyLast = new org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
						(lastKeyBufferOS.getBuffer(), 0, lastKeyBufferOS.size(), blkRecordCount);
					tfileIndex.addEntry(keyLast);
					// close the appender
					blkAppender.close();
					blkAppender = null;
					blkRecordCount = 0;
				}
			}
		}

		/// <summary>TFile Reader.</summary>
		/// <remarks>
		/// TFile Reader. Users may only read TFiles by creating TFile.Reader.Scanner.
		/// objects. A scanner may scan the whole TFile (
		/// <see cref="createScanner()"/>
		/// ) , a portion of TFile based on byte offsets (
		/// <see cref="createScannerByByteRange(long, long)"/>
		/// ), or a portion of TFile with keys
		/// fall in a certain key range (for sorted TFile only,
		/// <see cref="createScannerByKey(byte[], byte[])"/>
		/// or
		/// <see cref="createScannerByKey(RawComparable, RawComparable)"/>
		/// ).
		/// </remarks>
		public class Reader : java.io.Closeable
		{
			internal readonly org.apache.hadoop.io.file.tfile.BCFile.Reader readerBCF;

			internal org.apache.hadoop.io.file.tfile.TFile.TFileIndex tfileIndex = null;

			internal readonly org.apache.hadoop.io.file.tfile.TFile.TFileMeta tfileMeta;

			internal readonly org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator comparator;

			private readonly org.apache.hadoop.io.file.tfile.TFile.Reader.Location begin;

			private readonly org.apache.hadoop.io.file.tfile.TFile.Reader.Location end;

			/// <summary>Location representing a virtual position in the TFile.</summary>
			internal sealed class Location : java.lang.Comparable<org.apache.hadoop.io.file.tfile.TFile.Reader.Location
				>, System.ICloneable
			{
				private int blockIndex;

				private long recordIndex;

				internal Location(int blockIndex, long recordIndex)
				{
					// The underlying BCFile reader.
					// TFile index, it is loaded lazily.
					// global begin and end locations.
					// distance/offset from the beginning of the block
					set(blockIndex, recordIndex);
				}

				internal void incRecordIndex()
				{
					++recordIndex;
				}

				internal Location(org.apache.hadoop.io.file.tfile.TFile.Reader.Location other)
				{
					set(other);
				}

				internal int getBlockIndex()
				{
					return blockIndex;
				}

				internal long getRecordIndex()
				{
					return recordIndex;
				}

				internal void set(int blockIndex, long recordIndex)
				{
					if ((blockIndex | recordIndex) < 0)
					{
						throw new System.ArgumentException("Illegal parameter for BlockLocation.");
					}
					this.blockIndex = blockIndex;
					this.recordIndex = recordIndex;
				}

				internal void set(org.apache.hadoop.io.file.tfile.TFile.Reader.Location other)
				{
					set(other.blockIndex, other.recordIndex);
				}

				/// <seealso cref="java.lang.Comparable{T}.compareTo(object)"/>
				public int compareTo(org.apache.hadoop.io.file.tfile.TFile.Reader.Location other)
				{
					return compareTo(other.blockIndex, other.recordIndex);
				}

				internal int compareTo(int bid, long rid)
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
				protected internal org.apache.hadoop.io.file.tfile.TFile.Reader.Location clone()
				{
					return new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(blockIndex, recordIndex
						);
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
					if (Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject(
						obj))
					{
						return false;
					}
					org.apache.hadoop.io.file.tfile.TFile.Reader.Location other = (org.apache.hadoop.io.file.tfile.TFile.Reader.Location
						)obj;
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

				object System.ICloneable.Clone()
				{
					return MemberwiseClone();
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
			public Reader(org.apache.hadoop.fs.FSDataInputStream fsdis, long fileLength, org.apache.hadoop.conf.Configuration
				 conf)
			{
				readerBCF = new org.apache.hadoop.io.file.tfile.BCFile.Reader(fsdis, fileLength, 
					conf);
				// first, read TFile meta
				org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader brMeta = readerBCF.getMetaBlock
					(org.apache.hadoop.io.file.tfile.TFile.TFileMeta.BLOCK_NAME);
				try
				{
					tfileMeta = new org.apache.hadoop.io.file.tfile.TFile.TFileMeta(brMeta);
				}
				finally
				{
					brMeta.close();
				}
				comparator = tfileMeta.getComparator();
				// Set begin and end locations.
				begin = new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(0, 0);
				end = new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(readerBCF.getBlockCount
					(), 0);
			}

			/// <summary>Close the reader.</summary>
			/// <remarks>
			/// Close the reader. The state of the Reader object is undefined after
			/// close. Calling close() for multiple times has no effect.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				readerBCF.close();
			}

			/// <summary>Get the begin location of the TFile.</summary>
			/// <returns>
			/// If TFile is not empty, the location of the first key-value pair.
			/// Otherwise, it returns end().
			/// </returns>
			internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location begin()
			{
				return begin;
			}

			/// <summary>Get the end location of the TFile.</summary>
			/// <returns>The location right after the last key-value pair in TFile.</returns>
			internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location end()
			{
				return end;
			}

			/// <summary>Get the string representation of the comparator.</summary>
			/// <returns>
			/// If the TFile is not sorted by keys, an empty string will be
			/// returned. Otherwise, the actual comparator string that is
			/// provided during the TFile creation time will be returned.
			/// </returns>
			public virtual string getComparatorName()
			{
				return tfileMeta.getComparatorString();
			}

			/// <summary>Is the TFile sorted?</summary>
			/// <returns>true if TFile is sorted.</returns>
			public virtual bool isSorted()
			{
				return tfileMeta.isSorted();
			}

			/// <summary>Get the number of key-value pair entries in TFile.</summary>
			/// <returns>the number of key-value pairs in TFile</returns>
			public virtual long getEntryCount()
			{
				return tfileMeta.getRecordCount();
			}

			/// <summary>Lazily loading the TFile index.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void checkTFileDataIndex()
			{
				lock (this)
				{
					if (tfileIndex == null)
					{
						org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader brIndex = readerBCF.getMetaBlock
							(org.apache.hadoop.io.file.tfile.TFile.TFileIndex.BLOCK_NAME);
						try
						{
							tfileIndex = new org.apache.hadoop.io.file.tfile.TFile.TFileIndex(readerBCF.getBlockCount
								(), brIndex, tfileMeta.getComparator());
						}
						finally
						{
							brIndex.close();
						}
					}
				}
			}

			/// <summary>Get the first key in the TFile.</summary>
			/// <returns>The first key in the TFile.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.RawComparable getFirstKey()
			{
				checkTFileDataIndex();
				return tfileIndex.getFirstKey();
			}

			/// <summary>Get the last key in the TFile.</summary>
			/// <returns>The last key in the TFile.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.RawComparable getLastKey()
			{
				checkTFileDataIndex();
				return tfileIndex.getLastKey();
			}

			/// <summary>Get a Comparator object to compare Entries.</summary>
			/// <remarks>
			/// Get a Comparator object to compare Entries. It is useful when you want
			/// stores the entries in a collection (such as PriorityQueue) and perform
			/// sorting or comparison among entries based on the keys without copying out
			/// the key.
			/// </remarks>
			/// <returns>An Entry Comparator..</returns>
			public virtual java.util.Comparator<org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry
				> getEntryComparator()
			{
				if (!isSorted())
				{
					throw new System.Exception("Entries are not comparable for unsorted TFiles");
				}
				return new _Comparator_931(this);
			}

			private sealed class _Comparator_931 : java.util.Comparator<org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry
				>
			{
				public _Comparator_931(Reader _enclosing)
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
				public int compare(org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry o1, 
					org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry o2)
				{
					return this._enclosing.comparator.compare(o1.getKeyBuffer(), 0, o1.getKeyLength()
						, o2.getKeyBuffer(), 0, o2.getKeyLength());
				}

				private readonly Reader _enclosing;
			}

			/// <summary>
			/// Get an instance of the RawComparator that is constructed based on the
			/// string comparator representation.
			/// </summary>
			/// <returns>a Comparator that can compare RawComparable's.</returns>
			public virtual java.util.Comparator<org.apache.hadoop.io.file.tfile.RawComparable
				> getComparator()
			{
				return comparator;
			}

			/// <summary>Stream access to a meta block.``</summary>
			/// <param name="name">The name of the meta block.</param>
			/// <returns>The input stream.</returns>
			/// <exception cref="System.IO.IOException">on I/O error.</exception>
			/// <exception cref="MetaBlockDoesNotExist">If the meta block with the name does not exist.
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist"/>
			public virtual java.io.DataInputStream getMetaBlock(string name)
			{
				return readerBCF.getMetaBlock(name);
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
			internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location getBlockContainsKey
				(org.apache.hadoop.io.file.tfile.RawComparable key, bool greater)
			{
				if (!isSorted())
				{
					throw new System.Exception("Seeking in unsorted TFile");
				}
				checkTFileDataIndex();
				int blkIndex = (greater) ? tfileIndex.upperBound(key) : tfileIndex.lowerBound(key
					);
				if (blkIndex < 0)
				{
					return end;
				}
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(blkIndex, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location getLocationByRecordNum
				(long recNum)
			{
				checkTFileDataIndex();
				return tfileIndex.getLocationByRecordNum(recNum);
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual long getRecordNumByLocation(org.apache.hadoop.io.file.tfile.TFile.Reader.Location
				 location)
			{
				checkTFileDataIndex();
				return tfileIndex.getRecordNumByLocation(location);
			}

			internal virtual int compareKeys(byte[] a, int o1, int l1, byte[] b, int o2, int 
				l2)
			{
				if (!isSorted())
				{
					throw new System.Exception("Cannot compare keys for unsorted TFiles.");
				}
				return comparator.compare(a, o1, l1, b, o2, l2);
			}

			internal virtual int compareKeys(org.apache.hadoop.io.file.tfile.RawComparable a, 
				org.apache.hadoop.io.file.tfile.RawComparable b)
			{
				if (!isSorted())
				{
					throw new System.Exception("Cannot compare keys for unsorted TFiles.");
				}
				return comparator.compare(a, b);
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
			internal virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location getLocationNear
				(long offset)
			{
				int blockIndex = readerBCF.getBlockIndexNear(offset);
				if (blockIndex == -1)
				{
					return end;
				}
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(blockIndex, 0);
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
			public virtual long getRecordNumNear(long offset)
			{
				return getRecordNumByLocation(getLocationNear(offset));
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
			public virtual org.apache.hadoop.io.file.tfile.RawComparable getKeyNear(long offset
				)
			{
				int blockIndex = readerBCF.getBlockIndexNear(offset);
				if (blockIndex == -1)
				{
					return null;
				}
				checkTFileDataIndex();
				return new org.apache.hadoop.io.file.tfile.ByteArray(tfileIndex.getEntry(blockIndex
					).key);
			}

			/// <summary>Get a scanner than can scan the whole TFile.</summary>
			/// <returns>
			/// The scanner object. A valid Scanner is always returned even if
			/// the TFile is empty.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScanner
				()
			{
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner(this, begin, end);
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
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScannerByByteRange
				(long offset, long length)
			{
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner(this, offset, offset
					 + length);
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
			[System.ObsoleteAttribute(@"Use createScannerByKey(byte[], byte[]) instead.")]
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScanner
				(byte[] beginKey, byte[] endKey)
			{
				return createScannerByKey(beginKey, endKey);
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
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScannerByKey
				(byte[] beginKey, byte[] endKey)
			{
				return createScannerByKey((beginKey == null) ? null : new org.apache.hadoop.io.file.tfile.ByteArray
					(beginKey, 0, beginKey.Length), (endKey == null) ? null : new org.apache.hadoop.io.file.tfile.ByteArray
					(endKey, 0, endKey.Length));
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
			[System.ObsoleteAttribute(@"Use createScannerByKey(RawComparable, RawComparable) instead."
				)]
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScanner
				(org.apache.hadoop.io.file.tfile.RawComparable beginKey, org.apache.hadoop.io.file.tfile.RawComparable
				 endKey)
			{
				return createScannerByKey(beginKey, endKey);
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
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScannerByKey
				(org.apache.hadoop.io.file.tfile.RawComparable beginKey, org.apache.hadoop.io.file.tfile.RawComparable
				 endKey)
			{
				if ((beginKey != null) && (endKey != null) && (compareKeys(beginKey, endKey) >= 0
					))
				{
					return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner(this, beginKey, beginKey
						);
				}
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner(this, beginKey, endKey
					);
			}

			/// <summary>Create a scanner that covers a range of records.</summary>
			/// <param name="beginRecNum">The RecordNum for the first record (inclusive).</param>
			/// <param name="endRecNum">
			/// The RecordNum for the last record (exclusive). To scan the whole
			/// file, either specify endRecNum==-1 or endRecNum==getEntryCount().
			/// </param>
			/// <returns>The TFile scanner that covers the specified range of records.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner createScannerByRecordNum
				(long beginRecNum, long endRecNum)
			{
				if (beginRecNum < 0)
				{
					beginRecNum = 0;
				}
				if (endRecNum < 0 || endRecNum > getEntryCount())
				{
					endRecNum = getEntryCount();
				}
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner(this, getLocationByRecordNum
					(beginRecNum), getLocationByRecordNum(endRecNum));
			}

			/// <summary>The TFile Scanner.</summary>
			/// <remarks>
			/// The TFile Scanner. The Scanner has an implicit cursor, which, upon
			/// creation, points to the first key-value pair in the scan range. If the
			/// scan range is empty, the cursor will point to the end of the scan range.
			/// <p>
			/// Use
			/// <see cref="atEnd()"/>
			/// to test whether the cursor is at the end
			/// location of the scanner.
			/// <p>
			/// Use
			/// <see cref="advance()"/>
			/// to move the cursor to the next key-value
			/// pair (or end if none exists). Use seekTo methods (
			/// <see cref="seekTo(byte[])"/>
			/// or
			/// <see cref="seekTo(byte[], int, int)"/>
			/// ) to seek to any arbitrary
			/// location in the covered range (including backward seeking). Use
			/// <see cref="rewind()"/>
			/// to seek back to the beginning of the scanner.
			/// Use
			/// <see cref="seekToEnd()"/>
			/// to seek to the end of the scanner.
			/// <p>
			/// Actual keys and values may be obtained through
			/// <see cref="Entry"/>
			/// object, which is obtained through
			/// <see cref="entry()"/>
			/// .
			/// </remarks>
			public class Scanner : java.io.Closeable
			{
				internal readonly org.apache.hadoop.io.file.tfile.TFile.Reader reader;

				private org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader blkReader;

				internal org.apache.hadoop.io.file.tfile.TFile.Reader.Location beginLocation;

				internal org.apache.hadoop.io.file.tfile.TFile.Reader.Location endLocation;

				internal org.apache.hadoop.io.file.tfile.TFile.Reader.Location currentLocation;

				internal bool valueChecked = false;

				internal readonly byte[] keyBuffer;

				internal int klen = -1;

				internal const int MAX_VAL_TRANSFER_BUF_SIZE = 128 * 1024;

				internal org.apache.hadoop.io.BytesWritable valTransferBuffer;

				internal org.apache.hadoop.io.DataInputBuffer keyDataInputStream;

				internal org.apache.hadoop.io.file.tfile.Chunk.ChunkDecoder valueBufferInputStream;

				internal java.io.DataInputStream valueDataInputStream;

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
				protected internal Scanner(org.apache.hadoop.io.file.tfile.TFile.Reader reader, long
					 offBegin, long offEnd)
					: this(reader, reader.getLocationNear(offBegin), reader.getLocationNear(offEnd))
				{
				}

				/// <summary>Constructor</summary>
				/// <param name="reader">The TFile reader object.</param>
				/// <param name="begin">Begin location of the scan.</param>
				/// <param name="end">End location of the scan.</param>
				/// <exception cref="System.IO.IOException"/>
				internal Scanner(org.apache.hadoop.io.file.tfile.TFile.Reader reader, org.apache.hadoop.io.file.tfile.TFile.Reader.Location
					 begin, org.apache.hadoop.io.file.tfile.TFile.Reader.Location end)
				{
					// The underlying TFile reader.
					// current block (null if reaching end)
					// flag to ensure value is only examined once.
					// reusable buffer for keys.
					// length of key, -1 means key is invalid.
					// vlen == -1 if unknown.
					this.reader = reader;
					// ensure the TFile index is loaded throughout the life of scanner.
					reader.checkTFileDataIndex();
					beginLocation = begin;
					endLocation = end;
					valTransferBuffer = new org.apache.hadoop.io.BytesWritable();
					// TODO: remember the longest key in a TFile, and use it to replace
					// MAX_KEY_SIZE.
					keyBuffer = new byte[MAX_KEY_SIZE];
					keyDataInputStream = new org.apache.hadoop.io.DataInputBuffer();
					valueBufferInputStream = new org.apache.hadoop.io.file.tfile.Chunk.ChunkDecoder();
					valueDataInputStream = new java.io.DataInputStream(valueBufferInputStream);
					if (beginLocation.compareTo(endLocation) >= 0)
					{
						currentLocation = new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(endLocation
							);
					}
					else
					{
						currentLocation = new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(0, 0);
						initBlock(beginLocation.getBlockIndex());
						inBlockAdvance(beginLocation.getRecordIndex());
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
				protected internal Scanner(org.apache.hadoop.io.file.tfile.TFile.Reader reader, org.apache.hadoop.io.file.tfile.RawComparable
					 beginKey, org.apache.hadoop.io.file.tfile.RawComparable endKey)
					: this(reader, (beginKey == null) ? reader.begin() : reader.getBlockContainsKey(beginKey
						, false), reader.end())
				{
					if (beginKey != null)
					{
						inBlockAdvance(beginKey, false);
						beginLocation.set(currentLocation);
					}
					if (endKey != null)
					{
						seekTo(endKey, false);
						endLocation.set(currentLocation);
						seekTo(beginLocation);
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
				public virtual bool seekTo(byte[] key)
				{
					return seekTo(key, 0, key.Length);
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
				public virtual bool seekTo(byte[] key, int keyOffset, int keyLen)
				{
					return seekTo(new org.apache.hadoop.io.file.tfile.ByteArray(key, keyOffset, keyLen
						), false);
				}

				/// <exception cref="System.IO.IOException"/>
				private bool seekTo(org.apache.hadoop.io.file.tfile.RawComparable key, bool beyond
					)
				{
					org.apache.hadoop.io.file.tfile.TFile.Reader.Location l = reader.getBlockContainsKey
						(key, beyond);
					if (l.compareTo(beginLocation) < 0)
					{
						l = beginLocation;
					}
					else
					{
						if (l.compareTo(endLocation) >= 0)
						{
							seekTo(endLocation);
							return false;
						}
					}
					// check if what we are seeking is in the later part of the current
					// block.
					if (atEnd() || (l.getBlockIndex() != currentLocation.getBlockIndex()) || (compareCursorKeyTo
						(key) >= 0))
					{
						// sorry, we must seek to a different location first.
						seekTo(l);
					}
					return inBlockAdvance(key, beyond);
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
				private void seekTo(org.apache.hadoop.io.file.tfile.TFile.Reader.Location l)
				{
					if (l.compareTo(beginLocation) < 0)
					{
						throw new System.ArgumentException("Attempt to seek before the begin location.");
					}
					if (l.compareTo(endLocation) > 0)
					{
						throw new System.ArgumentException("Attempt to seek after the end location.");
					}
					if (l.compareTo(endLocation) == 0)
					{
						parkCursorAtEnd();
						return;
					}
					if (l.getBlockIndex() != currentLocation.getBlockIndex())
					{
						// going to a totally different block
						initBlock(l.getBlockIndex());
					}
					else
					{
						if (valueChecked)
						{
							// may temporarily go beyond the last record in the block (in which
							// case the next if loop will always be true).
							inBlockAdvance(1);
						}
						if (l.getRecordIndex() < currentLocation.getRecordIndex())
						{
							initBlock(l.getBlockIndex());
						}
					}
					inBlockAdvance(l.getRecordIndex() - currentLocation.getRecordIndex());
					return;
				}

				/// <summary>Rewind to the first entry in the scanner.</summary>
				/// <remarks>
				/// Rewind to the first entry in the scanner. The entry returned by the
				/// previous entry() call will be invalid.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void rewind()
				{
					seekTo(beginLocation);
				}

				/// <summary>Seek to the end of the scanner.</summary>
				/// <remarks>
				/// Seek to the end of the scanner. The entry returned by the previous
				/// entry() call will be invalid.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void seekToEnd()
				{
					parkCursorAtEnd();
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
				public virtual void lowerBound(byte[] key)
				{
					lowerBound(key, 0, key.Length);
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
				public virtual void lowerBound(byte[] key, int keyOffset, int keyLen)
				{
					seekTo(new org.apache.hadoop.io.file.tfile.ByteArray(key, keyOffset, keyLen), false
						);
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
				public virtual void upperBound(byte[] key)
				{
					upperBound(key, 0, key.Length);
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
				public virtual void upperBound(byte[] key, int keyOffset, int keyLen)
				{
					seekTo(new org.apache.hadoop.io.file.tfile.ByteArray(key, keyOffset, keyLen), true
						);
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
				public virtual bool advance()
				{
					if (atEnd())
					{
						return false;
					}
					int curBid = currentLocation.getBlockIndex();
					long curRid = currentLocation.getRecordIndex();
					long entriesInBlock = reader.getBlockEntryCount(curBid);
					if (curRid + 1 >= entriesInBlock)
					{
						if (endLocation.compareTo(curBid + 1, 0) <= 0)
						{
							// last entry in TFile.
							parkCursorAtEnd();
						}
						else
						{
							// last entry in Block.
							initBlock(curBid + 1);
						}
					}
					else
					{
						inBlockAdvance(1);
					}
					return true;
				}

				/// <summary>Load a compressed block for reading.</summary>
				/// <remarks>Load a compressed block for reading. Expecting blockIndex is valid.</remarks>
				/// <exception cref="System.IO.IOException"/>
				private void initBlock(int blockIndex)
				{
					klen = -1;
					if (blkReader != null)
					{
						try
						{
							blkReader.close();
						}
						finally
						{
							blkReader = null;
						}
					}
					blkReader = reader.getBlockReader(blockIndex);
					currentLocation.set(blockIndex, 0);
				}

				/// <exception cref="System.IO.IOException"/>
				private void parkCursorAtEnd()
				{
					klen = -1;
					currentLocation.set(endLocation);
					if (blkReader != null)
					{
						try
						{
							blkReader.close();
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
				public virtual void close()
				{
					parkCursorAtEnd();
				}

				/// <summary>Is cursor at the end location?</summary>
				/// <returns>true if the cursor is at the end location.</returns>
				public virtual bool atEnd()
				{
					return (currentLocation.compareTo(endLocation) >= 0);
				}

				/// <summary>check whether we have already successfully obtained the key.</summary>
				/// <remarks>
				/// check whether we have already successfully obtained the key. It also
				/// initializes the valueInputStream.
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				internal virtual void checkKey()
				{
					if (klen >= 0)
					{
						return;
					}
					if (atEnd())
					{
						throw new java.io.EOFException("No key-value to read");
					}
					klen = -1;
					vlen = -1;
					valueChecked = false;
					klen = org.apache.hadoop.io.file.tfile.Utils.readVInt(blkReader);
					blkReader.readFully(keyBuffer, 0, klen);
					valueBufferInputStream.reset(blkReader);
					if (valueBufferInputStream.isLastChunk())
					{
						vlen = valueBufferInputStream.getRemain();
					}
				}

				/// <summary>Get an entry to access the key and value.</summary>
				/// <returns>The Entry object to access the key and value.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry entry()
				{
					checkKey();
					return new org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry(this);
				}

				/// <summary>Get the RecordNum corresponding to the entry pointed by the cursor.</summary>
				/// <returns>The RecordNum corresponding to the entry pointed by the cursor.</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual long getRecordNum()
				{
					return reader.getRecordNumByLocation(currentLocation);
				}

				/// <summary>Internal API.</summary>
				/// <remarks>Internal API. Comparing the key at cursor to user-specified key.</remarks>
				/// <param name="other">user-specified key.</param>
				/// <returns>
				/// negative if key at cursor is smaller than user key; 0 if equal;
				/// and positive if key at cursor greater than user key.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				internal virtual int compareCursorKeyTo(org.apache.hadoop.io.file.tfile.RawComparable
					 other)
				{
					checkKey();
					return reader.compareKeys(keyBuffer, 0, klen, other.buffer(), other.offset(), other
						.size());
				}

				/// <summary>Entry to a &lt;Key, Value&gt; pair.</summary>
				public class Entry : java.lang.Comparable<org.apache.hadoop.io.file.tfile.RawComparable
					>
				{
					/// <summary>Get the length of the key.</summary>
					/// <returns>the length of the key.</returns>
					public virtual int getKeyLength()
					{
						return this._enclosing.klen;
					}

					internal virtual byte[] getKeyBuffer()
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
					public virtual void get(org.apache.hadoop.io.BytesWritable key, org.apache.hadoop.io.BytesWritable
						 value)
					{
						this.getKey(key);
						this.getValue(value);
					}

					/// <summary>Copy the key into BytesWritable.</summary>
					/// <remarks>
					/// Copy the key into BytesWritable. The input BytesWritable will be
					/// automatically resized to the actual key size.
					/// </remarks>
					/// <param name="key">BytesWritable to hold the key.</param>
					/// <exception cref="System.IO.IOException"/>
					public virtual int getKey(org.apache.hadoop.io.BytesWritable key)
					{
						key.setSize(this.getKeyLength());
						this.getKey(key.getBytes());
						return key.getLength();
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
					public virtual long getValue(org.apache.hadoop.io.BytesWritable value)
					{
						java.io.DataInputStream dis = this.getValueStream();
						int size = 0;
						try
						{
							int remain;
							while ((remain = this._enclosing.valueBufferInputStream.getRemain()) > 0)
							{
								value.setSize(size + remain);
								dis.readFully(value.getBytes(), size, remain);
								size += remain;
							}
							return value.getLength();
						}
						finally
						{
							dis.close();
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
					public virtual int writeKey(java.io.OutputStream @out)
					{
						@out.write(this._enclosing.keyBuffer, 0, this._enclosing.klen);
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
					public virtual long writeValue(java.io.OutputStream @out)
					{
						java.io.DataInputStream dis = this.getValueStream();
						long size = 0;
						try
						{
							int chunkSize;
							while ((chunkSize = this._enclosing.valueBufferInputStream.getRemain()) > 0)
							{
								chunkSize = System.Math.min(chunkSize, org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
									.MAX_VAL_TRANSFER_BUF_SIZE);
								this._enclosing.valTransferBuffer.setSize(chunkSize);
								dis.readFully(this._enclosing.valTransferBuffer.getBytes(), 0, chunkSize);
								@out.write(this._enclosing.valTransferBuffer.getBytes(), 0, chunkSize);
								size += chunkSize;
							}
							return size;
						}
						finally
						{
							dis.close();
						}
					}

					/// <summary>Copy the key into user supplied buffer.</summary>
					/// <param name="buf">
					/// The buffer supplied by user. The length of the buffer must
					/// not be shorter than the key length.
					/// </param>
					/// <returns>The length of the key.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int getKey(byte[] buf)
					{
						return this.getKey(buf, 0);
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
					public virtual int getKey(byte[] buf, int offset)
					{
						if ((offset | (buf.Length - offset - this._enclosing.klen)) < 0)
						{
							throw new System.IndexOutOfRangeException("Bufer not enough to store the key");
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
					public virtual java.io.DataInputStream getKeyStream()
					{
						this._enclosing.keyDataInputStream.reset(this._enclosing.keyBuffer, this._enclosing
							.klen);
						return this._enclosing.keyDataInputStream;
					}

					/// <summary>Get the length of the value.</summary>
					/// <remarks>
					/// Get the length of the value. isValueLengthKnown() must be tested
					/// true.
					/// </remarks>
					/// <returns>the length of the value.</returns>
					public virtual int getValueLength()
					{
						if (this._enclosing.vlen >= 0)
						{
							return this._enclosing.vlen;
						}
						throw new System.Exception("Value length unknown.");
					}

					/// <summary>Copy value into user-supplied buffer.</summary>
					/// <remarks>
					/// Copy value into user-supplied buffer. User supplied buffer must be
					/// large enough to hold the whole value. The value part of the key-value
					/// pair pointed by the current cursor is not cached and can only be
					/// examined once. Calling any of the following functions more than once
					/// without moving the cursor will result in exception:
					/// <see cref="getValue(byte[])"/>
					/// ,
					/// <see cref="getValue(byte[], int)"/>
					/// ,
					/// <see cref="getValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>
					/// the length of the value. Does not require
					/// isValueLengthKnown() to be true.
					/// </returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int getValue(byte[] buf)
					{
						return this.getValue(buf, 0);
					}

					/// <summary>Copy value into user-supplied buffer.</summary>
					/// <remarks>
					/// Copy value into user-supplied buffer. User supplied buffer must be
					/// large enough to hold the whole value (starting from the offset). The
					/// value part of the key-value pair pointed by the current cursor is not
					/// cached and can only be examined once. Calling any of the following
					/// functions more than once without moving the cursor will result in
					/// exception:
					/// <see cref="getValue(byte[])"/>
					/// ,
					/// <see cref="getValue(byte[], int)"/>
					/// ,
					/// <see cref="getValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>
					/// the length of the value. Does not require
					/// isValueLengthKnown() to be true.
					/// </returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual int getValue(byte[] buf, int offset)
					{
						java.io.DataInputStream dis = this.getValueStream();
						try
						{
							if (this.isValueLengthKnown())
							{
								if ((offset | (buf.Length - offset - this._enclosing.vlen)) < 0)
								{
									throw new System.IndexOutOfRangeException("Buffer too small to hold value");
								}
								dis.readFully(buf, offset, this._enclosing.vlen);
								return this._enclosing.vlen;
							}
							int nextOffset = offset;
							while (nextOffset < buf.Length)
							{
								int n = dis.read(buf, nextOffset, buf.Length - nextOffset);
								if (n < 0)
								{
									break;
								}
								nextOffset += n;
							}
							if (dis.read() >= 0)
							{
								// attempt to read one more byte to determine whether we reached
								// the
								// end or not.
								throw new System.IndexOutOfRangeException("Buffer too small to hold value");
							}
							return nextOffset - offset;
						}
						finally
						{
							dis.close();
						}
					}

					/// <summary>Stream access to value.</summary>
					/// <remarks>
					/// Stream access to value. The value part of the key-value pair pointed
					/// by the current cursor is not cached and can only be examined once.
					/// Calling any of the following functions more than once without moving
					/// the cursor will result in exception:
					/// <see cref="getValue(byte[])"/>
					/// ,
					/// <see cref="getValue(byte[], int)"/>
					/// ,
					/// <see cref="getValueStream()"/>
					/// .
					/// </remarks>
					/// <returns>The input stream for reading the value.</returns>
					/// <exception cref="System.IO.IOException"/>
					public virtual java.io.DataInputStream getValueStream()
					{
						if (this._enclosing.valueChecked == true)
						{
							throw new System.InvalidOperationException("Attempt to examine value multiple times."
								);
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
					public virtual bool isValueLengthKnown()
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
					public virtual int compareTo(byte[] buf)
					{
						return this.compareTo(buf, 0, buf.Length);
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
					public virtual int compareTo(byte[] buf, int offset, int length)
					{
						return this.compareTo(new org.apache.hadoop.io.file.tfile.ByteArray(buf, offset, 
							length));
					}

					/// <summary>Compare an entry with a RawComparable object.</summary>
					/// <remarks>
					/// Compare an entry with a RawComparable object. This is useful when
					/// Entries are stored in a collection, and we want to compare a user
					/// supplied key.
					/// </remarks>
					public virtual int compareTo(org.apache.hadoop.io.file.tfile.RawComparable key)
					{
						return this._enclosing.reader.compareKeys(this._enclosing.keyBuffer, 0, this.getKeyLength
							(), key.buffer(), key.offset(), key.size());
					}

					/// <summary>Compare whether this and other points to the same key value.</summary>
					public override bool Equals(object other)
					{
						if (this == other)
						{
							return true;
						}
						if (!(other is org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry))
						{
							return false;
						}
						return ((org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner.Entry)other).compareTo
							(this._enclosing.keyBuffer, 0, this.getKeyLength()) == 0;
					}

					public override int GetHashCode()
					{
						return org.apache.hadoop.io.WritableComparator.hashBytes(this._enclosing.keyBuffer
							, 0, this.getKeyLength());
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
				private void inBlockAdvance(long n)
				{
					for (long i = 0; i < n; ++i)
					{
						checkKey();
						if (!valueBufferInputStream.isClosed())
						{
							valueBufferInputStream.close();
						}
						klen = -1;
						currentLocation.incRecordIndex();
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
				private bool inBlockAdvance(org.apache.hadoop.io.file.tfile.RawComparable key, bool
					 greater)
				{
					int curBid = currentLocation.getBlockIndex();
					long entryInBlock = reader.getBlockEntryCount(curBid);
					if (curBid == endLocation.getBlockIndex())
					{
						entryInBlock = endLocation.getRecordIndex();
					}
					while (currentLocation.getRecordIndex() < entryInBlock)
					{
						int cmp = compareCursorKeyTo(key);
						if (cmp > 0)
						{
							return false;
						}
						if (cmp == 0 && !greater)
						{
							return true;
						}
						if (!valueBufferInputStream.isClosed())
						{
							valueBufferInputStream.close();
						}
						klen = -1;
						currentLocation.incRecordIndex();
					}
					throw new System.Exception("Cannot find matching key in block.");
				}
			}

			internal virtual long getBlockEntryCount(int curBid)
			{
				return tfileIndex.getEntry(curBid).entries();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader getBlockReader
				(int blockIndex)
			{
				return readerBCF.getDataBlock(blockIndex);
			}
		}

		/// <summary>Data structure representing "TFile.meta" meta block.</summary>
		internal sealed class TFileMeta
		{
			internal const string BLOCK_NAME = "TFile.meta";

			internal readonly org.apache.hadoop.io.file.tfile.Utils.Version version;

			private long recordCount;

			private readonly string strComparator;

			private readonly org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator comparator;

			public TFileMeta(string comparator)
			{
				// ctor for writes
				// set fileVersion to API version when we create it.
				version = org.apache.hadoop.io.file.tfile.TFile.API_VERSION;
				recordCount = 0;
				strComparator = (comparator == null) ? string.Empty : comparator;
				this.comparator = makeComparator(strComparator);
			}

			/// <exception cref="System.IO.IOException"/>
			public TFileMeta(java.io.DataInput @in)
			{
				// ctor for reads
				version = new org.apache.hadoop.io.file.tfile.Utils.Version(@in);
				if (!version.compatibleWith(org.apache.hadoop.io.file.tfile.TFile.API_VERSION))
				{
					throw new System.Exception("Incompatible TFile fileVersion.");
				}
				recordCount = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
				strComparator = org.apache.hadoop.io.file.tfile.Utils.readString(@in);
				comparator = makeComparator(strComparator);
			}

			internal static org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator makeComparator
				(string comparator)
			{
				if (comparator.Length == 0)
				{
					// unsorted keys
					return null;
				}
				if (comparator.Equals(COMPARATOR_MEMCMP))
				{
					// default comparator
					return new org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator(new org.apache.hadoop.io.file.tfile.CompareUtils.MemcmpRawComparator
						());
				}
				else
				{
					if (comparator.StartsWith(COMPARATOR_JCLASS))
					{
						string compClassName = Sharpen.Runtime.substring(comparator, COMPARATOR_JCLASS.Length
							).Trim();
						try
						{
							java.lang.Class compClass = java.lang.Class.forName(compClassName);
							// use its default ctor to create an instance
							return new org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator((org.apache.hadoop.io.RawComparator
								<object>)compClass.newInstance());
						}
						catch (System.Exception e)
						{
							throw new System.ArgumentException("Failed to instantiate comparator: " + comparator
								 + "(" + e.ToString() + ")");
						}
					}
					else
					{
						throw new System.ArgumentException("Unsupported comparator: " + comparator);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.TFile.API_VERSION.write(@out);
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, recordCount);
				org.apache.hadoop.io.file.tfile.Utils.writeString(@out, strComparator);
			}

			public long getRecordCount()
			{
				return recordCount;
			}

			public void incRecordCount()
			{
				++recordCount;
			}

			public bool isSorted()
			{
				return !strComparator.isEmpty();
			}

			public string getComparatorString()
			{
				return strComparator;
			}

			public org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator getComparator
				()
			{
				return comparator;
			}

			public org.apache.hadoop.io.file.tfile.Utils.Version getVersion()
			{
				return version;
			}
		}

		/// <summary>Data structure representing "TFile.index" meta block.</summary>
		internal class TFileIndex
		{
			internal const string BLOCK_NAME = "TFile.index";

			private org.apache.hadoop.io.file.tfile.ByteArray firstKey;

			private readonly System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
				> index;

			private readonly System.Collections.Generic.List<long> recordNumIndex;

			private readonly org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator comparator;

			private long sum = 0;

			/// <summary>For reading from file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public TFileIndex(int entryCount, java.io.DataInput @in, org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator
				 comparator)
			{
				// END: class MetaTFileMeta
				index = new System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
					>(entryCount);
				recordNumIndex = new System.Collections.Generic.List<long>(entryCount);
				int size = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
				// size for the first key entry.
				if (size > 0)
				{
					byte[] buffer = new byte[size];
					@in.readFully(buffer);
					java.io.DataInputStream firstKeyInputStream = new java.io.DataInputStream(new java.io.ByteArrayInputStream
						(buffer, 0, size));
					int firstKeyLength = org.apache.hadoop.io.file.tfile.Utils.readVInt(firstKeyInputStream
						);
					firstKey = new org.apache.hadoop.io.file.tfile.ByteArray(new byte[firstKeyLength]
						);
					firstKeyInputStream.readFully(firstKey.buffer());
					for (int i = 0; i < entryCount; i++)
					{
						size = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
						if (buffer.Length < size)
						{
							buffer = new byte[size];
						}
						@in.readFully(buffer, 0, size);
						org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry idx = new org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
							(new java.io.DataInputStream(new java.io.ByteArrayInputStream(buffer, 0, size)));
						index.add(idx);
						sum += idx.entries();
						recordNumIndex.add(sum);
					}
				}
				else
				{
					if (entryCount != 0)
					{
						throw new System.Exception("Internal error");
					}
				}
				this.comparator = comparator;
			}

			/// <param name="key">input key.</param>
			/// <returns>
			/// the ID of the first block that contains key &gt;= input key. Or -1
			/// if no such block exists.
			/// </returns>
			public virtual int lowerBound(org.apache.hadoop.io.file.tfile.RawComparable key)
			{
				if (comparator == null)
				{
					throw new System.Exception("Cannot search in unsorted TFile");
				}
				if (firstKey == null)
				{
					return -1;
				}
				// not found
				int ret = org.apache.hadoop.io.file.tfile.Utils.lowerBound(index, key, comparator
					);
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
			public virtual int upperBound(org.apache.hadoop.io.file.tfile.RawComparable key)
			{
				if (comparator == null)
				{
					throw new System.Exception("Cannot search in unsorted TFile");
				}
				if (firstKey == null)
				{
					return -1;
				}
				// not found
				int ret = org.apache.hadoop.io.file.tfile.Utils.upperBound(index, key, comparator
					);
				if (ret == index.Count)
				{
					return -1;
				}
				return ret;
			}

			/// <summary>For writing to file.</summary>
			public TFileIndex(org.apache.hadoop.io.file.tfile.CompareUtils.BytesComparator comparator
				)
			{
				index = new System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
					>();
				recordNumIndex = new System.Collections.Generic.List<long>();
				this.comparator = comparator;
			}

			public virtual org.apache.hadoop.io.file.tfile.RawComparable getFirstKey()
			{
				return firstKey;
			}

			public virtual org.apache.hadoop.io.file.tfile.TFile.Reader.Location getLocationByRecordNum
				(long recNum)
			{
				int idx = org.apache.hadoop.io.file.tfile.Utils.upperBound(recordNumIndex, recNum
					);
				long lastRecNum = (idx == 0) ? 0 : recordNumIndex[idx - 1];
				return new org.apache.hadoop.io.file.tfile.TFile.Reader.Location(idx, recNum - lastRecNum
					);
			}

			public virtual long getRecordNumByLocation(org.apache.hadoop.io.file.tfile.TFile.Reader.Location
				 location)
			{
				int blkIndex = location.getBlockIndex();
				long lastRecNum = (blkIndex == 0) ? 0 : recordNumIndex[blkIndex - 1];
				return lastRecNum + location.getRecordIndex();
			}

			public virtual void setFirstKey(byte[] key, int offset, int length)
			{
				firstKey = new org.apache.hadoop.io.file.tfile.ByteArray(new byte[length]);
				System.Array.Copy(key, offset, firstKey.buffer(), 0, length);
			}

			public virtual org.apache.hadoop.io.file.tfile.RawComparable getLastKey()
			{
				if (index.Count == 0)
				{
					return null;
				}
				return new org.apache.hadoop.io.file.tfile.ByteArray(index[index.Count - 1].buffer
					());
			}

			public virtual void addEntry(org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry
				 keyEntry)
			{
				index.add(keyEntry);
				sum += keyEntry.entries();
				recordNumIndex.add(sum);
			}

			public virtual org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry getEntry(int
				 bid)
			{
				return index[bid];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				if (firstKey == null)
				{
					org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, 0);
					return;
				}
				org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
					();
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(dob, firstKey.size());
				dob.write(firstKey.buffer());
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, dob.size());
				@out.write(dob.getData(), 0, dob.getLength());
				foreach (org.apache.hadoop.io.file.tfile.TFile.TFileIndexEntry entry in index)
				{
					dob.reset();
					entry.write(dob);
					org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, dob.getLength());
					@out.write(dob.getData(), 0, dob.getLength());
				}
			}
		}

		/// <summary>TFile Data Index entry.</summary>
		/// <remarks>
		/// TFile Data Index entry. We should try to make the memory footprint of each
		/// index entry as small as possible.
		/// </remarks>
		internal sealed class TFileIndexEntry : org.apache.hadoop.io.file.tfile.RawComparable
		{
			internal readonly byte[] key;

			internal readonly long kvEntries;

			/// <exception cref="System.IO.IOException"/>
			public TFileIndexEntry(java.io.DataInput @in)
			{
				// count of <key, value> entries in the block.
				int len = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
				key = new byte[len];
				@in.readFully(key, 0, len);
				kvEntries = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
			}

			public TFileIndexEntry(byte[] newkey, int offset, int len, long entries)
			{
				// default entry, without any padding
				key = new byte[len];
				System.Array.Copy(newkey, offset, key, 0, len);
				this.kvEntries = entries;
			}

			public byte[] buffer()
			{
				return key;
			}

			public int offset()
			{
				return 0;
			}

			public int size()
			{
				return key.Length;
			}

			internal long entries()
			{
				return kvEntries;
			}

			/// <exception cref="System.IO.IOException"/>
			public void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, key.Length);
				@out.write(key, 0, key.Length);
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, kvEntries);
			}
		}

		/// <summary>Dumping the TFile information.</summary>
		/// <param name="args">A list of TFile paths.</param>
		public static void Main(string[] args)
		{
			System.Console.Out.printf("TFile Dumper (TFile %s, BCFile %s)%n", org.apache.hadoop.io.file.tfile.TFile
				.API_VERSION.ToString(), org.apache.hadoop.io.file.tfile.BCFile.API_VERSION.ToString
				());
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("Usage: java ... org.apache.hadoop.io.file.tfile.TFile tfile-path [tfile-path ...]"
					);
				System.Environment.Exit(0);
			}
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			foreach (string file in args)
			{
				System.Console.Out.WriteLine("===" + file + "===");
				try
				{
					org.apache.hadoop.io.file.tfile.TFileDumper.dumpInfo(file, System.Console.Out, conf
						);
				}
				catch (System.IO.IOException e)
				{
					Sharpen.Runtime.printStackTrace(e, System.Console.Error);
				}
			}
		}
	}
}
