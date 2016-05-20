using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Block Compressed file, the underlying physical storage layer for TFile.</summary>
	/// <remarks>
	/// Block Compressed file, the underlying physical storage layer for TFile.
	/// BCFile provides the basic block level compression for the data block and meta
	/// blocks. It is separated from TFile as it may be used for other
	/// block-compressed file implementation.
	/// </remarks>
	internal sealed class BCFile
	{
		internal static readonly org.apache.hadoop.io.file.tfile.Utils.Version API_VERSION
			 = new org.apache.hadoop.io.file.tfile.Utils.Version((short)1, (short)0);

		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.file.tfile.BCFile
			)));

		/// <summary>Prevent the instantiation of BCFile objects.</summary>
		private BCFile()
		{
		}

		/// <summary>BCFile writer, the entry point for creating a new BCFile.</summary>
		public class Writer : java.io.Closeable
		{
			private readonly org.apache.hadoop.fs.FSDataOutputStream @out;

			private readonly org.apache.hadoop.conf.Configuration conf;

			internal readonly org.apache.hadoop.io.file.tfile.BCFile.DataIndex dataIndex;

			internal readonly org.apache.hadoop.io.file.tfile.BCFile.MetaIndex metaIndex;

			internal bool blkInProgress = false;

			private bool metaBlkSeen = false;

			private bool closed = false;

			internal long errorCount = 0;

			private org.apache.hadoop.io.BytesWritable fsOutputBuffer;

			/// <summary>Call-back interface to register a block after a block is closed.</summary>
			private interface BlockRegister
			{
				// the current version of BCFile impl, increment them (major or minor) made
				// enough changes
				// nothing
				// the single meta block containing index of compressed data blocks
				// index for meta blocks
				// reusable buffers.
				/// <summary>Register a block that is fully closed.</summary>
				/// <param name="raw">The size of block in terms of uncompressed bytes.</param>
				/// <param name="offsetStart">The start offset of the block.</param>
				/// <param name="offsetEnd">
				/// One byte after the end of the block. Compressed block size is
				/// offsetEnd - offsetStart.
				/// </param>
				void register(long raw, long offsetStart, long offsetEnd);
			}

			/// <summary>
			/// Intermediate class that maintain the state of a Writable Compression
			/// Block.
			/// </summary>
			private sealed class WBlockState
			{
				private readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm compressAlgo;

				private org.apache.hadoop.io.compress.Compressor compressor;

				private readonly org.apache.hadoop.fs.FSDataOutputStream fsOut;

				private readonly long posStart;

				private readonly org.apache.hadoop.io.file.tfile.SimpleBufferedOutputStream fsBufferedOutput;

				private java.io.OutputStream @out;

				/// <param name="compressionAlgo">The compression algorithm to be used to for compression.
				/// 	</param>
				/// <exception cref="System.IO.IOException"/>
				public WBlockState(org.apache.hadoop.io.file.tfile.Compression.Algorithm compressionAlgo
					, org.apache.hadoop.fs.FSDataOutputStream fsOut, org.apache.hadoop.io.BytesWritable
					 fsOutputBuffer, org.apache.hadoop.conf.Configuration conf)
				{
					// !null only if using native
					// Hadoop compression
					this.compressAlgo = compressionAlgo;
					this.fsOut = fsOut;
					this.posStart = fsOut.getPos();
					fsOutputBuffer.setCapacity(org.apache.hadoop.io.file.tfile.TFile.getFSOutputBufferSize
						(conf));
					this.fsBufferedOutput = new org.apache.hadoop.io.file.tfile.SimpleBufferedOutputStream
						(this.fsOut, fsOutputBuffer.getBytes());
					this.compressor = compressAlgo.getCompressor();
					try
					{
						this.@out = compressionAlgo.createCompressionStream(fsBufferedOutput, compressor, 
							0);
					}
					catch (System.IO.IOException e)
					{
						compressAlgo.returnCompressor(compressor);
						throw;
					}
				}

				/// <summary>Get the output stream for BlockAppender's consumption.</summary>
				/// <returns>the output stream suitable for writing block data.</returns>
				internal java.io.OutputStream getOutputStream()
				{
					return @out;
				}

				/// <summary>Get the current position in file.</summary>
				/// <returns>The current byte offset in underlying file.</returns>
				/// <exception cref="System.IO.IOException"/>
				internal long getCurrentPos()
				{
					return fsOut.getPos() + fsBufferedOutput.size();
				}

				internal long getStartPos()
				{
					return posStart;
				}

				/// <summary>Current size of compressed data.</summary>
				/// <returns/>
				/// <exception cref="System.IO.IOException"/>
				internal long getCompressedSize()
				{
					long ret = getCurrentPos() - posStart;
					return ret;
				}

				/// <summary>Finishing up the current block.</summary>
				/// <exception cref="System.IO.IOException"/>
				public void finish()
				{
					try
					{
						if (@out != null)
						{
							@out.flush();
							@out = null;
						}
					}
					finally
					{
						compressAlgo.returnCompressor(compressor);
						compressor = null;
					}
				}
			}

			/// <summary>Access point to stuff data into a block.</summary>
			/// <remarks>
			/// Access point to stuff data into a block.
			/// TODO: Change DataOutputStream to something else that tracks the size as
			/// long instead of int. Currently, we will wrap around if the row block size
			/// is greater than 4GB.
			/// </remarks>
			public class BlockAppender : java.io.DataOutputStream
			{
				private readonly org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockRegister blockRegister;

				private readonly org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState wBlkState;

				private bool closed = false;

				/// <summary>Constructor</summary>
				/// <param name="register">the block register, which is called when the block is closed.
				/// 	</param>
				/// <param name="wbs">The writable compression block state.</param>
				internal BlockAppender(Writer _enclosing, org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockRegister
					 register, org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState wbs)
					: base(wbs.getOutputStream())
				{
					this._enclosing = _enclosing;
					this.blockRegister = register;
					this.wBlkState = wbs;
				}

				/// <summary>Get the raw size of the block.</summary>
				/// <returns>
				/// the number of uncompressed bytes written through the
				/// BlockAppender so far.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual long getRawSize()
				{
					return this.size() & unchecked((long)(0x00000000ffffffffL));
				}

				/// <summary>Get the compressed size of the block in progress.</summary>
				/// <returns>
				/// the number of compressed bytes written to the underlying FS
				/// file. The size may be smaller than actual need to compress the
				/// all data written due to internal buffering inside the
				/// compressor.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual long getCompressedSize()
				{
					return this.wBlkState.getCompressedSize();
				}

				public override void flush()
				{
				}

				// The down stream is a special kind of stream that finishes a
				// compression block upon flush. So we disable flush() here.
				/// <summary>Signaling the end of write to the block.</summary>
				/// <remarks>
				/// Signaling the end of write to the block. The block register will be
				/// called for registering the finished block.
				/// </remarks>
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
						this.wBlkState.finish();
						this.blockRegister.register(this.getRawSize(), this.wBlkState.getStartPos(), this
							.wBlkState.getCurrentPos());
						--this._enclosing.errorCount;
					}
					finally
					{
						this.closed = true;
						this._enclosing.blkInProgress = false;
					}
				}

				private readonly Writer _enclosing;
			}

			/// <summary>Constructor</summary>
			/// <param name="fout">FS output stream.</param>
			/// <param name="compressionName">
			/// Name of the compression algorithm, which will be used for all
			/// data blocks.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			/// <seealso cref="Compression.getSupportedAlgorithms()"/>
			public Writer(org.apache.hadoop.fs.FSDataOutputStream fout, string compressionName
				, org.apache.hadoop.conf.Configuration conf)
			{
				if (fout.getPos() != 0)
				{
					throw new System.IO.IOException("Output file not at zero offset.");
				}
				this.@out = fout;
				this.conf = conf;
				dataIndex = new org.apache.hadoop.io.file.tfile.BCFile.DataIndex(compressionName);
				metaIndex = new org.apache.hadoop.io.file.tfile.BCFile.MetaIndex();
				fsOutputBuffer = new org.apache.hadoop.io.BytesWritable();
				org.apache.hadoop.io.file.tfile.BCFile.Magic.write(fout);
			}

			/// <summary>Close the BCFile Writer.</summary>
			/// <remarks>
			/// Close the BCFile Writer. Attempting to use the Writer after calling
			/// <code>close</code> is not allowed and may lead to undetermined results.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				if (closed == true)
				{
					return;
				}
				try
				{
					if (errorCount == 0)
					{
						if (blkInProgress == true)
						{
							throw new System.InvalidOperationException("Close() called with active block appender."
								);
						}
						// add metaBCFileIndex to metaIndex as the last meta block
						org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender appender = prepareMetaBlock
							(org.apache.hadoop.io.file.tfile.BCFile.DataIndex.BLOCK_NAME, getDefaultCompressionAlgorithm
							());
						try
						{
							dataIndex.write(appender);
						}
						finally
						{
							appender.close();
						}
						long offsetIndexMeta = @out.getPos();
						metaIndex.write(@out);
						// Meta Index and the trailing section are written out directly.
						@out.writeLong(offsetIndexMeta);
						API_VERSION.write(@out);
						org.apache.hadoop.io.file.tfile.BCFile.Magic.write(@out);
						@out.flush();
					}
				}
				finally
				{
					closed = true;
				}
			}

			private org.apache.hadoop.io.file.tfile.Compression.Algorithm getDefaultCompressionAlgorithm
				()
			{
				return dataIndex.getDefaultCompressionAlgorithm();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists"/>
			private org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender prepareMetaBlock
				(string name, org.apache.hadoop.io.file.tfile.Compression.Algorithm compressAlgo
				)
			{
				if (blkInProgress == true)
				{
					throw new System.InvalidOperationException("Cannot create Meta Block until previous block is closed."
						);
				}
				if (metaIndex.getMetaByName(name) != null)
				{
					throw new org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists("name=" + name);
				}
				org.apache.hadoop.io.file.tfile.BCFile.Writer.MetaBlockRegister mbr = new org.apache.hadoop.io.file.tfile.BCFile.Writer.MetaBlockRegister
					(this, name, compressAlgo);
				org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState wbs = new org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState
					(compressAlgo, @out, fsOutputBuffer, conf);
				org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender ba = new org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender
					(this, mbr, wbs);
				blkInProgress = true;
				metaBlkSeen = true;
				return ba;
			}

			/// <summary>
			/// Create a Meta Block and obtain an output stream for adding data into the
			/// block.
			/// </summary>
			/// <remarks>
			/// Create a Meta Block and obtain an output stream for adding data into the
			/// block. There can only be one BlockAppender stream active at any time.
			/// Regular Blocks may not be created after the first Meta Blocks. The caller
			/// must call BlockAppender.close() to conclude the block creation.
			/// </remarks>
			/// <param name="name">
			/// The name of the Meta Block. The name must not conflict with
			/// existing Meta Blocks.
			/// </param>
			/// <param name="compressionName">The name of the compression algorithm to be used.</param>
			/// <returns>The BlockAppender stream</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockAlreadyExists">If the meta block with the name already exists.
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists"/>
			public virtual org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender prepareMetaBlock
				(string name, string compressionName)
			{
				return prepareMetaBlock(name, org.apache.hadoop.io.file.tfile.Compression.getCompressionAlgorithmByName
					(compressionName));
			}

			/// <summary>
			/// Create a Meta Block and obtain an output stream for adding data into the
			/// block.
			/// </summary>
			/// <remarks>
			/// Create a Meta Block and obtain an output stream for adding data into the
			/// block. The Meta Block will be compressed with the same compression
			/// algorithm as data blocks. There can only be one BlockAppender stream
			/// active at any time. Regular Blocks may not be created after the first
			/// Meta Blocks. The caller must call BlockAppender.close() to conclude the
			/// block creation.
			/// </remarks>
			/// <param name="name">
			/// The name of the Meta Block. The name must not conflict with
			/// existing Meta Blocks.
			/// </param>
			/// <returns>The BlockAppender stream</returns>
			/// <exception cref="MetaBlockAlreadyExists">If the meta block with the name already exists.
			/// 	</exception>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists"/>
			public virtual org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender prepareMetaBlock
				(string name)
			{
				return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
			}

			/// <summary>
			/// Create a Data Block and obtain an output stream for adding data into the
			/// block.
			/// </summary>
			/// <remarks>
			/// Create a Data Block and obtain an output stream for adding data into the
			/// block. There can only be one BlockAppender stream active at any time.
			/// Data Blocks may not be created after the first Meta Blocks. The caller
			/// must call BlockAppender.close() to conclude the block creation.
			/// </remarks>
			/// <returns>The BlockAppender stream</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender prepareDataBlock
				()
			{
				if (blkInProgress == true)
				{
					throw new System.InvalidOperationException("Cannot create Data Block until previous block is closed."
						);
				}
				if (metaBlkSeen == true)
				{
					throw new System.InvalidOperationException("Cannot create Data Block after Meta Blocks."
						);
				}
				org.apache.hadoop.io.file.tfile.BCFile.Writer.DataBlockRegister dbr = new org.apache.hadoop.io.file.tfile.BCFile.Writer.DataBlockRegister
					(this);
				org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState wbs = new org.apache.hadoop.io.file.tfile.BCFile.Writer.WBlockState
					(getDefaultCompressionAlgorithm(), @out, fsOutputBuffer, conf);
				org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender ba = new org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockAppender
					(this, dbr, wbs);
				blkInProgress = true;
				return ba;
			}

			/// <summary>
			/// Callback to make sure a meta block is added to the internal list when its
			/// stream is closed.
			/// </summary>
			private class MetaBlockRegister : org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockRegister
			{
				private readonly string name;

				private readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm compressAlgo;

				internal MetaBlockRegister(Writer _enclosing, string name, org.apache.hadoop.io.file.tfile.Compression.Algorithm
					 compressAlgo)
				{
					this._enclosing = _enclosing;
					this.name = name;
					this.compressAlgo = compressAlgo;
				}

				public virtual void register(long raw, long begin, long end)
				{
					this._enclosing.metaIndex.addEntry(new org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
						(this.name, this.compressAlgo, new org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
						(begin, end - begin, raw)));
				}

				private readonly Writer _enclosing;
			}

			/// <summary>
			/// Callback to make sure a data block is added to the internal list when
			/// it's being closed.
			/// </summary>
			private class DataBlockRegister : org.apache.hadoop.io.file.tfile.BCFile.Writer.BlockRegister
			{
				internal DataBlockRegister(Writer _enclosing)
				{
					this._enclosing = _enclosing;
				}

				// do nothing
				public virtual void register(long raw, long begin, long end)
				{
					this._enclosing.dataIndex.addBlockRegion(new org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
						(begin, end - begin, raw));
				}

				private readonly Writer _enclosing;
			}
		}

		/// <summary>BCFile Reader, interface to read the file's data and meta blocks.</summary>
		public class Reader : java.io.Closeable
		{
			private readonly org.apache.hadoop.fs.FSDataInputStream @in;

			private readonly org.apache.hadoop.conf.Configuration conf;

			internal readonly org.apache.hadoop.io.file.tfile.BCFile.DataIndex dataIndex;

			internal readonly org.apache.hadoop.io.file.tfile.BCFile.MetaIndex metaIndex;

			internal readonly org.apache.hadoop.io.file.tfile.Utils.Version version;

			/// <summary>
			/// Intermediate class that maintain the state of a Readable Compression
			/// Block.
			/// </summary>
			private sealed class RBlockState
			{
				private readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm compressAlgo;

				private org.apache.hadoop.io.compress.Decompressor decompressor;

				private readonly org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region;

				private readonly java.io.InputStream @in;

				/// <exception cref="System.IO.IOException"/>
				public RBlockState(org.apache.hadoop.io.file.tfile.Compression.Algorithm compressionAlgo
					, org.apache.hadoop.fs.FSDataInputStream fsin, org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
					 region, org.apache.hadoop.conf.Configuration conf)
				{
					// Index for meta blocks
					this.compressAlgo = compressionAlgo;
					this.region = region;
					this.decompressor = compressionAlgo.getDecompressor();
					try
					{
						this.@in = compressAlgo.createDecompressionStream(new org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream
							(fsin, this.region.getOffset(), this.region.getCompressedSize()), decompressor, 
							org.apache.hadoop.io.file.tfile.TFile.getFSInputBufferSize(conf));
					}
					catch (System.IO.IOException e)
					{
						compressAlgo.returnDecompressor(decompressor);
						throw;
					}
				}

				/// <summary>Get the output stream for BlockAppender's consumption.</summary>
				/// <returns>the output stream suitable for writing block data.</returns>
				public java.io.InputStream getInputStream()
				{
					return @in;
				}

				public string getCompressionName()
				{
					return compressAlgo.getName();
				}

				public org.apache.hadoop.io.file.tfile.BCFile.BlockRegion getBlockRegion()
				{
					return region;
				}

				/// <exception cref="System.IO.IOException"/>
				public void finish()
				{
					try
					{
						@in.close();
					}
					finally
					{
						compressAlgo.returnDecompressor(decompressor);
						decompressor = null;
					}
				}
			}

			/// <summary>Access point to read a block.</summary>
			public class BlockReader : java.io.DataInputStream
			{
				private readonly org.apache.hadoop.io.file.tfile.BCFile.Reader.RBlockState rBlkState;

				private bool closed = false;

				internal BlockReader(org.apache.hadoop.io.file.tfile.BCFile.Reader.RBlockState rbs
					)
					: base(rbs.getInputStream())
				{
					rBlkState = rbs;
				}

				/// <summary>Finishing reading the block.</summary>
				/// <remarks>Finishing reading the block. Release all resources.</remarks>
				/// <exception cref="System.IO.IOException"/>
				public override void close()
				{
					if (closed == true)
					{
						return;
					}
					try
					{
						// Do not set rBlkState to null. People may access stats after calling
						// close().
						rBlkState.finish();
					}
					finally
					{
						closed = true;
					}
				}

				/// <summary>Get the name of the compression algorithm used to compress the block.</summary>
				/// <returns>name of the compression algorithm.</returns>
				public virtual string getCompressionName()
				{
					return rBlkState.getCompressionName();
				}

				/// <summary>Get the uncompressed size of the block.</summary>
				/// <returns>uncompressed size of the block.</returns>
				public virtual long getRawSize()
				{
					return rBlkState.getBlockRegion().getRawSize();
				}

				/// <summary>Get the compressed size of the block.</summary>
				/// <returns>compressed size of the block.</returns>
				public virtual long getCompressedSize()
				{
					return rBlkState.getBlockRegion().getCompressedSize();
				}

				/// <summary>Get the starting position of the block in the file.</summary>
				/// <returns>the starting position of the block in the file.</returns>
				public virtual long getStartPos()
				{
					return rBlkState.getBlockRegion().getOffset();
				}
			}

			/// <summary>Constructor</summary>
			/// <param name="fin">FS input stream.</param>
			/// <param name="fileLength">Length of the corresponding file</param>
			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.fs.FSDataInputStream fin, long fileLength, org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.@in = fin;
				this.conf = conf;
				// move the cursor to the beginning of the tail, containing: offset to the
				// meta block index, version and magic
				fin.seek(fileLength - org.apache.hadoop.io.file.tfile.BCFile.Magic.size() - org.apache.hadoop.io.file.tfile.Utils.Version
					.size() - long.SIZE / byte.SIZE);
				long offsetIndexMeta = fin.readLong();
				version = new org.apache.hadoop.io.file.tfile.Utils.Version(fin);
				org.apache.hadoop.io.file.tfile.BCFile.Magic.readAndVerify(fin);
				if (!version.compatibleWith(org.apache.hadoop.io.file.tfile.BCFile.API_VERSION))
				{
					throw new System.Exception("Incompatible BCFile fileBCFileVersion.");
				}
				// read meta index
				fin.seek(offsetIndexMeta);
				metaIndex = new org.apache.hadoop.io.file.tfile.BCFile.MetaIndex(fin);
				// read data:BCFile.index, the data block index
				org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader blockR = getMetaBlock(org.apache.hadoop.io.file.tfile.BCFile.DataIndex
					.BLOCK_NAME);
				try
				{
					dataIndex = new org.apache.hadoop.io.file.tfile.BCFile.DataIndex(blockR);
				}
				finally
				{
					blockR.close();
				}
			}

			/// <summary>Get the name of the default compression algorithm.</summary>
			/// <returns>the name of the default compression algorithm.</returns>
			public virtual string getDefaultCompressionName()
			{
				return dataIndex.getDefaultCompressionAlgorithm().getName();
			}

			/// <summary>Get version of BCFile file being read.</summary>
			/// <returns>version of BCFile file being read.</returns>
			public virtual org.apache.hadoop.io.file.tfile.Utils.Version getBCFileVersion()
			{
				return version;
			}

			/// <summary>Get version of BCFile API.</summary>
			/// <returns>version of BCFile API.</returns>
			public virtual org.apache.hadoop.io.file.tfile.Utils.Version getAPIVersion()
			{
				return API_VERSION;
			}

			/// <summary>Finishing reading the BCFile.</summary>
			/// <remarks>Finishing reading the BCFile. Release all resources.</remarks>
			public virtual void close()
			{
			}

			// nothing to be done now
			/// <summary>Get the number of data blocks.</summary>
			/// <returns>the number of data blocks.</returns>
			public virtual int getBlockCount()
			{
				return dataIndex.getBlockRegionList().Count;
			}

			/// <summary>Stream access to a Meta Block.</summary>
			/// <param name="name">meta block name</param>
			/// <returns>BlockReader input stream for reading the meta block.</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockDoesNotExist">The Meta Block with the given name does not exist.
			/// 	</exception>
			/// <exception cref="org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist"/>
			public virtual org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader getMetaBlock
				(string name)
			{
				org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry imeBCIndex = metaIndex.getMetaByName
					(name);
				if (imeBCIndex == null)
				{
					throw new org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist("name=" + name);
				}
				org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = imeBCIndex.getRegion(
					);
				return createReader(imeBCIndex.getCompressionAlgorithm(), region);
			}

			/// <summary>Stream access to a Data Block.</summary>
			/// <param name="blockIndex">0-based data block index.</param>
			/// <returns>BlockReader input stream for reading the data block.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader getDataBlock
				(int blockIndex)
			{
				if (blockIndex < 0 || blockIndex >= getBlockCount())
				{
					throw new System.IndexOutOfRangeException(string.format("blockIndex=%d, numBlocks=%d"
						, blockIndex, getBlockCount()));
				}
				org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = dataIndex.getBlockRegionList
					()[blockIndex];
				return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
			}

			/// <exception cref="System.IO.IOException"/>
			private org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader createReader(org.apache.hadoop.io.file.tfile.Compression.Algorithm
				 compressAlgo, org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region)
			{
				org.apache.hadoop.io.file.tfile.BCFile.Reader.RBlockState rbs = new org.apache.hadoop.io.file.tfile.BCFile.Reader.RBlockState
					(compressAlgo, @in, region, conf);
				return new org.apache.hadoop.io.file.tfile.BCFile.Reader.BlockReader(rbs);
			}

			/// <summary>
			/// Find the smallest Block index whose starting offset is greater than or
			/// equal to the specified offset.
			/// </summary>
			/// <param name="offset">User-specific offset.</param>
			/// <returns>
			/// the index to the data Block if such block exists; or -1
			/// otherwise.
			/// </returns>
			public virtual int getBlockIndexNear(long offset)
			{
				System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
					> list = dataIndex.getBlockRegionList();
				int idx = org.apache.hadoop.io.file.tfile.Utils.lowerBound(list, new org.apache.hadoop.io.file.tfile.CompareUtils.ScalarLong
					(offset), new org.apache.hadoop.io.file.tfile.CompareUtils.ScalarComparator());
				if (idx == list.Count)
				{
					return -1;
				}
				return idx;
			}
		}

		/// <summary>Index for all Meta blocks.</summary>
		internal class MetaIndex
		{
			internal readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
				> index;

			public MetaIndex()
			{
				// use a tree map, for getting a meta block entry by name
				// for write
				index = new System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
					>();
			}

			/// <exception cref="System.IO.IOException"/>
			public MetaIndex(java.io.DataInput @in)
			{
				// for read, construct the map from the file
				int count = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
				index = new System.Collections.Generic.SortedDictionary<string, org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
					>();
				for (int nx = 0; nx < count; nx++)
				{
					org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry indexEntry = new org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
						(@in);
					index[indexEntry.getMetaName()] = indexEntry;
				}
			}

			public virtual void addEntry(org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry
				 indexEntry)
			{
				index[indexEntry.getMetaName()] = indexEntry;
			}

			public virtual org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry getMetaByName
				(string name)
			{
				return index[name];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, index.Count);
				foreach (org.apache.hadoop.io.file.tfile.BCFile.MetaIndexEntry indexEntry in index
					.Values)
				{
					indexEntry.write(@out);
				}
			}
		}

		/// <summary>An entry describes a meta block in the MetaIndex.</summary>
		internal sealed class MetaIndexEntry
		{
			private readonly string metaName;

			private readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm compressionAlgorithm;

			private const string defaultPrefix = "data:";

			private readonly org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region;

			/// <exception cref="System.IO.IOException"/>
			public MetaIndexEntry(java.io.DataInput @in)
			{
				string fullMetaName = org.apache.hadoop.io.file.tfile.Utils.readString(@in);
				if (fullMetaName.StartsWith(defaultPrefix))
				{
					metaName = Sharpen.Runtime.substring(fullMetaName, defaultPrefix.Length, fullMetaName
						.Length);
				}
				else
				{
					throw new System.IO.IOException("Corrupted Meta region Index");
				}
				compressionAlgorithm = org.apache.hadoop.io.file.tfile.Compression.getCompressionAlgorithmByName
					(org.apache.hadoop.io.file.tfile.Utils.readString(@in));
				region = new org.apache.hadoop.io.file.tfile.BCFile.BlockRegion(@in);
			}

			public MetaIndexEntry(string metaName, org.apache.hadoop.io.file.tfile.Compression.Algorithm
				 compressionAlgorithm, org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region
				)
			{
				this.metaName = metaName;
				this.compressionAlgorithm = compressionAlgorithm;
				this.region = region;
			}

			public string getMetaName()
			{
				return metaName;
			}

			public org.apache.hadoop.io.file.tfile.Compression.Algorithm getCompressionAlgorithm
				()
			{
				return compressionAlgorithm;
			}

			public org.apache.hadoop.io.file.tfile.BCFile.BlockRegion getRegion()
			{
				return region;
			}

			/// <exception cref="System.IO.IOException"/>
			public void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeString(@out, defaultPrefix + metaName);
				org.apache.hadoop.io.file.tfile.Utils.writeString(@out, compressionAlgorithm.getName
					());
				region.write(@out);
			}
		}

		/// <summary>Index of all compressed data blocks.</summary>
		internal class DataIndex
		{
			internal const string BLOCK_NAME = "BCFile.index";

			private readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm defaultCompressionAlgorithm;

			private readonly System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
				> listRegions;

			/// <exception cref="System.IO.IOException"/>
			public DataIndex(java.io.DataInput @in)
			{
				// for data blocks, each entry specifies a block's offset, compressed size
				// and raw size
				// for read, deserialized from a file
				defaultCompressionAlgorithm = org.apache.hadoop.io.file.tfile.Compression.getCompressionAlgorithmByName
					(org.apache.hadoop.io.file.tfile.Utils.readString(@in));
				int n = org.apache.hadoop.io.file.tfile.Utils.readVInt(@in);
				listRegions = new System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
					>(n);
				for (int i = 0; i < n; i++)
				{
					org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region = new org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
						(@in);
					listRegions.add(region);
				}
			}

			public DataIndex(string defaultCompressionAlgorithmName)
			{
				// for write
				this.defaultCompressionAlgorithm = org.apache.hadoop.io.file.tfile.Compression.getCompressionAlgorithmByName
					(defaultCompressionAlgorithmName);
				listRegions = new System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
					>();
			}

			public virtual org.apache.hadoop.io.file.tfile.Compression.Algorithm getDefaultCompressionAlgorithm
				()
			{
				return defaultCompressionAlgorithm;
			}

			public virtual System.Collections.Generic.List<org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
				> getBlockRegionList()
			{
				return listRegions;
			}

			public virtual void addBlockRegion(org.apache.hadoop.io.file.tfile.BCFile.BlockRegion
				 region)
			{
				listRegions.add(region);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeString(@out, defaultCompressionAlgorithm
					.getName());
				org.apache.hadoop.io.file.tfile.Utils.writeVInt(@out, listRegions.Count);
				foreach (org.apache.hadoop.io.file.tfile.BCFile.BlockRegion region in listRegions)
				{
					region.write(@out);
				}
			}
		}

		/// <summary>Magic number uniquely identifying a BCFile in the header/footer.</summary>
		internal sealed class Magic
		{
			private static readonly byte[] AB_MAGIC_BCFILE = new byte[] { unchecked((byte)unchecked(
				(int)(0xd1))), unchecked((byte)unchecked((int)(0x11))), unchecked((byte)unchecked(
				(int)(0xd3))), unchecked((byte)unchecked((int)(0x68))), unchecked((byte)unchecked(
				(int)(0x91))), unchecked((byte)unchecked((int)(0xb5))), unchecked((byte)unchecked(
				(int)(0xd7))), unchecked((byte)unchecked((int)(0xb6))), unchecked((byte)unchecked(
				(int)(0x39))), unchecked((byte)unchecked((int)(0xdf))), unchecked((byte)unchecked(
				(int)(0x41))), unchecked((byte)unchecked((int)(0x40))), unchecked((byte)unchecked(
				(int)(0x92))), unchecked((byte)unchecked((int)(0xba))), unchecked((byte)unchecked(
				(int)(0xe1))), unchecked((byte)unchecked((int)(0x50))) };

			// ... total of 16 bytes
			/// <exception cref="System.IO.IOException"/>
			public static void readAndVerify(java.io.DataInput @in)
			{
				byte[] abMagic = new byte[size()];
				@in.readFully(abMagic);
				// check against AB_MAGIC_BCFILE, if not matching, throw an
				// Exception
				if (!java.util.Arrays.equals(abMagic, AB_MAGIC_BCFILE))
				{
					throw new System.IO.IOException("Not a valid BCFile.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public static void write(java.io.DataOutput @out)
			{
				@out.write(AB_MAGIC_BCFILE);
			}

			public static int size()
			{
				return AB_MAGIC_BCFILE.Length;
			}
		}

		/// <summary>Block region.</summary>
		internal sealed class BlockRegion : org.apache.hadoop.io.file.tfile.CompareUtils.Scalar
		{
			private readonly long offset;

			private readonly long compressedSize;

			private readonly long rawSize;

			/// <exception cref="System.IO.IOException"/>
			public BlockRegion(java.io.DataInput @in)
			{
				offset = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
				compressedSize = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
				rawSize = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
			}

			public BlockRegion(long offset, long compressedSize, long rawSize)
			{
				this.offset = offset;
				this.compressedSize = compressedSize;
				this.rawSize = rawSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public void write(java.io.DataOutput @out)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, offset);
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, compressedSize);
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, rawSize);
			}

			public long getOffset()
			{
				return offset;
			}

			public long getCompressedSize()
			{
				return compressedSize;
			}

			public long getRawSize()
			{
				return rawSize;
			}

			public long magnitude()
			{
				return offset;
			}
		}
	}
}
