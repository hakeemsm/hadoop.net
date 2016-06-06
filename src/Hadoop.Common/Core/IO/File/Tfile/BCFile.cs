using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;


namespace Org.Apache.Hadoop.IO.File.Tfile
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
		internal static readonly Utils.Version ApiVersion = new Utils.Version((short)1, (
			short)0);

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.File.Tfile.BCFile
			));

		/// <summary>Prevent the instantiation of BCFile objects.</summary>
		private BCFile()
		{
		}

		/// <summary>BCFile writer, the entry point for creating a new BCFile.</summary>
		public class Writer : IDisposable
		{
			private readonly FSDataOutputStream @out;

			private readonly Configuration conf;

			internal readonly BCFile.DataIndex dataIndex;

			internal readonly BCFile.MetaIndex metaIndex;

			internal bool blkInProgress = false;

			private bool metaBlkSeen = false;

			private bool closed = false;

			internal long errorCount = 0;

			private BytesWritable fsOutputBuffer;

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
				void Register(long raw, long offsetStart, long offsetEnd);
			}

			/// <summary>
			/// Intermediate class that maintain the state of a Writable Compression
			/// Block.
			/// </summary>
			private sealed class WBlockState
			{
				private readonly Compression.Algorithm compressAlgo;

				private Compressor compressor;

				private readonly FSDataOutputStream fsOut;

				private readonly long posStart;

				private readonly SimpleBufferedOutputStream fsBufferedOutput;

				private OutputStream @out;

				/// <param name="compressionAlgo">The compression algorithm to be used to for compression.
				/// 	</param>
				/// <exception cref="System.IO.IOException"/>
				public WBlockState(Compression.Algorithm compressionAlgo, FSDataOutputStream fsOut
					, BytesWritable fsOutputBuffer, Configuration conf)
				{
					// !null only if using native
					// Hadoop compression
					this.compressAlgo = compressionAlgo;
					this.fsOut = fsOut;
					this.posStart = fsOut.GetPos();
					fsOutputBuffer.Capacity = TFile.GetFSOutputBufferSize(conf);
					this.fsBufferedOutput = new SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer
						.Bytes);
					this.compressor = compressAlgo.GetCompressor();
					try
					{
						this.@out = compressionAlgo.CreateCompressionStream(fsBufferedOutput, compressor, 
							0);
					}
					catch (IOException e)
					{
						compressAlgo.ReturnCompressor(compressor);
						throw;
					}
				}

				/// <summary>Get the output stream for BlockAppender's consumption.</summary>
				/// <returns>the output stream suitable for writing block data.</returns>
				internal OutputStream GetOutputStream()
				{
					return @out;
				}

				/// <summary>Get the current position in file.</summary>
				/// <returns>The current byte offset in underlying file.</returns>
				/// <exception cref="System.IO.IOException"/>
				internal long GetCurrentPos()
				{
					return fsOut.GetPos() + fsBufferedOutput.Size();
				}

				internal long GetStartPos()
				{
					return posStart;
				}

				/// <summary>Current size of compressed data.</summary>
				/// <returns/>
				/// <exception cref="System.IO.IOException"/>
				internal long GetCompressedSize()
				{
					long ret = GetCurrentPos() - posStart;
					return ret;
				}

				/// <summary>Finishing up the current block.</summary>
				/// <exception cref="System.IO.IOException"/>
				public void Finish()
				{
					try
					{
						if (@out != null)
						{
							@out.Flush();
							@out = null;
						}
					}
					finally
					{
						compressAlgo.ReturnCompressor(compressor);
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
			public class BlockAppender : DataOutputStream
			{
				private readonly BCFile.Writer.BlockRegister blockRegister;

				private readonly BCFile.Writer.WBlockState wBlkState;

				private bool closed = false;

				/// <summary>Constructor</summary>
				/// <param name="register">the block register, which is called when the block is closed.
				/// 	</param>
				/// <param name="wbs">The writable compression block state.</param>
				internal BlockAppender(Writer _enclosing, BCFile.Writer.BlockRegister register, BCFile.Writer.WBlockState
					 wbs)
					: base(wbs.GetOutputStream())
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
				public virtual long GetRawSize()
				{
					return this.Size() & unchecked((long)(0x00000000ffffffffL));
				}

				/// <summary>Get the compressed size of the block in progress.</summary>
				/// <returns>
				/// the number of compressed bytes written to the underlying FS
				/// file. The size may be smaller than actual need to compress the
				/// all data written due to internal buffering inside the
				/// compressor.
				/// </returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual long GetCompressedSize()
				{
					return this.wBlkState.GetCompressedSize();
				}

				public override void Flush()
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
				public override void Close()
				{
					if (this.closed == true)
					{
						return;
					}
					try
					{
						++this._enclosing.errorCount;
						this.wBlkState.Finish();
						this.blockRegister.Register(this.GetRawSize(), this.wBlkState.GetStartPos(), this
							.wBlkState.GetCurrentPos());
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
			/// <seealso cref="Compression.GetSupportedAlgorithms()"/>
			public Writer(FSDataOutputStream fout, string compressionName, Configuration conf
				)
			{
				if (fout.GetPos() != 0)
				{
					throw new IOException("Output file not at zero offset.");
				}
				this.@out = fout;
				this.conf = conf;
				dataIndex = new BCFile.DataIndex(compressionName);
				metaIndex = new BCFile.MetaIndex();
				fsOutputBuffer = new BytesWritable();
				BCFile.Magic.Write(fout);
			}

			/// <summary>Close the BCFile Writer.</summary>
			/// <remarks>
			/// Close the BCFile Writer. Attempting to use the Writer after calling
			/// <code>close</code> is not allowed and may lead to undetermined results.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
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
							throw new InvalidOperationException("Close() called with active block appender.");
						}
						// add metaBCFileIndex to metaIndex as the last meta block
						BCFile.Writer.BlockAppender appender = PrepareMetaBlock(BCFile.DataIndex.BlockName
							, GetDefaultCompressionAlgorithm());
						try
						{
							dataIndex.Write(appender);
						}
						finally
						{
							appender.Close();
						}
						long offsetIndexMeta = @out.GetPos();
						metaIndex.Write(@out);
						// Meta Index and the trailing section are written out directly.
						@out.WriteLong(offsetIndexMeta);
						ApiVersion.Write(@out);
						BCFile.Magic.Write(@out);
						@out.Flush();
					}
				}
				finally
				{
					closed = true;
				}
			}

			private Compression.Algorithm GetDefaultCompressionAlgorithm()
			{
				return dataIndex.GetDefaultCompressionAlgorithm();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockAlreadyExists"/>
			private BCFile.Writer.BlockAppender PrepareMetaBlock(string name, Compression.Algorithm
				 compressAlgo)
			{
				if (blkInProgress == true)
				{
					throw new InvalidOperationException("Cannot create Meta Block until previous block is closed."
						);
				}
				if (metaIndex.GetMetaByName(name) != null)
				{
					throw new MetaBlockAlreadyExists("name=" + name);
				}
				BCFile.Writer.MetaBlockRegister mbr = new BCFile.Writer.MetaBlockRegister(this, name
					, compressAlgo);
				BCFile.Writer.WBlockState wbs = new BCFile.Writer.WBlockState(compressAlgo, @out, 
					fsOutputBuffer, conf);
				BCFile.Writer.BlockAppender ba = new BCFile.Writer.BlockAppender(this, mbr, wbs);
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
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockAlreadyExists"/>
			public virtual BCFile.Writer.BlockAppender PrepareMetaBlock(string name, string compressionName
				)
			{
				return PrepareMetaBlock(name, Compression.GetCompressionAlgorithmByName(compressionName
					));
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
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockAlreadyExists"/>
			public virtual BCFile.Writer.BlockAppender PrepareMetaBlock(string name)
			{
				return PrepareMetaBlock(name, GetDefaultCompressionAlgorithm());
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
			public virtual BCFile.Writer.BlockAppender PrepareDataBlock()
			{
				if (blkInProgress == true)
				{
					throw new InvalidOperationException("Cannot create Data Block until previous block is closed."
						);
				}
				if (metaBlkSeen == true)
				{
					throw new InvalidOperationException("Cannot create Data Block after Meta Blocks."
						);
				}
				BCFile.Writer.DataBlockRegister dbr = new BCFile.Writer.DataBlockRegister(this);
				BCFile.Writer.WBlockState wbs = new BCFile.Writer.WBlockState(GetDefaultCompressionAlgorithm
					(), @out, fsOutputBuffer, conf);
				BCFile.Writer.BlockAppender ba = new BCFile.Writer.BlockAppender(this, dbr, wbs);
				blkInProgress = true;
				return ba;
			}

			/// <summary>
			/// Callback to make sure a meta block is added to the internal list when its
			/// stream is closed.
			/// </summary>
			private class MetaBlockRegister : BCFile.Writer.BlockRegister
			{
				private readonly string name;

				private readonly Compression.Algorithm compressAlgo;

				internal MetaBlockRegister(Writer _enclosing, string name, Compression.Algorithm 
					compressAlgo)
				{
					this._enclosing = _enclosing;
					this.name = name;
					this.compressAlgo = compressAlgo;
				}

				public virtual void Register(long raw, long begin, long end)
				{
					this._enclosing.metaIndex.AddEntry(new BCFile.MetaIndexEntry(this.name, this.compressAlgo
						, new BCFile.BlockRegion(begin, end - begin, raw)));
				}

				private readonly Writer _enclosing;
			}

			/// <summary>
			/// Callback to make sure a data block is added to the internal list when
			/// it's being closed.
			/// </summary>
			private class DataBlockRegister : BCFile.Writer.BlockRegister
			{
				internal DataBlockRegister(Writer _enclosing)
				{
					this._enclosing = _enclosing;
				}

				// do nothing
				public virtual void Register(long raw, long begin, long end)
				{
					this._enclosing.dataIndex.AddBlockRegion(new BCFile.BlockRegion(begin, end - begin
						, raw));
				}

				private readonly Writer _enclosing;
			}
		}

		/// <summary>BCFile Reader, interface to read the file's data and meta blocks.</summary>
		public class Reader : IDisposable
		{
			private readonly FSDataInputStream @in;

			private readonly Configuration conf;

			internal readonly BCFile.DataIndex dataIndex;

			internal readonly BCFile.MetaIndex metaIndex;

			internal readonly Utils.Version version;

			/// <summary>
			/// Intermediate class that maintain the state of a Readable Compression
			/// Block.
			/// </summary>
			private sealed class RBlockState
			{
				private readonly Compression.Algorithm compressAlgo;

				private Decompressor decompressor;

				private readonly BCFile.BlockRegion region;

				private readonly InputStream @in;

				/// <exception cref="System.IO.IOException"/>
				public RBlockState(Compression.Algorithm compressionAlgo, FSDataInputStream fsin, 
					BCFile.BlockRegion region, Configuration conf)
				{
					// Index for meta blocks
					this.compressAlgo = compressionAlgo;
					this.region = region;
					this.decompressor = compressionAlgo.GetDecompressor();
					try
					{
						this.@in = compressAlgo.CreateDecompressionStream(new BoundedRangeFileInputStream
							(fsin, this.region.GetOffset(), this.region.GetCompressedSize()), decompressor, 
							TFile.GetFSInputBufferSize(conf));
					}
					catch (IOException e)
					{
						compressAlgo.ReturnDecompressor(decompressor);
						throw;
					}
				}

				/// <summary>Get the output stream for BlockAppender's consumption.</summary>
				/// <returns>the output stream suitable for writing block data.</returns>
				public InputStream GetInputStream()
				{
					return @in;
				}

				public string GetCompressionName()
				{
					return compressAlgo.GetName();
				}

				public BCFile.BlockRegion GetBlockRegion()
				{
					return region;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Finish()
				{
					try
					{
						@in.Close();
					}
					finally
					{
						compressAlgo.ReturnDecompressor(decompressor);
						decompressor = null;
					}
				}
			}

			/// <summary>Access point to read a block.</summary>
			public class BlockReader : DataInputStream
			{
				private readonly BCFile.Reader.RBlockState rBlkState;

				private bool closed = false;

				internal BlockReader(BCFile.Reader.RBlockState rbs)
					: base(rbs.GetInputStream())
				{
					rBlkState = rbs;
				}

				/// <summary>Finishing reading the block.</summary>
				/// <remarks>Finishing reading the block. Release all resources.</remarks>
				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					if (closed == true)
					{
						return;
					}
					try
					{
						// Do not set rBlkState to null. People may access stats after calling
						// close().
						rBlkState.Finish();
					}
					finally
					{
						closed = true;
					}
				}

				/// <summary>Get the name of the compression algorithm used to compress the block.</summary>
				/// <returns>name of the compression algorithm.</returns>
				public virtual string GetCompressionName()
				{
					return rBlkState.GetCompressionName();
				}

				/// <summary>Get the uncompressed size of the block.</summary>
				/// <returns>uncompressed size of the block.</returns>
				public virtual long GetRawSize()
				{
					return rBlkState.GetBlockRegion().GetRawSize();
				}

				/// <summary>Get the compressed size of the block.</summary>
				/// <returns>compressed size of the block.</returns>
				public virtual long GetCompressedSize()
				{
					return rBlkState.GetBlockRegion().GetCompressedSize();
				}

				/// <summary>Get the starting position of the block in the file.</summary>
				/// <returns>the starting position of the block in the file.</returns>
				public virtual long GetStartPos()
				{
					return rBlkState.GetBlockRegion().GetOffset();
				}
			}

			/// <summary>Constructor</summary>
			/// <param name="fin">FS input stream.</param>
			/// <param name="fileLength">Length of the corresponding file</param>
			/// <exception cref="System.IO.IOException"/>
			public Reader(FSDataInputStream fin, long fileLength, Configuration conf)
			{
				this.@in = fin;
				this.conf = conf;
				// move the cursor to the beginning of the tail, containing: offset to the
				// meta block index, version and magic
				fin.Seek(fileLength - BCFile.Magic.Size() - Utils.Version.Size() - long.Size / byte
					.Size);
				long offsetIndexMeta = fin.ReadLong();
				version = new Utils.Version(fin);
				BCFile.Magic.ReadAndVerify(fin);
				if (!version.CompatibleWith(BCFile.ApiVersion))
				{
					throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
				}
				// read meta index
				fin.Seek(offsetIndexMeta);
				metaIndex = new BCFile.MetaIndex(fin);
				// read data:BCFile.index, the data block index
				BCFile.Reader.BlockReader blockR = GetMetaBlock(BCFile.DataIndex.BlockName);
				try
				{
					dataIndex = new BCFile.DataIndex(blockR);
				}
				finally
				{
					blockR.Close();
				}
			}

			/// <summary>Get the name of the default compression algorithm.</summary>
			/// <returns>the name of the default compression algorithm.</returns>
			public virtual string GetDefaultCompressionName()
			{
				return dataIndex.GetDefaultCompressionAlgorithm().GetName();
			}

			/// <summary>Get version of BCFile file being read.</summary>
			/// <returns>version of BCFile file being read.</returns>
			public virtual Utils.Version GetBCFileVersion()
			{
				return version;
			}

			/// <summary>Get version of BCFile API.</summary>
			/// <returns>version of BCFile API.</returns>
			public virtual Utils.Version GetAPIVersion()
			{
				return ApiVersion;
			}

			/// <summary>Finishing reading the BCFile.</summary>
			/// <remarks>Finishing reading the BCFile. Release all resources.</remarks>
			public virtual void Close()
			{
			}

			// nothing to be done now
			/// <summary>Get the number of data blocks.</summary>
			/// <returns>the number of data blocks.</returns>
			public virtual int GetBlockCount()
			{
				return dataIndex.GetBlockRegionList().Count;
			}

			/// <summary>Stream access to a Meta Block.</summary>
			/// <param name="name">meta block name</param>
			/// <returns>BlockReader input stream for reading the meta block.</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="MetaBlockDoesNotExist">The Meta Block with the given name does not exist.
			/// 	</exception>
			/// <exception cref="Org.Apache.Hadoop.IO.File.Tfile.MetaBlockDoesNotExist"/>
			public virtual BCFile.Reader.BlockReader GetMetaBlock(string name)
			{
				BCFile.MetaIndexEntry imeBCIndex = metaIndex.GetMetaByName(name);
				if (imeBCIndex == null)
				{
					throw new MetaBlockDoesNotExist("name=" + name);
				}
				BCFile.BlockRegion region = imeBCIndex.GetRegion();
				return CreateReader(imeBCIndex.GetCompressionAlgorithm(), region);
			}

			/// <summary>Stream access to a Data Block.</summary>
			/// <param name="blockIndex">0-based data block index.</param>
			/// <returns>BlockReader input stream for reading the data block.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual BCFile.Reader.BlockReader GetDataBlock(int blockIndex)
			{
				if (blockIndex < 0 || blockIndex >= GetBlockCount())
				{
					throw new IndexOutOfRangeException(string.Format("blockIndex=%d, numBlocks=%d", blockIndex
						, GetBlockCount()));
				}
				BCFile.BlockRegion region = dataIndex.GetBlockRegionList()[blockIndex];
				return CreateReader(dataIndex.GetDefaultCompressionAlgorithm(), region);
			}

			/// <exception cref="System.IO.IOException"/>
			private BCFile.Reader.BlockReader CreateReader(Compression.Algorithm compressAlgo
				, BCFile.BlockRegion region)
			{
				BCFile.Reader.RBlockState rbs = new BCFile.Reader.RBlockState(compressAlgo, @in, 
					region, conf);
				return new BCFile.Reader.BlockReader(rbs);
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
			public virtual int GetBlockIndexNear(long offset)
			{
				AList<BCFile.BlockRegion> list = dataIndex.GetBlockRegionList();
				int idx = Utils.LowerBound(list, new CompareUtils.ScalarLong(offset), new CompareUtils.ScalarComparator
					());
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
			internal readonly IDictionary<string, BCFile.MetaIndexEntry> index;

			public MetaIndex()
			{
				// use a tree map, for getting a meta block entry by name
				// for write
				index = new SortedDictionary<string, BCFile.MetaIndexEntry>();
			}

			/// <exception cref="System.IO.IOException"/>
			public MetaIndex(BinaryReader reader)
			{
				// for read, construct the map from the file
				int count = Utils.ReadVInt(@in);
				index = new SortedDictionary<string, BCFile.MetaIndexEntry>();
				for (int nx = 0; nx < count; nx++)
				{
					BCFile.MetaIndexEntry indexEntry = new BCFile.MetaIndexEntry(@in);
					index[indexEntry.GetMetaName()] = indexEntry;
				}
			}

			public virtual void AddEntry(BCFile.MetaIndexEntry indexEntry)
			{
				index[indexEntry.GetMetaName()] = indexEntry;
			}

			public virtual BCFile.MetaIndexEntry GetMetaByName(string name)
			{
				return index[name];
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter writer)
			{
				Utils.WriteVInt(@out, index.Count);
				foreach (BCFile.MetaIndexEntry indexEntry in index.Values)
				{
					indexEntry.Write(@out);
				}
			}
		}

		/// <summary>An entry describes a meta block in the MetaIndex.</summary>
		internal sealed class MetaIndexEntry
		{
			private readonly string metaName;

			private readonly Compression.Algorithm compressionAlgorithm;

			private const string defaultPrefix = "data:";

			private readonly BCFile.BlockRegion region;

			/// <exception cref="System.IO.IOException"/>
			public MetaIndexEntry(BinaryReader reader)
			{
				string fullMetaName = Utils.ReadString(@in);
				if (fullMetaName.StartsWith(defaultPrefix))
				{
					metaName = Runtime.Substring(fullMetaName, defaultPrefix.Length, fullMetaName
						.Length);
				}
				else
				{
					throw new IOException("Corrupted Meta region Index");
				}
				compressionAlgorithm = Compression.GetCompressionAlgorithmByName(Utils.ReadString
					(@in));
				region = new BCFile.BlockRegion(@in);
			}

			public MetaIndexEntry(string metaName, Compression.Algorithm compressionAlgorithm
				, BCFile.BlockRegion region)
			{
				this.metaName = metaName;
				this.compressionAlgorithm = compressionAlgorithm;
				this.region = region;
			}

			public string GetMetaName()
			{
				return metaName;
			}

			public Compression.Algorithm GetCompressionAlgorithm()
			{
				return compressionAlgorithm;
			}

			public BCFile.BlockRegion GetRegion()
			{
				return region;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(BinaryWriter writer)
			{
				Utils.WriteString(@out, defaultPrefix + metaName);
				Utils.WriteString(@out, compressionAlgorithm.GetName());
				region.Write(@out);
			}
		}

		/// <summary>Index of all compressed data blocks.</summary>
		internal class DataIndex
		{
			internal const string BlockName = "BCFile.index";

			private readonly Compression.Algorithm defaultCompressionAlgorithm;

			private readonly AList<BCFile.BlockRegion> listRegions;

			/// <exception cref="System.IO.IOException"/>
			public DataIndex(BinaryReader reader)
			{
				// for data blocks, each entry specifies a block's offset, compressed size
				// and raw size
				// for read, deserialized from a file
				defaultCompressionAlgorithm = Compression.GetCompressionAlgorithmByName(Utils.ReadString
					(@in));
				int n = Utils.ReadVInt(@in);
				listRegions = new AList<BCFile.BlockRegion>(n);
				for (int i = 0; i < n; i++)
				{
					BCFile.BlockRegion region = new BCFile.BlockRegion(@in);
					listRegions.AddItem(region);
				}
			}

			public DataIndex(string defaultCompressionAlgorithmName)
			{
				// for write
				this.defaultCompressionAlgorithm = Compression.GetCompressionAlgorithmByName(defaultCompressionAlgorithmName
					);
				listRegions = new AList<BCFile.BlockRegion>();
			}

			public virtual Compression.Algorithm GetDefaultCompressionAlgorithm()
			{
				return defaultCompressionAlgorithm;
			}

			public virtual AList<BCFile.BlockRegion> GetBlockRegionList()
			{
				return listRegions;
			}

			public virtual void AddBlockRegion(BCFile.BlockRegion region)
			{
				listRegions.AddItem(region);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(BinaryWriter writer)
			{
				Utils.WriteString(@out, defaultCompressionAlgorithm.GetName());
				Utils.WriteVInt(@out, listRegions.Count);
				foreach (BCFile.BlockRegion region in listRegions)
				{
					region.Write(@out);
				}
			}
		}

		/// <summary>Magic number uniquely identifying a BCFile in the header/footer.</summary>
		internal sealed class Magic
		{
			private static readonly byte[] AbMagicBcfile = new byte[] { unchecked((byte)unchecked(
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
			public static void ReadAndVerify(BinaryReader reader)
			{
				byte[] abMagic = new byte[Size()];
				@in.ReadFully(abMagic);
				// check against AB_MAGIC_BCFILE, if not matching, throw an
				// Exception
				if (!Arrays.Equals(abMagic, AbMagicBcfile))
				{
					throw new IOException("Not a valid BCFile.");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public static void Write(BinaryWriter writer)
			{
				@out.Write(AbMagicBcfile);
			}

			public static int Size()
			{
				return AbMagicBcfile.Length;
			}
		}

		/// <summary>Block region.</summary>
		internal sealed class BlockRegion : CompareUtils.Scalar
		{
			private readonly long offset;

			private readonly long compressedSize;

			private readonly long rawSize;

			/// <exception cref="System.IO.IOException"/>
			public BlockRegion(BinaryReader reader)
			{
				offset = Utils.ReadVLong(@in);
				compressedSize = Utils.ReadVLong(@in);
				rawSize = Utils.ReadVLong(@in);
			}

			public BlockRegion(long offset, long compressedSize, long rawSize)
			{
				this.offset = offset;
				this.compressedSize = compressedSize;
				this.rawSize = rawSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(BinaryWriter writer)
			{
				Utils.WriteVLong(@out, offset);
				Utils.WriteVLong(@out, compressedSize);
				Utils.WriteVLong(@out, rawSize);
			}

			public long GetOffset()
			{
				return offset;
			}

			public long GetCompressedSize()
			{
				return compressedSize;
			}

			public long GetRawSize()
			{
				return rawSize;
			}

			public long Magnitude()
			{
				return offset;
			}
		}
	}
}
