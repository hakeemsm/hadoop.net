using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.IO;
using Java.Rmi.Server;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Compress.Zlib;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// <code>SequenceFile</code>s are flat files consisting of binary key/value
	/// pairs.
	/// </summary>
	/// <remarks>
	/// <code>SequenceFile</code>s are flat files consisting of binary key/value
	/// pairs.
	/// <p><code>SequenceFile</code> provides
	/// <see cref="Writer"/>
	/// ,
	/// <see cref="Reader"/>
	/// and
	/// <see cref="Sorter"/>
	/// classes for writing,
	/// reading and sorting respectively.</p>
	/// There are three <code>SequenceFile</code> <code>Writer</code>s based on the
	/// <see cref="CompressionType"/>
	/// used to compress key/value pairs:
	/// <ol>
	/// <li>
	/// <code>Writer</code> : Uncompressed records.
	/// </li>
	/// <li>
	/// <code>RecordCompressWriter</code> : Record-compressed files, only compress
	/// values.
	/// </li>
	/// <li>
	/// <code>BlockCompressWriter</code> : Block-compressed files, both keys &
	/// values are collected in 'blocks'
	/// separately and compressed. The size of
	/// the 'block' is configurable.
	/// </ol>
	/// <p>The actual compression algorithm used to compress key and/or values can be
	/// specified by using the appropriate
	/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
	/// .</p>
	/// <p>The recommended way is to use the static <tt>createWriter</tt> methods
	/// provided by the <code>SequenceFile</code> to chose the preferred format.</p>
	/// <p>The
	/// <see cref="Reader"/>
	/// acts as the bridge and can read any of the
	/// above <code>SequenceFile</code> formats.</p>
	/// <h4 id="Formats">SequenceFile Formats</h4>
	/// <p>Essentially there are 3 different formats for <code>SequenceFile</code>s
	/// depending on the <code>CompressionType</code> specified. All of them share a
	/// <a href="#Header">common header</a> described below.
	/// <h5 id="Header">SequenceFile Header</h5>
	/// <ul>
	/// <li>
	/// version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of actual
	/// version number (e.g. SEQ4 or SEQ6)
	/// </li>
	/// <li>
	/// keyClassName -key class
	/// </li>
	/// <li>
	/// valueClassName - value class
	/// </li>
	/// <li>
	/// compression - A boolean which specifies if compression is turned on for
	/// keys/values in this file.
	/// </li>
	/// <li>
	/// blockCompression - A boolean which specifies if block-compression is
	/// turned on for keys/values in this file.
	/// </li>
	/// <li>
	/// compression codec - <code>CompressionCodec</code> class which is used for
	/// compression of keys and/or values (if compression is
	/// enabled).
	/// </li>
	/// <li>
	/// metadata -
	/// <see cref="Metadata"/>
	/// for this file.
	/// </li>
	/// <li>
	/// sync - A sync marker to denote end of the header.
	/// </li>
	/// </ul>
	/// <h5 id="#UncompressedFormat">Uncompressed SequenceFile Format</h5>
	/// <ul>
	/// <li>
	/// <a href="#Header">Header</a>
	/// </li>
	/// <li>
	/// Record
	/// <ul>
	/// <li>Record length</li>
	/// <li>Key length</li>
	/// <li>Key</li>
	/// <li>Value</li>
	/// </ul>
	/// </li>
	/// <li>
	/// A sync-marker every few <code>100</code> bytes or so.
	/// </li>
	/// </ul>
	/// <h5 id="#RecordCompressedFormat">Record-Compressed SequenceFile Format</h5>
	/// <ul>
	/// <li>
	/// <a href="#Header">Header</a>
	/// </li>
	/// <li>
	/// Record
	/// <ul>
	/// <li>Record length</li>
	/// <li>Key length</li>
	/// <li>Key</li>
	/// <li><i>Compressed</i> Value</li>
	/// </ul>
	/// </li>
	/// <li>
	/// A sync-marker every few <code>100</code> bytes or so.
	/// </li>
	/// </ul>
	/// <h5 id="#BlockCompressedFormat">Block-Compressed SequenceFile Format</h5>
	/// <ul>
	/// <li>
	/// <a href="#Header">Header</a>
	/// </li>
	/// <li>
	/// Record <i>Block</i>
	/// <ul>
	/// <li>Uncompressed number of records in the block</li>
	/// <li>Compressed key-lengths block-size</li>
	/// <li>Compressed key-lengths block</li>
	/// <li>Compressed keys block-size</li>
	/// <li>Compressed keys block</li>
	/// <li>Compressed value-lengths block-size</li>
	/// <li>Compressed value-lengths block</li>
	/// <li>Compressed values block-size</li>
	/// <li>Compressed values block</li>
	/// </ul>
	/// </li>
	/// <li>
	/// A sync-marker every block.
	/// </li>
	/// </ul>
	/// <p>The compressed blocks of key lengths and value lengths consist of the
	/// actual lengths of individual keys/values encoded in ZeroCompressedInteger
	/// format.</p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
	public class SequenceFile
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.SequenceFile
			));

		private SequenceFile()
		{
		}

		private const byte BlockCompressVersion = unchecked((byte)4);

		private const byte CustomCompressVersion = unchecked((byte)5);

		private const byte VersionWithMetadata = unchecked((byte)6);

		private static byte[] Version = new byte[] { unchecked((byte)(byte)('S')), unchecked(
			(byte)(byte)('E')), unchecked((byte)(byte)('Q')), VersionWithMetadata };

		private const int SyncEscape = -1;

		private const int SyncHashSize = 16;

		private const int SyncSize = 4 + SyncHashSize;

		/// <summary>The number of bytes between sync points.</summary>
		public const int SyncInterval = 100 * SyncSize;

		/// <summary>
		/// The compression type used to compress key/value pairs in the
		/// <see cref="SequenceFile"/>
		/// .
		/// </summary>
		/// <seealso cref="Writer"/>
		public enum CompressionType
		{
			None,
			Record,
			Block
		}

		// no public ctor
		// "length" of sync entries
		// number of bytes in hash 
		// escape + hash
		/// <summary>Get the compression type for the reduce outputs</summary>
		/// <param name="job">the job config to look in</param>
		/// <returns>the kind of compression to use</returns>
		public static SequenceFile.CompressionType GetDefaultCompressionType(Configuration
			 job)
		{
			string name = job.Get("io.seqfile.compression.type");
			return name == null ? SequenceFile.CompressionType.Record : SequenceFile.CompressionType
				.ValueOf(name);
		}

		/// <summary>Set the default compression type for sequence files.</summary>
		/// <param name="job">the configuration to modify</param>
		/// <param name="val">the new compression type (none, block, record)</param>
		public static void SetDefaultCompressionType(Configuration job, SequenceFile.CompressionType
			 val)
		{
			job.Set("io.seqfile.compression.type", val.ToString());
		}

		/// <summary>Create a new Writer with the given options.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <param name="opts">the options to create the file with</param>
		/// <returns>a new Writer</returns>
		/// <exception cref="System.IO.IOException"/>
		public static SequenceFile.Writer CreateWriter(Configuration conf, params SequenceFile.Writer.Option
			[] opts)
		{
			SequenceFile.Writer.CompressionOption compressionOption = Options.GetOption<SequenceFile.Writer.CompressionOption
				>(opts);
			SequenceFile.CompressionType kind;
			if (compressionOption != null)
			{
				kind = compressionOption.GetValue();
			}
			else
			{
				kind = GetDefaultCompressionType(conf);
				opts = Options.PrependOptions(opts, SequenceFile.Writer.Compression(kind));
			}
			switch (kind)
			{
				case SequenceFile.CompressionType.None:
				default:
				{
					return new SequenceFile.Writer(conf, opts);
				}

				case SequenceFile.CompressionType.Record:
				{
					return new SequenceFile.RecordCompressWriter(conf, opts);
				}

				case SequenceFile.CompressionType.Block:
				{
					return new SequenceFile.BlockCompressWriter(conf, opts);
				}
			}
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass)
		{
			return CreateWriter(conf, SequenceFile.Writer.Filesystem(fs), SequenceFile.Writer
				.File(name), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass
				(valClass));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			)
		{
			return CreateWriter(conf, SequenceFile.Writer.Filesystem(fs), SequenceFile.Writer
				.File(name), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass
				(valClass), SequenceFile.Writer.Compression(compressionType));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="progress">The Progressable object to track progress.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, Progressable progress)
		{
			return CreateWriter(conf, SequenceFile.Writer.File(name), SequenceFile.Writer.Filesystem
				(fs), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
				), SequenceFile.Writer.Compression(compressionType), SequenceFile.Writer.Progressable
				(progress));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec)
		{
			return CreateWriter(conf, SequenceFile.Writer.File(name), SequenceFile.Writer.Filesystem
				(fs), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
				), SequenceFile.Writer.Compression(compressionType, codec));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="progress">The Progressable object to track progress.</param>
		/// <param name="metadata">The metadata of the file.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec, Progressable progress, SequenceFile.Metadata metadata)
		{
			return CreateWriter(conf, SequenceFile.Writer.File(name), SequenceFile.Writer.Filesystem
				(fs), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
				), SequenceFile.Writer.Compression(compressionType, codec), SequenceFile.Writer.
				Progressable(progress), SequenceFile.Writer.Metadata(metadata));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="bufferSize">buffer size for the underlaying outputstream.</param>
		/// <param name="replication">replication factor for the file.</param>
		/// <param name="blockSize">block size for the file.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="progress">The Progressable object to track progress.</param>
		/// <param name="metadata">The metadata of the file.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, int bufferSize, short replication, long
			 blockSize, SequenceFile.CompressionType compressionType, CompressionCodec codec
			, Progressable progress, SequenceFile.Metadata metadata)
		{
			return CreateWriter(conf, SequenceFile.Writer.File(name), SequenceFile.Writer.Filesystem
				(fs), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
				), SequenceFile.Writer.BufferSize(bufferSize), SequenceFile.Writer.Replication(replication
				), SequenceFile.Writer.BlockSize(blockSize), SequenceFile.Writer.Compression(compressionType
				, codec), SequenceFile.Writer.Progressable(progress), SequenceFile.Writer.Metadata
				(metadata));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="bufferSize">buffer size for the underlaying outputstream.</param>
		/// <param name="replication">replication factor for the file.</param>
		/// <param name="blockSize">block size for the file.</param>
		/// <param name="createParent">create parent directory if non-existent</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="metadata">The metadata of the file.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, int bufferSize, short replication, long
			 blockSize, bool createParent, SequenceFile.CompressionType compressionType, CompressionCodec
			 codec, SequenceFile.Metadata metadata)
		{
			return CreateWriter(FileContext.GetFileContext(fs.GetUri(), conf), conf, name, keyClass
				, valClass, compressionType, codec, metadata, EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite), Options.CreateOpts.BufferSize(bufferSize), createParent ? Options.CreateOpts
				.CreateParent() : Options.CreateOpts.DonotCreateParent(), Options.CreateOpts.RepFac
				(replication), Options.CreateOpts.BlockSize(blockSize));
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fc">The context for the specified file.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="metadata">The metadata of the file.</param>
		/// <param name="createFlag">gives the semantics of create: overwrite, append etc.</param>
		/// <param name="opts">
		/// file creation options; see
		/// <see cref="Org.Apache.Hadoop.FS.Options.CreateOpts"/>
		/// .
		/// </param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static SequenceFile.Writer CreateWriter(FileContext fc, Configuration conf
			, Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec, SequenceFile.Metadata metadata, EnumSet<CreateFlag> createFlag
			, params Options.CreateOpts[] opts)
		{
			return CreateWriter(conf, fc.Create(name, createFlag, opts), keyClass, valClass, 
				compressionType, codec, metadata).OwnStream();
		}

		/// <summary>Construct the preferred type of SequenceFile Writer.</summary>
		/// <param name="fs">The configured filesystem.</param>
		/// <param name="conf">The configuration.</param>
		/// <param name="name">The name of the file.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="progress">The Progressable object to track progress.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(FileSystem fs, Configuration conf, 
			Path name, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec, Progressable progress)
		{
			return CreateWriter(conf, SequenceFile.Writer.File(name), SequenceFile.Writer.Filesystem
				(fs), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
				), SequenceFile.Writer.Compression(compressionType, codec), SequenceFile.Writer.
				Progressable(progress));
		}

		/// <summary>Construct the preferred type of 'raw' SequenceFile Writer.</summary>
		/// <param name="conf">The configuration.</param>
		/// <param name="out">The stream on top which the writer is to be constructed.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <param name="metadata">The metadata of the file.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(Configuration conf, FSDataOutputStream
			 @out, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec, SequenceFile.Metadata metadata)
		{
			return CreateWriter(conf, SequenceFile.Writer.Stream(@out), SequenceFile.Writer.KeyClass
				(keyClass), SequenceFile.Writer.ValueClass(valClass), SequenceFile.Writer.Compression
				(compressionType, codec), SequenceFile.Writer.Metadata(metadata));
		}

		/// <summary>Construct the preferred type of 'raw' SequenceFile Writer.</summary>
		/// <param name="conf">The configuration.</param>
		/// <param name="out">The stream on top which the writer is to be constructed.</param>
		/// <param name="keyClass">The 'key' type.</param>
		/// <param name="valClass">The 'value' type.</param>
		/// <param name="compressionType">The compression type.</param>
		/// <param name="codec">The compression codec.</param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
			)]
		public static SequenceFile.Writer CreateWriter(Configuration conf, FSDataOutputStream
			 @out, Type keyClass, Type valClass, SequenceFile.CompressionType compressionType
			, CompressionCodec codec)
		{
			return CreateWriter(conf, SequenceFile.Writer.Stream(@out), SequenceFile.Writer.KeyClass
				(keyClass), SequenceFile.Writer.ValueClass(valClass), SequenceFile.Writer.Compression
				(compressionType, codec));
		}

		/// <summary>The interface to 'raw' values of SequenceFiles.</summary>
		public interface ValueBytes
		{
			/// <summary>Writes the uncompressed bytes to the outStream.</summary>
			/// <param name="outStream">: Stream to write uncompressed bytes into.</param>
			/// <exception cref="System.IO.IOException"/>
			void WriteUncompressedBytes(DataOutputStream outStream);

			/// <summary>Write compressed bytes to outStream.</summary>
			/// <remarks>
			/// Write compressed bytes to outStream.
			/// Note: that it will NOT compress the bytes if they are not compressed.
			/// </remarks>
			/// <param name="outStream">: Stream to write compressed bytes into.</param>
			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			void WriteCompressedBytes(DataOutputStream outStream);

			/// <summary>Size of stored data.</summary>
			int GetSize();
		}

		private class UncompressedBytes : SequenceFile.ValueBytes
		{
			private int dataSize;

			private byte[] data;

			private UncompressedBytes()
			{
				data = null;
				dataSize = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			private void Reset(DataInputStream @in, int length)
			{
				if (data == null)
				{
					data = new byte[length];
				}
				else
				{
					if (length > data.Length)
					{
						data = new byte[Math.Max(length, data.Length * 2)];
					}
				}
				dataSize = -1;
				@in.ReadFully(data, 0, length);
				dataSize = length;
			}

			public virtual int GetSize()
			{
				return dataSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteUncompressedBytes(DataOutputStream outStream)
			{
				outStream.Write(data, 0, dataSize);
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteCompressedBytes(DataOutputStream outStream)
			{
				throw new ArgumentException("UncompressedBytes cannot be compressed!");
			}
		}

		private class CompressedBytes : SequenceFile.ValueBytes
		{
			private int dataSize;

			private byte[] data;

			internal DataInputBuffer rawData = null;

			internal CompressionCodec codec = null;

			internal CompressionInputStream decompressedStream = null;

			private CompressedBytes(CompressionCodec codec)
			{
				// UncompressedBytes
				data = null;
				dataSize = 0;
				this.codec = codec;
			}

			/// <exception cref="System.IO.IOException"/>
			private void Reset(DataInputStream @in, int length)
			{
				if (data == null)
				{
					data = new byte[length];
				}
				else
				{
					if (length > data.Length)
					{
						data = new byte[Math.Max(length, data.Length * 2)];
					}
				}
				dataSize = -1;
				@in.ReadFully(data, 0, length);
				dataSize = length;
			}

			public virtual int GetSize()
			{
				return dataSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteUncompressedBytes(DataOutputStream outStream)
			{
				if (decompressedStream == null)
				{
					rawData = new DataInputBuffer();
					decompressedStream = codec.CreateInputStream(rawData);
				}
				else
				{
					decompressedStream.ResetState();
				}
				rawData.Reset(data, 0, dataSize);
				byte[] buffer = new byte[8192];
				int bytesRead = 0;
				while ((bytesRead = decompressedStream.Read(buffer, 0, 8192)) != -1)
				{
					outStream.Write(buffer, 0, bytesRead);
				}
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteCompressedBytes(DataOutputStream outStream)
			{
				outStream.Write(data, 0, dataSize);
			}
		}

		/// <summary>The class encapsulating with the metadata of a file.</summary>
		/// <remarks>
		/// The class encapsulating with the metadata of a file.
		/// The metadata of a file is a list of attribute name/value
		/// pairs of Text type.
		/// </remarks>
		public class Metadata : Writable
		{
			private SortedDictionary<Text, Text> theMetadata;

			public Metadata()
				: this(new SortedDictionary<Text, Text>())
			{
			}

			public Metadata(SortedDictionary<Text, Text> arg)
			{
				// CompressedBytes
				if (arg == null)
				{
					this.theMetadata = new SortedDictionary<Text, Text>();
				}
				else
				{
					this.theMetadata = arg;
				}
			}

			public virtual Text Get(Text name)
			{
				return this.theMetadata[name];
			}

			public virtual void Set(Text name, Text value)
			{
				this.theMetadata[name] = value;
			}

			public virtual SortedDictionary<Text, Text> GetMetadata()
			{
				return new SortedDictionary<Text, Text>(this.theMetadata);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteInt(this.theMetadata.Count);
				IEnumerator<KeyValuePair<Text, Text>> iter = this.theMetadata.GetEnumerator();
				while (iter.HasNext())
				{
					KeyValuePair<Text, Text> en = iter.Next();
					en.Key.Write(@out);
					en.Value.Write(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				int sz = @in.ReadInt();
				if (sz < 0)
				{
					throw new IOException("Invalid size: " + sz + " for file metadata object");
				}
				this.theMetadata = new SortedDictionary<Text, Text>();
				for (int i = 0; i < sz; i++)
				{
					Text key = new Text();
					Text val = new Text();
					key.ReadFields(@in);
					val.ReadFields(@in);
					this.theMetadata[key] = val;
				}
			}

			public override bool Equals(object other)
			{
				if (other == null)
				{
					return false;
				}
				if (other.GetType() != this.GetType())
				{
					return false;
				}
				else
				{
					return Equals((SequenceFile.Metadata)other);
				}
			}

			public virtual bool Equals(SequenceFile.Metadata other)
			{
				if (other == null)
				{
					return false;
				}
				if (this.theMetadata.Count != other.theMetadata.Count)
				{
					return false;
				}
				IEnumerator<KeyValuePair<Text, Text>> iter1 = this.theMetadata.GetEnumerator();
				IEnumerator<KeyValuePair<Text, Text>> iter2 = other.theMetadata.GetEnumerator();
				while (iter1.HasNext() && iter2.HasNext())
				{
					KeyValuePair<Text, Text> en1 = iter1.Next();
					KeyValuePair<Text, Text> en2 = iter2.Next();
					if (!en1.Key.Equals(en2.Key))
					{
						return false;
					}
					if (!en1.Value.Equals(en2.Value))
					{
						return false;
					}
				}
				if (iter1.HasNext() || iter2.HasNext())
				{
					return false;
				}
				return true;
			}

			public override int GetHashCode()
			{
				System.Diagnostics.Debug.Assert(false, "hashCode not designed");
				return 42;
			}

			// any arbitrary constant will do 
			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("size: ").Append(this.theMetadata.Count).Append("\n");
				IEnumerator<KeyValuePair<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text>> iter
					 = this.theMetadata.GetEnumerator();
				while (iter.HasNext())
				{
					KeyValuePair<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> en = iter.Next
						();
					sb.Append("\t").Append(en.Key.ToString()).Append("\t").Append(en.Value.ToString()
						);
					sb.Append("\n");
				}
				return sb.ToString();
			}
		}

		/// <summary>Write key/value pairs to a sequence-format file.</summary>
		public class Writer : IDisposable, Syncable
		{
			private Configuration conf;

			internal FSDataOutputStream @out;

			internal bool ownOutputStream = true;

			internal DataOutputBuffer buffer = new DataOutputBuffer();

			internal Type keyClass;

			internal Type valClass;

			private readonly SequenceFile.CompressionType compress;

			internal CompressionCodec codec = null;

			internal CompressionOutputStream deflateFilter = null;

			internal DataOutputStream deflateOut = null;

			internal SequenceFile.Metadata metadata = null;

			internal Compressor compressor = null;

			private bool appendMode = false;

			protected internal Org.Apache.Hadoop.IO.Serializer.Serializer keySerializer;

			protected internal Org.Apache.Hadoop.IO.Serializer.Serializer uncompressedValSerializer;

			protected internal Org.Apache.Hadoop.IO.Serializer.Serializer compressedValSerializer;

			internal long lastSyncPos;

			internal byte[] sync;

			public interface Option
			{
				// Insert a globally unique 16-byte value every few entries, so that one
				// can seek into the middle of a file and then synchronize with record
				// starts and ends by scanning for this value.
				// position of last sync
				// 16 random bytes
			}

			internal class FileOption : Options.PathOption, SequenceFile.Writer.Option
			{
				internal FileOption(Path path)
					: base(path)
				{
				}
			}

			[System.ObsoleteAttribute(@"only used for backwards-compatibility in the createWriter methods that take FileSystem."
				)]
			private class FileSystemOption : SequenceFile.Writer.Option
			{
				private readonly FileSystem value;

				protected internal FileSystemOption(FileSystem value)
				{
					this.value = value;
				}

				public virtual FileSystem GetValue()
				{
					return value;
				}
			}

			internal class StreamOption : Options.FSDataOutputStreamOption, SequenceFile.Writer.Option
			{
				internal StreamOption(FSDataOutputStream stream)
					: base(stream)
				{
				}
			}

			internal class BufferSizeOption : Options.IntegerOption, SequenceFile.Writer.Option
			{
				internal BufferSizeOption(int value)
					: base(value)
				{
				}
			}

			internal class BlockSizeOption : Options.LongOption, SequenceFile.Writer.Option
			{
				internal BlockSizeOption(long value)
					: base(value)
				{
				}
			}

			internal class ReplicationOption : Options.IntegerOption, SequenceFile.Writer.Option
			{
				internal ReplicationOption(int value)
					: base(value)
				{
				}
			}

			internal class AppendIfExistsOption : Options.BooleanOption, SequenceFile.Writer.Option
			{
				internal AppendIfExistsOption(bool value)
					: base(value)
				{
				}
			}

			internal class KeyClassOption : Options.ClassOption, SequenceFile.Writer.Option
			{
				internal KeyClassOption(Type value)
					: base(value)
				{
				}
			}

			internal class ValueClassOption : Options.ClassOption, SequenceFile.Writer.Option
			{
				internal ValueClassOption(Type value)
					: base(value)
				{
				}
			}

			internal class MetadataOption : SequenceFile.Writer.Option
			{
				private readonly SequenceFile.Metadata value;

				internal MetadataOption(SequenceFile.Metadata value)
				{
					this.value = value;
				}

				internal virtual SequenceFile.Metadata GetValue()
				{
					return value;
				}
			}

			internal class ProgressableOption : Options.ProgressableOption, SequenceFile.Writer.Option
			{
				internal ProgressableOption(Progressable value)
					: base(value)
				{
				}
			}

			private class CompressionOption : SequenceFile.Writer.Option
			{
				private readonly SequenceFile.CompressionType value;

				private readonly CompressionCodec codec;

				internal CompressionOption(SequenceFile.CompressionType value)
					: this(value, null)
				{
				}

				internal CompressionOption(SequenceFile.CompressionType value, CompressionCodec codec
					)
				{
					this.value = value;
					this.codec = (SequenceFile.CompressionType.None != value && null == codec) ? new 
						DefaultCodec() : codec;
				}

				internal virtual SequenceFile.CompressionType GetValue()
				{
					return value;
				}

				internal virtual CompressionCodec GetCodec()
				{
					return codec;
				}
			}

			public static SequenceFile.Writer.Option File(Path value)
			{
				return new SequenceFile.Writer.FileOption(value);
			}

			[System.ObsoleteAttribute(@"only used for backwards-compatibility in the createWriter methods that take FileSystem."
				)]
			private static SequenceFile.Writer.Option Filesystem(FileSystem fs)
			{
				return new SequenceFile.Writer.FileSystemOption(fs);
			}

			public static SequenceFile.Writer.Option BufferSize(int value)
			{
				return new SequenceFile.Writer.BufferSizeOption(value);
			}

			public static SequenceFile.Writer.Option Stream(FSDataOutputStream value)
			{
				return new SequenceFile.Writer.StreamOption(value);
			}

			public static SequenceFile.Writer.Option Replication(short value)
			{
				return new SequenceFile.Writer.ReplicationOption(value);
			}

			public static SequenceFile.Writer.Option AppendIfExists(bool value)
			{
				return new SequenceFile.Writer.AppendIfExistsOption(value);
			}

			public static SequenceFile.Writer.Option BlockSize(long value)
			{
				return new SequenceFile.Writer.BlockSizeOption(value);
			}

			public static SequenceFile.Writer.Option Progressable(Progressable value)
			{
				return new SequenceFile.Writer.ProgressableOption(value);
			}

			public static SequenceFile.Writer.Option KeyClass(Type value)
			{
				return new SequenceFile.Writer.KeyClassOption(value);
			}

			public static SequenceFile.Writer.Option ValueClass(Type value)
			{
				return new SequenceFile.Writer.ValueClassOption(value);
			}

			public static SequenceFile.Writer.Option Metadata(SequenceFile.Metadata value)
			{
				return new SequenceFile.Writer.MetadataOption(value);
			}

			public static SequenceFile.Writer.Option Compression(SequenceFile.CompressionType
				 value)
			{
				return new SequenceFile.Writer.CompressionOption(value);
			}

			public static SequenceFile.Writer.Option Compression(SequenceFile.CompressionType
				 value, CompressionCodec codec)
			{
				return new SequenceFile.Writer.CompressionOption(value, codec);
			}

			/// <summary>Construct a uncompressed writer from a set of options.</summary>
			/// <param name="conf">the configuration to use</param>
			/// <param name="options">the options used when creating the writer</param>
			/// <exception cref="System.IO.IOException">if it fails</exception>
			internal Writer(Configuration conf, params SequenceFile.Writer.Option[] opts)
			{
				{
					try
					{
						MessageDigest digester = MessageDigest.GetInstance("MD5");
						long time = Time.Now();
						digester.Update(Sharpen.Runtime.GetBytesForString((new UID() + "@" + time), Charsets
							.Utf8));
						sync = digester.Digest();
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				SequenceFile.Writer.BlockSizeOption blockSizeOption = Options.GetOption<SequenceFile.Writer.BlockSizeOption
					>(opts);
				SequenceFile.Writer.BufferSizeOption bufferSizeOption = Options.GetOption<SequenceFile.Writer.BufferSizeOption
					>(opts);
				SequenceFile.Writer.ReplicationOption replicationOption = Options.GetOption<SequenceFile.Writer.ReplicationOption
					>(opts);
				SequenceFile.Writer.ProgressableOption progressOption = Options.GetOption<SequenceFile.Writer.ProgressableOption
					>(opts);
				SequenceFile.Writer.FileOption fileOption = Options.GetOption<SequenceFile.Writer.FileOption
					>(opts);
				SequenceFile.Writer.AppendIfExistsOption appendIfExistsOption = Options.GetOption
					<SequenceFile.Writer.AppendIfExistsOption>(opts);
				SequenceFile.Writer.FileSystemOption fsOption = Options.GetOption<SequenceFile.Writer.FileSystemOption
					>(opts);
				SequenceFile.Writer.StreamOption streamOption = Options.GetOption<SequenceFile.Writer.StreamOption
					>(opts);
				SequenceFile.Writer.KeyClassOption keyClassOption = Options.GetOption<SequenceFile.Writer.KeyClassOption
					>(opts);
				SequenceFile.Writer.ValueClassOption valueClassOption = Options.GetOption<SequenceFile.Writer.ValueClassOption
					>(opts);
				SequenceFile.Writer.MetadataOption metadataOption = Options.GetOption<SequenceFile.Writer.MetadataOption
					>(opts);
				SequenceFile.Writer.CompressionOption compressionTypeOption = Options.GetOption<SequenceFile.Writer.CompressionOption
					>(opts);
				// check consistency of options
				if ((fileOption == null) == (streamOption == null))
				{
					throw new ArgumentException("file or stream must be specified");
				}
				if (fileOption == null && (blockSizeOption != null || bufferSizeOption != null ||
					 replicationOption != null || progressOption != null))
				{
					throw new ArgumentException("file modifier options not " + "compatible with stream"
						);
				}
				FSDataOutputStream @out;
				bool ownStream = fileOption != null;
				if (ownStream)
				{
					Path p = fileOption.GetValue();
					FileSystem fs;
					if (fsOption != null)
					{
						fs = fsOption.GetValue();
					}
					else
					{
						fs = p.GetFileSystem(conf);
					}
					int bufferSize = bufferSizeOption == null ? GetBufferSize(conf) : bufferSizeOption
						.GetValue();
					short replication = replicationOption == null ? fs.GetDefaultReplication(p) : (short
						)replicationOption.GetValue();
					long blockSize = blockSizeOption == null ? fs.GetDefaultBlockSize(p) : blockSizeOption
						.GetValue();
					Progressable progress = progressOption == null ? null : progressOption.GetValue();
					if (appendIfExistsOption != null && appendIfExistsOption.GetValue() && fs.Exists(
						p))
					{
						// Read the file and verify header details
						SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.File
							(p), new SequenceFile.Reader.OnlyHeaderOption());
						try
						{
							if (keyClassOption.GetValue() != reader.GetKeyClass() || valueClassOption.GetValue
								() != reader.GetValueClass())
							{
								throw new ArgumentException("Key/value class provided does not match the file");
							}
							if (reader.GetVersion() != Version[3])
							{
								throw new VersionMismatchException(Version[3], reader.GetVersion());
							}
							if (metadataOption != null)
							{
								Log.Info("MetaData Option is ignored during append");
							}
							metadataOption = (SequenceFile.Writer.MetadataOption)SequenceFile.Writer.Metadata
								(reader.GetMetadata());
							SequenceFile.Writer.CompressionOption readerCompressionOption = new SequenceFile.Writer.CompressionOption
								(reader.GetCompressionType(), reader.GetCompressionCodec());
							if (readerCompressionOption.value != compressionTypeOption.value || !readerCompressionOption
								.codec.GetType().FullName.Equals(compressionTypeOption.codec.GetType().FullName))
							{
								throw new ArgumentException("Compression option provided does not match the file"
									);
							}
							sync = reader.GetSync();
						}
						finally
						{
							reader.Close();
						}
						@out = fs.Append(p, bufferSize, progress);
						this.appendMode = true;
					}
					else
					{
						@out = fs.Create(p, true, bufferSize, replication, blockSize, progress);
					}
				}
				else
				{
					@out = streamOption.GetValue();
				}
				Type keyClass = keyClassOption == null ? typeof(object) : keyClassOption.GetValue
					();
				Type valueClass = valueClassOption == null ? typeof(object) : valueClassOption.GetValue
					();
				SequenceFile.Metadata metadata = metadataOption == null ? new SequenceFile.Metadata
					() : metadataOption.GetValue();
				this.compress = compressionTypeOption.GetValue();
				CompressionCodec codec = compressionTypeOption.GetCodec();
				if (codec != null && (codec is GzipCodec) && !NativeCodeLoader.IsNativeCodeLoaded
					() && !ZlibFactory.IsNativeZlibLoaded(conf))
				{
					throw new ArgumentException("SequenceFile doesn't work with " + "GzipCodec without native-hadoop "
						 + "code!");
				}
				Init(conf, @out, ownStream, keyClass, valueClass, codec, metadata);
			}

			/// <summary>Create the named file.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
				)]
			public Writer(FileSystem fs, Configuration conf, Path name, Type keyClass, Type valClass
				)
			{
				{
					try
					{
						MessageDigest digester = MessageDigest.GetInstance("MD5");
						long time = Time.Now();
						digester.Update(Sharpen.Runtime.GetBytesForString((new UID() + "@" + time), Charsets
							.Utf8));
						sync = digester.Digest();
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				this.compress = SequenceFile.CompressionType.None;
				Init(conf, fs.Create(name), true, keyClass, valClass, null, new SequenceFile.Metadata
					());
			}

			/// <summary>Create the named file with write-progress reporter.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
				)]
			public Writer(FileSystem fs, Configuration conf, Path name, Type keyClass, Type valClass
				, Progressable progress, SequenceFile.Metadata metadata)
			{
				{
					try
					{
						MessageDigest digester = MessageDigest.GetInstance("MD5");
						long time = Time.Now();
						digester.Update(Sharpen.Runtime.GetBytesForString((new UID() + "@" + time), Charsets
							.Utf8));
						sync = digester.Digest();
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				this.compress = SequenceFile.CompressionType.None;
				Init(conf, fs.Create(name, progress), true, keyClass, valClass, null, metadata);
			}

			/// <summary>Create the named file with write-progress reporter.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.CreateWriter(Org.Apache.Hadoop.Conf.Configuration, Option[]) instead."
				)]
			public Writer(FileSystem fs, Configuration conf, Path name, Type keyClass, Type valClass
				, int bufferSize, short replication, long blockSize, Progressable progress, SequenceFile.Metadata
				 metadata)
			{
				{
					try
					{
						MessageDigest digester = MessageDigest.GetInstance("MD5");
						long time = Time.Now();
						digester.Update(Sharpen.Runtime.GetBytesForString((new UID() + "@" + time), Charsets
							.Utf8));
						sync = digester.Digest();
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				this.compress = SequenceFile.CompressionType.None;
				Init(conf, fs.Create(name, true, bufferSize, replication, blockSize, progress), true
					, keyClass, valClass, null, metadata);
			}

			internal virtual bool IsCompressed()
			{
				return compress != SequenceFile.CompressionType.None;
			}

			internal virtual bool IsBlockCompressed()
			{
				return compress == SequenceFile.CompressionType.Block;
			}

			internal virtual SequenceFile.Writer OwnStream()
			{
				this.ownOutputStream = true;
				return this;
			}

			/// <summary>Write and flush the file header.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void WriteFileHeader()
			{
				@out.Write(Version);
				Org.Apache.Hadoop.IO.Text.WriteString(@out, keyClass.FullName);
				Org.Apache.Hadoop.IO.Text.WriteString(@out, valClass.FullName);
				@out.WriteBoolean(this.IsCompressed());
				@out.WriteBoolean(this.IsBlockCompressed());
				if (this.IsCompressed())
				{
					Org.Apache.Hadoop.IO.Text.WriteString(@out, (codec.GetType()).FullName);
				}
				this.metadata.Write(@out);
				@out.Write(sync);
				// write the sync bytes
				@out.Flush();
			}

			// flush header
			/// <summary>Initialize.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void Init(Configuration conf, FSDataOutputStream @out, bool ownStream
				, Type keyClass, Type valClass, CompressionCodec codec, SequenceFile.Metadata metadata
				)
			{
				this.conf = conf;
				this.@out = @out;
				this.ownOutputStream = ownStream;
				this.keyClass = keyClass;
				this.valClass = valClass;
				this.codec = codec;
				this.metadata = metadata;
				SerializationFactory serializationFactory = new SerializationFactory(conf);
				this.keySerializer = serializationFactory.GetSerializer(keyClass);
				if (this.keySerializer == null)
				{
					throw new IOException("Could not find a serializer for the Key class: '" + keyClass
						.GetCanonicalName() + "'. " + "Please ensure that the configuration '" + CommonConfigurationKeys
						.IoSerializationsKey + "' is " + "properly configured, if you're using" + "custom serialization."
						);
				}
				this.keySerializer.Open(buffer);
				this.uncompressedValSerializer = serializationFactory.GetSerializer(valClass);
				if (this.uncompressedValSerializer == null)
				{
					throw new IOException("Could not find a serializer for the Value class: '" + valClass
						.GetCanonicalName() + "'. " + "Please ensure that the configuration '" + CommonConfigurationKeys
						.IoSerializationsKey + "' is " + "properly configured, if you're using" + "custom serialization."
						);
				}
				this.uncompressedValSerializer.Open(buffer);
				if (this.codec != null)
				{
					ReflectionUtils.SetConf(this.codec, this.conf);
					this.compressor = CodecPool.GetCompressor(this.codec);
					this.deflateFilter = this.codec.CreateOutputStream(buffer, compressor);
					this.deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
					this.compressedValSerializer = serializationFactory.GetSerializer(valClass);
					if (this.compressedValSerializer == null)
					{
						throw new IOException("Could not find a serializer for the Value class: '" + valClass
							.GetCanonicalName() + "'. " + "Please ensure that the configuration '" + CommonConfigurationKeys
							.IoSerializationsKey + "' is " + "properly configured, if you're using" + "custom serialization."
							);
					}
					this.compressedValSerializer.Open(deflateOut);
				}
				if (appendMode)
				{
					Sync();
				}
				else
				{
					WriteFileHeader();
				}
			}

			/// <summary>Returns the class of keys in this file.</summary>
			public virtual Type GetKeyClass()
			{
				return keyClass;
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual Type GetValueClass()
			{
				return valClass;
			}

			/// <summary>Returns the compression codec of data in this file.</summary>
			public virtual CompressionCodec GetCompressionCodec()
			{
				return codec;
			}

			/// <summary>create a sync point</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Sync()
			{
				if (sync != null && lastSyncPos != @out.GetPos())
				{
					@out.WriteInt(SyncEscape);
					// mark the start of the sync
					@out.Write(sync);
					// write sync
					lastSyncPos = @out.GetPos();
				}
			}

			// update lastSyncPos
			/// <summary>flush all currently written data to the file system</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Hsync() or Hflush() instead")]
			public virtual void SyncFs()
			{
				if (@out != null)
				{
					@out.Sync();
				}
			}

			// flush contents to file system
			/// <exception cref="System.IO.IOException"/>
			public virtual void Hsync()
			{
				if (@out != null)
				{
					@out.Hsync();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Hflush()
			{
				if (@out != null)
				{
					@out.Hflush();
				}
			}

			/// <summary>Returns the configuration of this file.</summary>
			internal virtual Configuration GetConf()
			{
				return conf;
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				lock (this)
				{
					keySerializer.Close();
					uncompressedValSerializer.Close();
					if (compressedValSerializer != null)
					{
						compressedValSerializer.Close();
					}
					CodecPool.ReturnCompressor(compressor);
					compressor = null;
					if (@out != null)
					{
						// Close the underlying stream iff we own it...
						if (ownOutputStream)
						{
							@out.Close();
						}
						else
						{
							@out.Flush();
						}
						@out = null;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CheckAndWriteSync()
			{
				lock (this)
				{
					if (sync != null && @out.GetPos() >= lastSyncPos + SyncInterval)
					{
						// time to emit sync
						Sync();
					}
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(Writable key, Writable val)
			{
				Append((object)key, (object)val);
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(object key, object val)
			{
				lock (this)
				{
					if (key.GetType() != keyClass)
					{
						throw new IOException("wrong key class: " + key.GetType().FullName + " is not " +
							 keyClass);
					}
					if (val.GetType() != valClass)
					{
						throw new IOException("wrong value class: " + val.GetType().FullName + " is not "
							 + valClass);
					}
					buffer.Reset();
					// Append the 'key'
					keySerializer.Serialize(key);
					int keyLength = buffer.GetLength();
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed: " + key);
					}
					// Append the 'value'
					if (compress == SequenceFile.CompressionType.Record)
					{
						deflateFilter.ResetState();
						compressedValSerializer.Serialize(val);
						deflateOut.Flush();
						deflateFilter.Finish();
					}
					else
					{
						uncompressedValSerializer.Serialize(val);
					}
					// Write the record out
					CheckAndWriteSync();
					// sync
					@out.WriteInt(buffer.GetLength());
					// total record length
					@out.WriteInt(keyLength);
					// key portion length
					@out.Write(buffer.GetData(), 0, buffer.GetLength());
				}
			}

			// data
			/// <exception cref="System.IO.IOException"/>
			public virtual void AppendRaw(byte[] keyData, int keyOffset, int keyLength, SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed: " + keyLength);
					}
					int valLength = val.GetSize();
					CheckAndWriteSync();
					@out.WriteInt(keyLength + valLength);
					// total record length
					@out.WriteInt(keyLength);
					// key portion length
					@out.Write(keyData, keyOffset, keyLength);
					// key
					val.WriteUncompressedBytes(@out);
				}
			}

			// value
			/// <summary>Returns the current length of the output file.</summary>
			/// <remarks>
			/// Returns the current length of the output file.
			/// <p>This always returns a synchronized position.  In other words,
			/// immediately after calling
			/// <see cref="Reader.Seek(long)"/>
			/// with a position
			/// returned by this method,
			/// <see cref="Reader.Next(Writable)"/>
			/// may be called.  However
			/// the key may be earlier in the file than key last written when this
			/// method was called (e.g., with block-compression, it may be the first key
			/// in the block that was being written when this method was called).
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetLength()
			{
				lock (this)
				{
					return @out.GetPos();
				}
			}
		}

		/// <summary>Write key/compressed-value pairs to a sequence-format file.</summary>
		internal class RecordCompressWriter : SequenceFile.Writer
		{
			/// <exception cref="System.IO.IOException"/>
			internal RecordCompressWriter(Configuration conf, params SequenceFile.Writer.Option
				[] options)
				: base(conf, options)
			{
			}

			// class Writer
			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Append(object key, object val)
			{
				lock (this)
				{
					if (key.GetType() != keyClass)
					{
						throw new IOException("wrong key class: " + key.GetType().FullName + " is not " +
							 keyClass);
					}
					if (val.GetType() != valClass)
					{
						throw new IOException("wrong value class: " + val.GetType().FullName + " is not "
							 + valClass);
					}
					buffer.Reset();
					// Append the 'key'
					keySerializer.Serialize(key);
					int keyLength = buffer.GetLength();
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed: " + key);
					}
					// Compress 'value' and append it
					deflateFilter.ResetState();
					compressedValSerializer.Serialize(val);
					deflateOut.Flush();
					deflateFilter.Finish();
					// Write the record out
					CheckAndWriteSync();
					// sync
					@out.WriteInt(buffer.GetLength());
					// total record length
					@out.WriteInt(keyLength);
					// key portion length
					@out.Write(buffer.GetData(), 0, buffer.GetLength());
				}
			}

			// data
			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void AppendRaw(byte[] keyData, int keyOffset, int keyLength, SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed: " + keyLength);
					}
					int valLength = val.GetSize();
					CheckAndWriteSync();
					// sync
					@out.WriteInt(keyLength + valLength);
					// total record length
					@out.WriteInt(keyLength);
					// key portion length
					@out.Write(keyData, keyOffset, keyLength);
					// 'key' data
					val.WriteCompressedBytes(@out);
				}
			}
			// 'value' data
		}

		/// <summary>Write compressed key/value blocks to a sequence-format file.</summary>
		internal class BlockCompressWriter : SequenceFile.Writer
		{
			private int noBufferedRecords = 0;

			private DataOutputBuffer keyLenBuffer = new DataOutputBuffer();

			private DataOutputBuffer keyBuffer = new DataOutputBuffer();

			private DataOutputBuffer valLenBuffer = new DataOutputBuffer();

			private DataOutputBuffer valBuffer = new DataOutputBuffer();

			private readonly int compressionBlockSize;

			/// <exception cref="System.IO.IOException"/>
			internal BlockCompressWriter(Configuration conf, params SequenceFile.Writer.Option
				[] options)
				: base(conf, options)
			{
				// RecordCompressionWriter
				compressionBlockSize = conf.GetInt("io.seqfile.compress.blocksize", 1000000);
				keySerializer.Close();
				keySerializer.Open(keyBuffer);
				uncompressedValSerializer.Close();
				uncompressedValSerializer.Open(valBuffer);
			}

			/// <summary>Workhorse to check and write out compressed data/lengths</summary>
			/// <exception cref="System.IO.IOException"/>
			private void WriteBuffer(DataOutputBuffer uncompressedDataBuffer)
			{
				lock (this)
				{
					deflateFilter.ResetState();
					buffer.Reset();
					deflateOut.Write(uncompressedDataBuffer.GetData(), 0, uncompressedDataBuffer.GetLength
						());
					deflateOut.Flush();
					deflateFilter.Finish();
					WritableUtils.WriteVInt(@out, buffer.GetLength());
					@out.Write(buffer.GetData(), 0, buffer.GetLength());
				}
			}

			/// <summary>Compress and flush contents to dfs</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Sync()
			{
				lock (this)
				{
					if (noBufferedRecords > 0)
					{
						base.Sync();
						// No. of records
						WritableUtils.WriteVInt(@out, noBufferedRecords);
						// Write 'keys' and lengths
						WriteBuffer(keyLenBuffer);
						WriteBuffer(keyBuffer);
						// Write 'values' and lengths
						WriteBuffer(valLenBuffer);
						WriteBuffer(valBuffer);
						// Flush the file-stream
						@out.Flush();
						// Reset internal states
						keyLenBuffer.Reset();
						keyBuffer.Reset();
						valLenBuffer.Reset();
						valBuffer.Reset();
						noBufferedRecords = 0;
					}
				}
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				lock (this)
				{
					if (@out != null)
					{
						Sync();
					}
					base.Close();
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Append(object key, object val)
			{
				lock (this)
				{
					if (key.GetType() != keyClass)
					{
						throw new IOException("wrong key class: " + key + " is not " + keyClass);
					}
					if (val.GetType() != valClass)
					{
						throw new IOException("wrong value class: " + val + " is not " + valClass);
					}
					// Save key/value into respective buffers 
					int oldKeyLength = keyBuffer.GetLength();
					keySerializer.Serialize(key);
					int keyLength = keyBuffer.GetLength() - oldKeyLength;
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed: " + key);
					}
					WritableUtils.WriteVInt(keyLenBuffer, keyLength);
					int oldValLength = valBuffer.GetLength();
					uncompressedValSerializer.Serialize(val);
					int valLength = valBuffer.GetLength() - oldValLength;
					WritableUtils.WriteVInt(valLenBuffer, valLength);
					// Added another key/value pair
					++noBufferedRecords;
					// Compress and flush?
					int currentBlockSize = keyBuffer.GetLength() + valBuffer.GetLength();
					if (currentBlockSize >= compressionBlockSize)
					{
						Sync();
					}
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void AppendRaw(byte[] keyData, int keyOffset, int keyLength, SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new IOException("negative length keys not allowed");
					}
					int valLength = val.GetSize();
					// Save key/value data in relevant buffers
					WritableUtils.WriteVInt(keyLenBuffer, keyLength);
					keyBuffer.Write(keyData, keyOffset, keyLength);
					WritableUtils.WriteVInt(valLenBuffer, valLength);
					val.WriteUncompressedBytes(valBuffer);
					// Added another key/value pair
					++noBufferedRecords;
					// Compress and flush?
					int currentBlockSize = keyBuffer.GetLength() + valBuffer.GetLength();
					if (currentBlockSize >= compressionBlockSize)
					{
						Sync();
					}
				}
			}
		}

		// BlockCompressionWriter
		/// <summary>Get the configured buffer size</summary>
		private static int GetBufferSize(Configuration conf)
		{
			return conf.GetInt("io.file.buffer.size", 4096);
		}

		/// <summary>Reads key/value pairs from a sequence-format file.</summary>
		public class Reader : IDisposable
		{
			private string filename;

			private FSDataInputStream @in;

			private DataOutputBuffer outBuf = new DataOutputBuffer();

			private byte version;

			private string keyClassName;

			private string valClassName;

			private Type keyClass;

			private Type valClass;

			private CompressionCodec codec = null;

			private SequenceFile.Metadata metadata = null;

			private byte[] sync = new byte[SyncHashSize];

			private byte[] syncCheck = new byte[SyncHashSize];

			private bool syncSeen;

			private long headerEnd;

			private long end;

			private int keyLength;

			private int recordLength;

			private bool decompress;

			private bool blockCompressed;

			private Configuration conf;

			private int noBufferedRecords = 0;

			private bool lazyDecompress = true;

			private bool valuesDecompressed = true;

			private int noBufferedKeys = 0;

			private int noBufferedValues = 0;

			private DataInputBuffer keyLenBuffer = null;

			private CompressionInputStream keyLenInFilter = null;

			private DataInputStream keyLenIn = null;

			private Decompressor keyLenDecompressor = null;

			private DataInputBuffer keyBuffer = null;

			private CompressionInputStream keyInFilter = null;

			private DataInputStream keyIn = null;

			private Decompressor keyDecompressor = null;

			private DataInputBuffer valLenBuffer = null;

			private CompressionInputStream valLenInFilter = null;

			private DataInputStream valLenIn = null;

			private Decompressor valLenDecompressor = null;

			private DataInputBuffer valBuffer = null;

			private CompressionInputStream valInFilter = null;

			private DataInputStream valIn = null;

			private Decompressor valDecompressor = null;

			private Deserializer keyDeserializer;

			private Deserializer valDeserializer;

			/// <summary>A tag interface for all of the Reader options</summary>
			public interface Option
			{
			}

			/// <summary>Create an option to specify the path name of the sequence file.</summary>
			/// <param name="value">the path to read</param>
			/// <returns>a new option</returns>
			public static SequenceFile.Reader.Option File(Path value)
			{
				return new SequenceFile.Reader.FileOption(value);
			}

			/// <summary>Create an option to specify the stream with the sequence file.</summary>
			/// <param name="value">the stream to read.</param>
			/// <returns>a new option</returns>
			public static SequenceFile.Reader.Option Stream(FSDataInputStream value)
			{
				return new SequenceFile.Reader.InputStreamOption(value);
			}

			/// <summary>Create an option to specify the starting byte to read.</summary>
			/// <param name="value">the number of bytes to skip over</param>
			/// <returns>a new option</returns>
			public static SequenceFile.Reader.Option Start(long value)
			{
				return new SequenceFile.Reader.StartOption(value);
			}

			/// <summary>Create an option to specify the number of bytes to read.</summary>
			/// <param name="value">the number of bytes to read</param>
			/// <returns>a new option</returns>
			public static SequenceFile.Reader.Option Length(long value)
			{
				return new SequenceFile.Reader.LengthOption(value);
			}

			/// <summary>Create an option with the buffer size for reading the given pathname.</summary>
			/// <param name="value">the number of bytes to buffer</param>
			/// <returns>a new option</returns>
			public static SequenceFile.Reader.Option BufferSize(int value)
			{
				return new SequenceFile.Reader.BufferSizeOption(value);
			}

			private class FileOption : Options.PathOption, SequenceFile.Reader.Option
			{
				private FileOption(Path value)
					: base(value)
				{
				}
			}

			private class InputStreamOption : Options.FSDataInputStreamOption, SequenceFile.Reader.Option
			{
				private InputStreamOption(FSDataInputStream value)
					: base(value)
				{
				}
			}

			private class StartOption : Options.LongOption, SequenceFile.Reader.Option
			{
				private StartOption(long value)
					: base(value)
				{
				}
			}

			private class LengthOption : Options.LongOption, SequenceFile.Reader.Option
			{
				private LengthOption(long value)
					: base(value)
				{
				}
			}

			private class BufferSizeOption : Options.IntegerOption, SequenceFile.Reader.Option
			{
				private BufferSizeOption(int value)
					: base(value)
				{
				}
			}

			private class OnlyHeaderOption : Options.BooleanOption, SequenceFile.Reader.Option
			{
				private OnlyHeaderOption()
					: base(true)
				{
				}
				// only used directly
			}

			/// <exception cref="System.IO.IOException"/>
			public Reader(Configuration conf, params SequenceFile.Reader.Option[] opts)
			{
				// Look up the options, these are null if not set
				SequenceFile.Reader.FileOption fileOpt = Options.GetOption<SequenceFile.Reader.FileOption
					>(opts);
				SequenceFile.Reader.InputStreamOption streamOpt = Options.GetOption<SequenceFile.Reader.InputStreamOption
					>(opts);
				SequenceFile.Reader.StartOption startOpt = Options.GetOption<SequenceFile.Reader.StartOption
					>(opts);
				SequenceFile.Reader.LengthOption lenOpt = Options.GetOption<SequenceFile.Reader.LengthOption
					>(opts);
				SequenceFile.Reader.BufferSizeOption bufOpt = Options.GetOption<SequenceFile.Reader.BufferSizeOption
					>(opts);
				SequenceFile.Reader.OnlyHeaderOption headerOnly = Options.GetOption<SequenceFile.Reader.OnlyHeaderOption
					>(opts);
				// check for consistency
				if ((fileOpt == null) == (streamOpt == null))
				{
					throw new ArgumentException("File or stream option must be specified");
				}
				if (fileOpt == null && bufOpt != null)
				{
					throw new ArgumentException("buffer size can only be set when" + " a file is specified."
						);
				}
				// figure out the real values
				Path filename = null;
				FSDataInputStream file;
				long len;
				if (fileOpt != null)
				{
					filename = fileOpt.GetValue();
					FileSystem fs = filename.GetFileSystem(conf);
					int bufSize = bufOpt == null ? GetBufferSize(conf) : bufOpt.GetValue();
					len = null == lenOpt ? fs.GetFileStatus(filename).GetLen() : lenOpt.GetValue();
					file = OpenFile(fs, filename, bufSize, len);
				}
				else
				{
					len = null == lenOpt ? long.MaxValue : lenOpt.GetValue();
					file = streamOpt.GetValue();
				}
				long start = startOpt == null ? 0 : startOpt.GetValue();
				// really set up
				Initialize(filename, file, start, len, conf, headerOnly != null);
			}

			/// <summary>Construct a reader by opening a file from the given file system.</summary>
			/// <param name="fs">The file system used to open the file.</param>
			/// <param name="file">The file being read.</param>
			/// <param name="conf">Configuration</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Reader(Configuration, Option...) instead.")]
			public Reader(FileSystem fs, Path file, Configuration conf)
				: this(conf, File(file.MakeQualified(fs)))
			{
			}

			/// <summary>Construct a reader by the given input stream.</summary>
			/// <param name="in">An input stream.</param>
			/// <param name="buffersize">unused</param>
			/// <param name="start">The starting position.</param>
			/// <param name="length">The length being read.</param>
			/// <param name="conf">Configuration</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Reader(Configuration, Reader.Option...) instead."
				)]
			public Reader(FSDataInputStream @in, int buffersize, long start, long length, Configuration
				 conf)
				: this(conf, Stream(@in), Start(start), Length(length))
			{
			}

			/// <summary>Common work of the constructors.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void Initialize(Path filename, FSDataInputStream @in, long start, long length
				, Configuration conf, bool tempReader)
			{
				if (@in == null)
				{
					throw new ArgumentException("in == null");
				}
				this.filename = filename == null ? "<unknown>" : filename.ToString();
				this.@in = @in;
				this.conf = conf;
				bool succeeded = false;
				try
				{
					Seek(start);
					this.end = this.@in.GetPos() + length;
					// if it wrapped around, use the max
					if (end < length)
					{
						end = long.MaxValue;
					}
					Init(tempReader);
					succeeded = true;
				}
				finally
				{
					if (!succeeded)
					{
						IOUtils.Cleanup(Log, this.@in);
					}
				}
			}

			/// <summary>
			/// Override this method to specialize the type of
			/// <see cref="Org.Apache.Hadoop.FS.FSDataInputStream"/>
			/// returned.
			/// </summary>
			/// <param name="fs">The file system used to open the file.</param>
			/// <param name="file">The file being read.</param>
			/// <param name="bufferSize">The buffer size used to read the file.</param>
			/// <param name="length">
			/// The length being read if it is &gt;= 0.  Otherwise,
			/// the length is not available.
			/// </param>
			/// <returns>The opened stream.</returns>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual FSDataInputStream OpenFile(FileSystem fs, Path file, int
				 bufferSize, long length)
			{
				return fs.Open(file, bufferSize);
			}

			/// <summary>
			/// Initialize the
			/// <see cref="Reader"/>
			/// </summary>
			/// <param name="tmpReader">
			/// <code>true</code> if we are constructing a temporary
			/// reader
			/// <see cref="SequenceFile.Sorter.cloneFileAttributes"/>
			/// ,
			/// and hence do not initialize every component;
			/// <code>false</code> otherwise.
			/// </param>
			/// <exception cref="System.IO.IOException"/>
			private void Init(bool tempReader)
			{
				byte[] versionBlock = new byte[Version.Length];
				@in.ReadFully(versionBlock);
				if ((versionBlock[0] != Version[0]) || (versionBlock[1] != Version[1]) || (versionBlock
					[2] != Version[2]))
				{
					throw new IOException(this + " not a SequenceFile");
				}
				// Set 'version'
				version = versionBlock[3];
				if (version > Version[3])
				{
					throw new VersionMismatchException(Version[3], version);
				}
				if (((sbyte)version) < BlockCompressVersion)
				{
					UTF8 className = new UTF8();
					className.ReadFields(@in);
					keyClassName = className.ToStringChecked();
					// key class name
					className.ReadFields(@in);
					valClassName = className.ToStringChecked();
				}
				else
				{
					// val class name
					keyClassName = Org.Apache.Hadoop.IO.Text.ReadString(@in);
					valClassName = Org.Apache.Hadoop.IO.Text.ReadString(@in);
				}
				if (version > 2)
				{
					// if version > 2
					this.decompress = @in.ReadBoolean();
				}
				else
				{
					// is compressed?
					decompress = false;
				}
				if (version >= BlockCompressVersion)
				{
					// if version >= 4
					this.blockCompressed = @in.ReadBoolean();
				}
				else
				{
					// is block-compressed?
					blockCompressed = false;
				}
				// if version >= 5
				// setup the compression codec
				if (decompress)
				{
					if (version >= CustomCompressVersion)
					{
						string codecClassname = Org.Apache.Hadoop.IO.Text.ReadString(@in);
						try
						{
							Type codecClass = conf.GetClassByName(codecClassname).AsSubclass<CompressionCodec
								>();
							this.codec = ReflectionUtils.NewInstance(codecClass, conf);
						}
						catch (TypeLoadException cnfe)
						{
							throw new ArgumentException("Unknown codec: " + codecClassname, cnfe);
						}
					}
					else
					{
						codec = new DefaultCodec();
						((Configurable)codec).SetConf(conf);
					}
				}
				this.metadata = new SequenceFile.Metadata();
				if (version >= VersionWithMetadata)
				{
					// if version >= 6
					this.metadata.ReadFields(@in);
				}
				if (version > 1)
				{
					// if version > 1
					@in.ReadFully(sync);
					// read sync bytes
					headerEnd = @in.GetPos();
				}
				// record end of header
				// Initialize... *not* if this we are constructing a temporary Reader
				if (!tempReader)
				{
					valBuffer = new DataInputBuffer();
					if (decompress)
					{
						valDecompressor = CodecPool.GetDecompressor(codec);
						valInFilter = codec.CreateInputStream(valBuffer, valDecompressor);
						valIn = new DataInputStream(valInFilter);
					}
					else
					{
						valIn = valBuffer;
					}
					if (blockCompressed)
					{
						keyLenBuffer = new DataInputBuffer();
						keyBuffer = new DataInputBuffer();
						valLenBuffer = new DataInputBuffer();
						keyLenDecompressor = CodecPool.GetDecompressor(codec);
						keyLenInFilter = codec.CreateInputStream(keyLenBuffer, keyLenDecompressor);
						keyLenIn = new DataInputStream(keyLenInFilter);
						keyDecompressor = CodecPool.GetDecompressor(codec);
						keyInFilter = codec.CreateInputStream(keyBuffer, keyDecompressor);
						keyIn = new DataInputStream(keyInFilter);
						valLenDecompressor = CodecPool.GetDecompressor(codec);
						valLenInFilter = codec.CreateInputStream(valLenBuffer, valLenDecompressor);
						valLenIn = new DataInputStream(valLenInFilter);
					}
					SerializationFactory serializationFactory = new SerializationFactory(conf);
					this.keyDeserializer = GetDeserializer(serializationFactory, GetKeyClass());
					if (this.keyDeserializer == null)
					{
						throw new IOException("Could not find a deserializer for the Key class: '" + GetKeyClass
							().GetCanonicalName() + "'. " + "Please ensure that the configuration '" + CommonConfigurationKeys
							.IoSerializationsKey + "' is " + "properly configured, if you're using " + "custom serialization."
							);
					}
					if (!blockCompressed)
					{
						this.keyDeserializer.Open(valBuffer);
					}
					else
					{
						this.keyDeserializer.Open(keyIn);
					}
					this.valDeserializer = GetDeserializer(serializationFactory, GetValueClass());
					if (this.valDeserializer == null)
					{
						throw new IOException("Could not find a deserializer for the Value class: '" + GetValueClass
							().GetCanonicalName() + "'. " + "Please ensure that the configuration '" + CommonConfigurationKeys
							.IoSerializationsKey + "' is " + "properly configured, if you're using " + "custom serialization."
							);
					}
					this.valDeserializer.Open(valIn);
				}
			}

			private Deserializer GetDeserializer(SerializationFactory sf, Type c)
			{
				return sf.GetDeserializer(c);
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				lock (this)
				{
					// Return the decompressors to the pool
					CodecPool.ReturnDecompressor(keyLenDecompressor);
					CodecPool.ReturnDecompressor(keyDecompressor);
					CodecPool.ReturnDecompressor(valLenDecompressor);
					CodecPool.ReturnDecompressor(valDecompressor);
					keyLenDecompressor = keyDecompressor = null;
					valLenDecompressor = valDecompressor = null;
					if (keyDeserializer != null)
					{
						keyDeserializer.Close();
					}
					if (valDeserializer != null)
					{
						valDeserializer.Close();
					}
					// Close the input-stream
					@in.Close();
				}
			}

			/// <summary>Returns the name of the key class.</summary>
			public virtual string GetKeyClassName()
			{
				return keyClassName;
			}

			/// <summary>Returns the class of keys in this file.</summary>
			public virtual Type GetKeyClass()
			{
				lock (this)
				{
					if (null == keyClass)
					{
						try
						{
							keyClass = WritableName.GetClass(GetKeyClassName(), conf);
						}
						catch (IOException e)
						{
							throw new RuntimeException(e);
						}
					}
					return keyClass;
				}
			}

			/// <summary>Returns the name of the value class.</summary>
			public virtual string GetValueClassName()
			{
				return valClassName;
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual Type GetValueClass()
			{
				lock (this)
				{
					if (null == valClass)
					{
						try
						{
							valClass = WritableName.GetClass(GetValueClassName(), conf);
						}
						catch (IOException e)
						{
							throw new RuntimeException(e);
						}
					}
					return valClass;
				}
			}

			/// <summary>Returns true if values are compressed.</summary>
			public virtual bool IsCompressed()
			{
				return decompress;
			}

			/// <summary>Returns true if records are block-compressed.</summary>
			public virtual bool IsBlockCompressed()
			{
				return blockCompressed;
			}

			/// <summary>Returns the compression codec of data in this file.</summary>
			public virtual CompressionCodec GetCompressionCodec()
			{
				return codec;
			}

			private byte[] GetSync()
			{
				return sync;
			}

			private byte GetVersion()
			{
				return version;
			}

			/// <summary>Get the compression type for this file.</summary>
			/// <returns>the compression type</returns>
			public virtual SequenceFile.CompressionType GetCompressionType()
			{
				if (decompress)
				{
					return blockCompressed ? SequenceFile.CompressionType.Block : SequenceFile.CompressionType
						.Record;
				}
				else
				{
					return SequenceFile.CompressionType.None;
				}
			}

			/// <summary>Returns the metadata object of the file</summary>
			public virtual SequenceFile.Metadata GetMetadata()
			{
				return this.metadata;
			}

			/// <summary>Returns the configuration used for this file.</summary>
			internal virtual Configuration GetConf()
			{
				return conf;
			}

			/// <summary>Read a compressed buffer</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ReadBuffer(DataInputBuffer buffer, CompressionInputStream filter)
			{
				lock (this)
				{
					// Read data into a temporary buffer
					DataOutputBuffer dataBuffer = new DataOutputBuffer();
					try
					{
						int dataBufferLength = WritableUtils.ReadVInt(@in);
						dataBuffer.Write(@in, dataBufferLength);
						// Set up 'buffer' connected to the input-stream
						buffer.Reset(dataBuffer.GetData(), 0, dataBuffer.GetLength());
					}
					finally
					{
						dataBuffer.Close();
					}
					// Reset the codec
					filter.ResetState();
				}
			}

			/// <summary>Read the next 'compressed' block</summary>
			/// <exception cref="System.IO.IOException"/>
			private void ReadBlock()
			{
				lock (this)
				{
					// Check if we need to throw away a whole block of 
					// 'values' due to 'lazy decompression' 
					if (lazyDecompress && !valuesDecompressed)
					{
						@in.Seek(WritableUtils.ReadVInt(@in) + @in.GetPos());
						@in.Seek(WritableUtils.ReadVInt(@in) + @in.GetPos());
					}
					// Reset internal states
					noBufferedKeys = 0;
					noBufferedValues = 0;
					noBufferedRecords = 0;
					valuesDecompressed = false;
					//Process sync
					if (sync != null)
					{
						@in.ReadInt();
						@in.ReadFully(syncCheck);
						// read syncCheck
						if (!Arrays.Equals(sync, syncCheck))
						{
							// check it
							throw new IOException("File is corrupt!");
						}
					}
					syncSeen = true;
					// Read number of records in this block
					noBufferedRecords = WritableUtils.ReadVInt(@in);
					// Read key lengths and keys
					ReadBuffer(keyLenBuffer, keyLenInFilter);
					ReadBuffer(keyBuffer, keyInFilter);
					noBufferedKeys = noBufferedRecords;
					// Read value lengths and values
					if (!lazyDecompress)
					{
						ReadBuffer(valLenBuffer, valLenInFilter);
						ReadBuffer(valBuffer, valInFilter);
						noBufferedValues = noBufferedRecords;
						valuesDecompressed = true;
					}
				}
			}

			/// <summary>
			/// Position valLenIn/valIn to the 'value'
			/// corresponding to the 'current' key
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void SeekToCurrentValue()
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						if (decompress)
						{
							valInFilter.ResetState();
						}
						valBuffer.Reset();
					}
					else
					{
						// Check if this is the first value in the 'block' to be read
						if (lazyDecompress && !valuesDecompressed)
						{
							// Read the value lengths and values
							ReadBuffer(valLenBuffer, valLenInFilter);
							ReadBuffer(valBuffer, valInFilter);
							noBufferedValues = noBufferedRecords;
							valuesDecompressed = true;
						}
						// Calculate the no. of bytes to skip
						// Note: 'current' key has already been read!
						int skipValBytes = 0;
						int currentKey = noBufferedKeys + 1;
						for (int i = noBufferedValues; i > currentKey; --i)
						{
							skipValBytes += WritableUtils.ReadVInt(valLenIn);
							--noBufferedValues;
						}
						// Skip to the 'val' corresponding to 'current' key
						if (skipValBytes > 0)
						{
							if (valIn.SkipBytes(skipValBytes) != skipValBytes)
							{
								throw new IOException("Failed to seek to " + currentKey + "(th) value!");
							}
						}
					}
				}
			}

			/// <summary>Get the 'value' corresponding to the last read 'key'.</summary>
			/// <param name="val">: The 'value' to be read.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void GetCurrentValue(Writable val)
			{
				lock (this)
				{
					if (val is Configurable)
					{
						((Configurable)val).SetConf(this.conf);
					}
					// Position stream to 'current' value
					SeekToCurrentValue();
					if (!blockCompressed)
					{
						val.ReadFields(valIn);
						if (valIn.Read() > 0)
						{
							Log.Info("available bytes: " + valIn.Available());
							throw new IOException(val + " read " + (valBuffer.GetPosition() - keyLength) + " bytes, should read "
								 + (valBuffer.GetLength() - keyLength));
						}
					}
					else
					{
						// Get the value
						int valLength = WritableUtils.ReadVInt(valLenIn);
						val.ReadFields(valIn);
						// Read another compressed 'value'
						--noBufferedValues;
						// Sanity check
						if ((valLength < 0) && Log.IsDebugEnabled())
						{
							Log.Debug(val + " is a zero-length value");
						}
					}
				}
			}

			/// <summary>Get the 'value' corresponding to the last read 'key'.</summary>
			/// <param name="val">: The 'value' to be read.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual object GetCurrentValue(object val)
			{
				lock (this)
				{
					if (val is Configurable)
					{
						((Configurable)val).SetConf(this.conf);
					}
					// Position stream to 'current' value
					SeekToCurrentValue();
					if (!blockCompressed)
					{
						val = DeserializeValue(val);
						if (valIn.Read() > 0)
						{
							Log.Info("available bytes: " + valIn.Available());
							throw new IOException(val + " read " + (valBuffer.GetPosition() - keyLength) + " bytes, should read "
								 + (valBuffer.GetLength() - keyLength));
						}
					}
					else
					{
						// Get the value
						int valLength = WritableUtils.ReadVInt(valLenIn);
						val = DeserializeValue(val);
						// Read another compressed 'value'
						--noBufferedValues;
						// Sanity check
						if ((valLength < 0) && Log.IsDebugEnabled())
						{
							Log.Debug(val + " is a zero-length value");
						}
					}
					return val;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private object DeserializeValue(object val)
			{
				return valDeserializer.Deserialize(val);
			}

			/// <summary>
			/// Read the next key in the file into <code>key</code>, skipping its
			/// value.
			/// </summary>
			/// <remarks>
			/// Read the next key in the file into <code>key</code>, skipping its
			/// value.  True if another entry exists, and false at end of file.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(Writable key)
			{
				lock (this)
				{
					if (key.GetType() != GetKeyClass())
					{
						throw new IOException("wrong key class: " + key.GetType().FullName + " is not " +
							 keyClass);
					}
					if (!blockCompressed)
					{
						outBuf.Reset();
						keyLength = Next(outBuf);
						if (keyLength < 0)
						{
							return false;
						}
						valBuffer.Reset(outBuf.GetData(), outBuf.GetLength());
						key.ReadFields(valBuffer);
						valBuffer.Mark(0);
						if (valBuffer.GetPosition() != keyLength)
						{
							throw new IOException(key + " read " + valBuffer.GetPosition() + " bytes, should read "
								 + keyLength);
						}
					}
					else
					{
						//Reset syncSeen
						syncSeen = false;
						if (noBufferedKeys == 0)
						{
							try
							{
								ReadBlock();
							}
							catch (EOFException)
							{
								return false;
							}
						}
						int keyLength = WritableUtils.ReadVInt(keyLenIn);
						// Sanity check
						if (keyLength < 0)
						{
							return false;
						}
						//Read another compressed 'key'
						key.ReadFields(keyIn);
						--noBufferedKeys;
					}
					return true;
				}
			}

			/// <summary>
			/// Read the next key/value pair in the file into <code>key</code> and
			/// <code>val</code>.
			/// </summary>
			/// <remarks>
			/// Read the next key/value pair in the file into <code>key</code> and
			/// <code>val</code>.  Returns true if such a pair exists and false when at
			/// end of file
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(Writable key, Writable val)
			{
				lock (this)
				{
					if (val.GetType() != GetValueClass())
					{
						throw new IOException("wrong value class: " + val + " is not " + valClass);
					}
					bool more = Next(key);
					if (more)
					{
						GetCurrentValue(val);
					}
					return more;
				}
			}

			/// <summary>
			/// Read and return the next record length, potentially skipping over
			/// a sync block.
			/// </summary>
			/// <returns>the length of the next record or -1 if there is no next record</returns>
			/// <exception cref="System.IO.IOException"/>
			private int ReadRecordLength()
			{
				lock (this)
				{
					if (@in.GetPos() >= end)
					{
						return -1;
					}
					int length = @in.ReadInt();
					if (version > 1 && sync != null && length == SyncEscape)
					{
						// process a sync entry
						@in.ReadFully(syncCheck);
						// read syncCheck
						if (!Arrays.Equals(sync, syncCheck))
						{
							// check it
							throw new IOException("File is corrupt!");
						}
						syncSeen = true;
						if (@in.GetPos() >= end)
						{
							return -1;
						}
						length = @in.ReadInt();
					}
					else
					{
						// re-read length
						syncSeen = false;
					}
					return length;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Call NextRaw(DataOutputBuffer, ValueBytes) .")]
			internal virtual int Next(DataOutputBuffer buffer)
			{
				lock (this)
				{
					// Unsupported for block-compressed sequence files
					if (blockCompressed)
					{
						throw new IOException("Unsupported call for block-compressed" + " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)"
							);
					}
					try
					{
						int length = ReadRecordLength();
						if (length == -1)
						{
							return -1;
						}
						int keyLength = @in.ReadInt();
						buffer.Write(@in, length);
						return keyLength;
					}
					catch (ChecksumException e)
					{
						// checksum failure
						HandleChecksumException(e);
						return Next(buffer);
					}
				}
			}

			public virtual SequenceFile.ValueBytes CreateValueBytes()
			{
				SequenceFile.ValueBytes val = null;
				if (!decompress || blockCompressed)
				{
					val = new SequenceFile.UncompressedBytes();
				}
				else
				{
					val = new SequenceFile.CompressedBytes(codec);
				}
				return val;
			}

			/// <summary>Read 'raw' records.</summary>
			/// <param name="key">- The buffer into which the key is read</param>
			/// <param name="val">- The 'raw' value</param>
			/// <returns>Returns the total record length or -1 for end of file</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int NextRaw(DataOutputBuffer key, SequenceFile.ValueBytes val)
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						int length = ReadRecordLength();
						if (length == -1)
						{
							return -1;
						}
						int keyLength = @in.ReadInt();
						int valLength = length - keyLength;
						key.Write(@in, keyLength);
						if (decompress)
						{
							SequenceFile.CompressedBytes value = (SequenceFile.CompressedBytes)val;
							value.Reset(@in, valLength);
						}
						else
						{
							SequenceFile.UncompressedBytes value = (SequenceFile.UncompressedBytes)val;
							value.Reset(@in, valLength);
						}
						return length;
					}
					else
					{
						//Reset syncSeen
						syncSeen = false;
						// Read 'key'
						if (noBufferedKeys == 0)
						{
							if (@in.GetPos() >= end)
							{
								return -1;
							}
							try
							{
								ReadBlock();
							}
							catch (EOFException)
							{
								return -1;
							}
						}
						int keyLength = WritableUtils.ReadVInt(keyLenIn);
						if (keyLength < 0)
						{
							throw new IOException("zero length key found!");
						}
						key.Write(keyIn, keyLength);
						--noBufferedKeys;
						// Read raw 'value'
						SeekToCurrentValue();
						int valLength = WritableUtils.ReadVInt(valLenIn);
						SequenceFile.UncompressedBytes rawValue = (SequenceFile.UncompressedBytes)val;
						rawValue.Reset(valIn, valLength);
						--noBufferedValues;
						return (keyLength + valLength);
					}
				}
			}

			/// <summary>Read 'raw' keys.</summary>
			/// <param name="key">- The buffer into which the key is read</param>
			/// <returns>Returns the key length or -1 for end of file</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int NextRawKey(DataOutputBuffer key)
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						recordLength = ReadRecordLength();
						if (recordLength == -1)
						{
							return -1;
						}
						keyLength = @in.ReadInt();
						key.Write(@in, keyLength);
						return keyLength;
					}
					else
					{
						//Reset syncSeen
						syncSeen = false;
						// Read 'key'
						if (noBufferedKeys == 0)
						{
							if (@in.GetPos() >= end)
							{
								return -1;
							}
							try
							{
								ReadBlock();
							}
							catch (EOFException)
							{
								return -1;
							}
						}
						int keyLength = WritableUtils.ReadVInt(keyLenIn);
						if (keyLength < 0)
						{
							throw new IOException("zero length key found!");
						}
						key.Write(keyIn, keyLength);
						--noBufferedKeys;
						return keyLength;
					}
				}
			}

			/// <summary>
			/// Read the next key in the file, skipping its
			/// value.
			/// </summary>
			/// <remarks>
			/// Read the next key in the file, skipping its
			/// value.  Return null at end of file.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual object Next(object key)
			{
				lock (this)
				{
					if (key != null && key.GetType() != GetKeyClass())
					{
						throw new IOException("wrong key class: " + key.GetType().FullName + " is not " +
							 keyClass);
					}
					if (!blockCompressed)
					{
						outBuf.Reset();
						keyLength = Next(outBuf);
						if (keyLength < 0)
						{
							return null;
						}
						valBuffer.Reset(outBuf.GetData(), outBuf.GetLength());
						key = DeserializeKey(key);
						valBuffer.Mark(0);
						if (valBuffer.GetPosition() != keyLength)
						{
							throw new IOException(key + " read " + valBuffer.GetPosition() + " bytes, should read "
								 + keyLength);
						}
					}
					else
					{
						//Reset syncSeen
						syncSeen = false;
						if (noBufferedKeys == 0)
						{
							try
							{
								ReadBlock();
							}
							catch (EOFException)
							{
								return null;
							}
						}
						int keyLength = WritableUtils.ReadVInt(keyLenIn);
						// Sanity check
						if (keyLength < 0)
						{
							return null;
						}
						//Read another compressed 'key'
						key = DeserializeKey(key);
						--noBufferedKeys;
					}
					return key;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private object DeserializeKey(object key)
			{
				return keyDeserializer.Deserialize(key);
			}

			/// <summary>Read 'raw' values.</summary>
			/// <param name="val">- The 'raw' value</param>
			/// <returns>Returns the value length</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int NextRawValue(SequenceFile.ValueBytes val)
			{
				lock (this)
				{
					// Position stream to current value
					SeekToCurrentValue();
					if (!blockCompressed)
					{
						int valLength = recordLength - keyLength;
						if (decompress)
						{
							SequenceFile.CompressedBytes value = (SequenceFile.CompressedBytes)val;
							value.Reset(@in, valLength);
						}
						else
						{
							SequenceFile.UncompressedBytes value = (SequenceFile.UncompressedBytes)val;
							value.Reset(@in, valLength);
						}
						return valLength;
					}
					else
					{
						int valLength = WritableUtils.ReadVInt(valLenIn);
						SequenceFile.UncompressedBytes rawValue = (SequenceFile.UncompressedBytes)val;
						rawValue.Reset(valIn, valLength);
						--noBufferedValues;
						return valLength;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void HandleChecksumException(ChecksumException e)
			{
				if (this.conf.GetBoolean("io.skip.checksum.errors", false))
				{
					Log.Warn("Bad checksum at " + GetPosition() + ". Skipping entries.");
					Sync(GetPosition() + this.conf.GetInt("io.bytes.per.checksum", 512));
				}
				else
				{
					throw e;
				}
			}

			/// <summary>disables sync.</summary>
			/// <remarks>disables sync. often invoked for tmp files</remarks>
			internal virtual void IgnoreSync()
			{
				lock (this)
				{
					sync = null;
				}
			}

			/// <summary>Set the current byte position in the input file.</summary>
			/// <remarks>
			/// Set the current byte position in the input file.
			/// <p>The position passed must be a position returned by
			/// <see cref="Writer.GetLength()"/>
			/// when writing this file.  To seek to an arbitrary
			/// position, use
			/// <see cref="Sync(long)"/>
			/// .
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Seek(long position)
			{
				lock (this)
				{
					@in.Seek(position);
					if (blockCompressed)
					{
						// trigger block read
						noBufferedKeys = 0;
						valuesDecompressed = true;
					}
				}
			}

			/// <summary>Seek to the next sync mark past a given position.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Sync(long position)
			{
				lock (this)
				{
					if (position + SyncSize >= end)
					{
						Seek(end);
						return;
					}
					if (position < headerEnd)
					{
						// seek directly to first record
						@in.Seek(headerEnd);
						// note the sync marker "seen" in the header
						syncSeen = true;
						return;
					}
					try
					{
						Seek(position + 4);
						// skip escape
						@in.ReadFully(syncCheck);
						int syncLen = sync.Length;
						for (int i = 0; @in.GetPos() < end; i++)
						{
							int j = 0;
							for (; j < syncLen; j++)
							{
								if (sync[j] != syncCheck[(i + j) % syncLen])
								{
									break;
								}
							}
							if (j == syncLen)
							{
								@in.Seek(@in.GetPos() - SyncSize);
								// position before sync
								return;
							}
							syncCheck[i % syncLen] = @in.ReadByte();
						}
					}
					catch (ChecksumException e)
					{
						// checksum failure
						HandleChecksumException(e);
					}
				}
			}

			/// <summary>Returns true iff the previous call to next passed a sync mark.</summary>
			public virtual bool SyncSeen()
			{
				lock (this)
				{
					return syncSeen;
				}
			}

			/// <summary>Return the current byte position in the input file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPosition()
			{
				lock (this)
				{
					return @in.GetPos();
				}
			}

			/// <summary>Returns the name of the file.</summary>
			public override string ToString()
			{
				return filename;
			}
		}

		/// <summary>Sorts key/value pairs in a sequence-format file.</summary>
		/// <remarks>
		/// Sorts key/value pairs in a sequence-format file.
		/// <p>For best performance, applications should make sure that the
		/// <see cref="Writable.ReadFields(System.IO.DataInput)"/>
		/// implementation of their keys is
		/// very efficient.  In particular, it should avoid allocating memory.
		/// </remarks>
		public class Sorter
		{
			private RawComparator comparator;

			private MergeSort mergeSort;

			private Path[] inFiles;

			private Path outFile;

			private int memory;

			private int factor;

			private FileSystem fs = null;

			private Type keyClass;

			private Type valClass;

			private Configuration conf;

			private SequenceFile.Metadata metadata;

			private Progressable progressable = null;

			/// <summary>Sort and merge files containing the named classes.</summary>
			public Sorter(FileSystem fs, Type keyClass, Type valClass, Configuration conf)
				: this(fs, WritableComparator.Get(keyClass, conf), keyClass, valClass, conf)
			{
			}

			/// <summary>
			/// Sort and merge using an arbitrary
			/// <see cref="RawComparator{T}"/>
			/// .
			/// </summary>
			public Sorter(FileSystem fs, RawComparator comparator, Type keyClass, Type valClass
				, Configuration conf)
				: this(fs, comparator, keyClass, valClass, conf, new SequenceFile.Metadata())
			{
			}

			/// <summary>
			/// Sort and merge using an arbitrary
			/// <see cref="RawComparator{T}"/>
			/// .
			/// </summary>
			public Sorter(FileSystem fs, RawComparator comparator, Type keyClass, Type valClass
				, Configuration conf, SequenceFile.Metadata metadata)
			{
				//the implementation of merge sort
				// when merging or sorting
				// bytes
				// merged per pass
				this.fs = fs;
				this.comparator = comparator;
				this.keyClass = keyClass;
				this.valClass = valClass;
				this.memory = conf.GetInt("io.sort.mb", 100) * 1024 * 1024;
				this.factor = conf.GetInt("io.sort.factor", 100);
				this.conf = conf;
				this.metadata = metadata;
			}

			/// <summary>Set the number of streams to merge at once.</summary>
			public virtual void SetFactor(int factor)
			{
				this.factor = factor;
			}

			/// <summary>Get the number of streams to merge at once.</summary>
			public virtual int GetFactor()
			{
				return factor;
			}

			/// <summary>Set the total amount of buffer memory, in bytes.</summary>
			public virtual void SetMemory(int memory)
			{
				this.memory = memory;
			}

			/// <summary>Get the total amount of buffer memory, in bytes.</summary>
			public virtual int GetMemory()
			{
				return memory;
			}

			/// <summary>Set the progressable object in order to report progress.</summary>
			public virtual void SetProgressable(Progressable progressable)
			{
				this.progressable = progressable;
			}

			/// <summary>Perform a file sort from a set of input files into an output file.</summary>
			/// <param name="inFiles">the files to be sorted</param>
			/// <param name="outFile">the sorted output file</param>
			/// <param name="deleteInput">should the input files be deleted as they are read?</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Sort(Path[] inFiles, Path outFile, bool deleteInput)
			{
				if (fs.Exists(outFile))
				{
					throw new IOException("already exists: " + outFile);
				}
				this.inFiles = inFiles;
				this.outFile = outFile;
				int segments = SortPass(deleteInput);
				if (segments > 1)
				{
					MergePass(outFile.GetParent());
				}
			}

			/// <summary>Perform a file sort from a set of input files and return an iterator.</summary>
			/// <param name="inFiles">the files to be sorted</param>
			/// <param name="tempDir">the directory where temp files are created during sort</param>
			/// <param name="deleteInput">should the input files be deleted as they are read?</param>
			/// <returns>iterator the RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Sorter.RawKeyValueIterator SortAndIterate(Path[] inFiles
				, Path tempDir, bool deleteInput)
			{
				Path outFile = new Path(tempDir + Path.Separator + "all.2");
				if (fs.Exists(outFile))
				{
					throw new IOException("already exists: " + outFile);
				}
				this.inFiles = inFiles;
				//outFile will basically be used as prefix for temp files in the cases
				//where sort outputs multiple sorted segments. For the single segment
				//case, the outputFile itself will contain the sorted data for that
				//segment
				this.outFile = outFile;
				int segments = SortPass(deleteInput);
				if (segments > 1)
				{
					return Merge(outFile.Suffix(".0"), outFile.Suffix(".0.index"), tempDir);
				}
				else
				{
					if (segments == 1)
					{
						return Merge(new Path[] { outFile }, true, tempDir);
					}
					else
					{
						return null;
					}
				}
			}

			/// <summary>The backwards compatible interface to sort.</summary>
			/// <param name="inFile">the input file to sort</param>
			/// <param name="outFile">the sorted output file</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Sort(Path inFile, Path outFile)
			{
				Sort(new Path[] { inFile }, outFile, false);
			}

			/// <exception cref="System.IO.IOException"/>
			private int SortPass(bool deleteInput)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("running sort pass");
				}
				SequenceFile.Sorter.SortPass sortPass = new SequenceFile.Sorter.SortPass(this);
				// make the SortPass
				sortPass.SetProgressable(progressable);
				mergeSort = new MergeSort(new SequenceFile.Sorter.SortPass.SeqFileComparator(this
					));
				try
				{
					return sortPass.Run(deleteInput);
				}
				finally
				{
					// run it
					sortPass.Close();
				}
			}

			private class SortPass
			{
				private int memoryLimit = this._enclosing.memory / 4;

				private int recordLimit = 1000000;

				private DataOutputBuffer rawKeys = new DataOutputBuffer();

				private byte[] rawBuffer;

				private int[] keyOffsets = new int[1024];

				private int[] pointers = new int[this.keyOffsets.Length];

				private int[] pointersCopy = new int[this.keyOffsets.Length];

				private int[] keyLengths = new int[this.keyOffsets.Length];

				private SequenceFile.ValueBytes[] rawValues = new SequenceFile.ValueBytes[this.keyOffsets
					.Length];

				private ArrayList segmentLengths = new ArrayList();

				private SequenceFile.Reader @in = null;

				private FSDataOutputStream @out = null;

				private FSDataOutputStream indexOut = null;

				private Path outName;

				private Progressable progressable = null;

				// close it
				/// <exception cref="System.IO.IOException"/>
				public virtual int Run(bool deleteInput)
				{
					int segments = 0;
					int currentFile = 0;
					bool atEof = (currentFile >= this._enclosing.inFiles.Length);
					SequenceFile.CompressionType compressionType;
					CompressionCodec codec = null;
					this.segmentLengths.Clear();
					if (atEof)
					{
						return 0;
					}
					// Initialize
					this.@in = new SequenceFile.Reader(this._enclosing.fs, this._enclosing.inFiles[currentFile
						], this._enclosing.conf);
					compressionType = this.@in.GetCompressionType();
					codec = this.@in.GetCompressionCodec();
					for (int i = 0; i < this.rawValues.Length; ++i)
					{
						this.rawValues[i] = null;
					}
					while (!atEof)
					{
						int count = 0;
						int bytesProcessed = 0;
						this.rawKeys.Reset();
						while (!atEof && bytesProcessed < this.memoryLimit && count < this.recordLimit)
						{
							// Read a record into buffer
							// Note: Attempt to re-use 'rawValue' as far as possible
							int keyOffset = this.rawKeys.GetLength();
							SequenceFile.ValueBytes rawValue = (count == this.keyOffsets.Length || this.rawValues
								[count] == null) ? this.@in.CreateValueBytes() : this.rawValues[count];
							int recordLength = this.@in.NextRaw(this.rawKeys, rawValue);
							if (recordLength == -1)
							{
								this.@in.Close();
								if (deleteInput)
								{
									this._enclosing.fs.Delete(this._enclosing.inFiles[currentFile], true);
								}
								currentFile += 1;
								atEof = currentFile >= this._enclosing.inFiles.Length;
								if (!atEof)
								{
									this.@in = new SequenceFile.Reader(this._enclosing.fs, this._enclosing.inFiles[currentFile
										], this._enclosing.conf);
								}
								else
								{
									this.@in = null;
								}
								continue;
							}
							int keyLength = this.rawKeys.GetLength() - keyOffset;
							if (count == this.keyOffsets.Length)
							{
								this.Grow();
							}
							this.keyOffsets[count] = keyOffset;
							// update pointers
							this.pointers[count] = count;
							this.keyLengths[count] = keyLength;
							this.rawValues[count] = rawValue;
							bytesProcessed += recordLength;
							count++;
						}
						// buffer is full -- sort & flush it
						if (SequenceFile.Log.IsDebugEnabled())
						{
							SequenceFile.Log.Debug("flushing segment " + segments);
						}
						this.rawBuffer = this.rawKeys.GetData();
						this.Sort(count);
						// indicate we're making progress
						if (this.progressable != null)
						{
							this.progressable.Progress();
						}
						this.Flush(count, bytesProcessed, compressionType, codec, segments == 0 && atEof);
						segments++;
					}
					return segments;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Close()
				{
					if (this.@in != null)
					{
						this.@in.Close();
					}
					if (this.@out != null)
					{
						this.@out.Close();
					}
					if (this.indexOut != null)
					{
						this.indexOut.Close();
					}
				}

				private void Grow()
				{
					int newLength = this.keyOffsets.Length * 3 / 2;
					this.keyOffsets = this.Grow(this.keyOffsets, newLength);
					this.pointers = this.Grow(this.pointers, newLength);
					this.pointersCopy = new int[newLength];
					this.keyLengths = this.Grow(this.keyLengths, newLength);
					this.rawValues = this.Grow(this.rawValues, newLength);
				}

				private int[] Grow(int[] old, int newLength)
				{
					int[] result = new int[newLength];
					System.Array.Copy(old, 0, result, 0, old.Length);
					return result;
				}

				private SequenceFile.ValueBytes[] Grow(SequenceFile.ValueBytes[] old, int newLength
					)
				{
					SequenceFile.ValueBytes[] result = new SequenceFile.ValueBytes[newLength];
					System.Array.Copy(old, 0, result, 0, old.Length);
					for (int i = old.Length; i < newLength; ++i)
					{
						result[i] = null;
					}
					return result;
				}

				/// <exception cref="System.IO.IOException"/>
				private void Flush(int count, int bytesProcessed, SequenceFile.CompressionType compressionType
					, CompressionCodec codec, bool done)
				{
					if (this.@out == null)
					{
						this.outName = done ? this._enclosing.outFile : this._enclosing.outFile.Suffix(".0"
							);
						this.@out = this._enclosing.fs.Create(this.outName);
						if (!done)
						{
							this.indexOut = this._enclosing.fs.Create(this.outName.Suffix(".index"));
						}
					}
					long segmentStart = this.@out.GetPos();
					SequenceFile.Writer writer = SequenceFile.CreateWriter(this._enclosing.conf, SequenceFile.Writer
						.Stream(this.@out), SequenceFile.Writer.KeyClass(this._enclosing.keyClass), SequenceFile.Writer
						.ValueClass(this._enclosing.valClass), SequenceFile.Writer.Compression(compressionType
						, codec), SequenceFile.Writer.Metadata(done ? this._enclosing.metadata : new SequenceFile.Metadata
						()));
					if (!done)
					{
						writer.sync = null;
					}
					// disable sync on temp files
					for (int i = 0; i < count; i++)
					{
						// write in sorted order
						int p = this.pointers[i];
						writer.AppendRaw(this.rawBuffer, this.keyOffsets[p], this.keyLengths[p], this.rawValues
							[p]);
					}
					writer.Close();
					if (!done)
					{
						// Save the segment length
						WritableUtils.WriteVLong(this.indexOut, segmentStart);
						WritableUtils.WriteVLong(this.indexOut, (this.@out.GetPos() - segmentStart));
						this.indexOut.Flush();
					}
				}

				private void Sort(int count)
				{
					System.Array.Copy(this.pointers, 0, this.pointersCopy, 0, count);
					this._enclosing.mergeSort.MergeSort(this.pointersCopy, this.pointers, 0, count);
				}

				internal class SeqFileComparator : IComparer<IntWritable>
				{
					public virtual int Compare(IntWritable I, IntWritable J)
					{
						return this._enclosing._enclosing.comparator.Compare(this._enclosing.rawBuffer, this
							._enclosing.keyOffsets[I.Get()], this._enclosing.keyLengths[I.Get()], this._enclosing
							.rawBuffer, this._enclosing.keyOffsets[J.Get()], this._enclosing.keyLengths[J.Get
							()]);
					}

					internal SeqFileComparator(SortPass _enclosing)
					{
						this._enclosing = _enclosing;
					}

					private readonly SortPass _enclosing;
				}

				/// <summary>set the progressable object in order to report progress</summary>
				public virtual void SetProgressable(Progressable progressable)
				{
					this.progressable = progressable;
				}

				internal SortPass(Sorter _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly Sorter _enclosing;
			}

			/// <summary>The interface to iterate over raw keys/values of SequenceFiles.</summary>
			public interface RawKeyValueIterator
			{
				// SequenceFile.Sorter.SortPass
				/// <summary>Gets the current raw key</summary>
				/// <returns>DataOutputBuffer</returns>
				/// <exception cref="System.IO.IOException"/>
				DataOutputBuffer GetKey();

				/// <summary>Gets the current raw value</summary>
				/// <returns>ValueBytes</returns>
				/// <exception cref="System.IO.IOException"/>
				SequenceFile.ValueBytes GetValue();

				/// <summary>Sets up the current key and value (for getKey and getValue)</summary>
				/// <returns>true if there exists a key/value, false otherwise</returns>
				/// <exception cref="System.IO.IOException"/>
				bool Next();

				/// <summary>closes the iterator so that the underlying streams can be closed</summary>
				/// <exception cref="System.IO.IOException"/>
				void Close();

				/// <summary>
				/// Gets the Progress object; this has a float (0.0 - 1.0)
				/// indicating the bytes processed by the iterator so far
				/// </summary>
				Progress GetProgress();
			}

			/// <summary>Merges the list of segments of type <code>SegmentDescriptor</code></summary>
			/// <param name="segments">the list of SegmentDescriptors</param>
			/// <param name="tmpDir">the directory to write temporary files into</param>
			/// <returns>RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Sorter.RawKeyValueIterator Merge(IList<SequenceFile.Sorter.SegmentDescriptor
				> segments, Path tmpDir)
			{
				// pass in object to report progress, if present
				SequenceFile.Sorter.MergeQueue mQueue = new SequenceFile.Sorter.MergeQueue(this, 
					segments, tmpDir, progressable);
				return mQueue.Merge();
			}

			/// <summary>
			/// Merges the contents of files passed in Path[] using a max factor value
			/// that is already set
			/// </summary>
			/// <param name="inNames">the array of path names</param>
			/// <param name="deleteInputs">
			/// true if the input files should be deleted when
			/// unnecessary
			/// </param>
			/// <param name="tmpDir">the directory to write temporary files into</param>
			/// <returns>RawKeyValueIteratorMergeQueue</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Sorter.RawKeyValueIterator Merge(Path[] inNames, bool
				 deleteInputs, Path tmpDir)
			{
				return Merge(inNames, deleteInputs, (inNames.Length < factor) ? inNames.Length : 
					factor, tmpDir);
			}

			/// <summary>Merges the contents of files passed in Path[]</summary>
			/// <param name="inNames">the array of path names</param>
			/// <param name="deleteInputs">
			/// true if the input files should be deleted when
			/// unnecessary
			/// </param>
			/// <param name="factor">the factor that will be used as the maximum merge fan-in</param>
			/// <param name="tmpDir">the directory to write temporary files into</param>
			/// <returns>RawKeyValueIteratorMergeQueue</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Sorter.RawKeyValueIterator Merge(Path[] inNames, bool
				 deleteInputs, int factor, Path tmpDir)
			{
				//get the segments from inNames
				AList<SequenceFile.Sorter.SegmentDescriptor> a = new AList<SequenceFile.Sorter.SegmentDescriptor
					>();
				for (int i = 0; i < inNames.Length; i++)
				{
					SequenceFile.Sorter.SegmentDescriptor s = new SequenceFile.Sorter.SegmentDescriptor
						(this, 0, fs.GetFileStatus(inNames[i]).GetLen(), inNames[i]);
					s.PreserveInput(!deleteInputs);
					s.DoSync();
					a.AddItem(s);
				}
				this.factor = factor;
				SequenceFile.Sorter.MergeQueue mQueue = new SequenceFile.Sorter.MergeQueue(this, 
					a, tmpDir, progressable);
				return mQueue.Merge();
			}

			/// <summary>Merges the contents of files passed in Path[]</summary>
			/// <param name="inNames">the array of path names</param>
			/// <param name="tempDir">the directory for creating temp files during merge</param>
			/// <param name="deleteInputs">
			/// true if the input files should be deleted when
			/// unnecessary
			/// </param>
			/// <returns>RawKeyValueIteratorMergeQueue</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Sorter.RawKeyValueIterator Merge(Path[] inNames, Path
				 tempDir, bool deleteInputs)
			{
				//outFile will basically be used as prefix for temp files for the
				//intermediate merge outputs           
				this.outFile = new Path(tempDir + Path.Separator + "merged");
				//get the segments from inNames
				AList<SequenceFile.Sorter.SegmentDescriptor> a = new AList<SequenceFile.Sorter.SegmentDescriptor
					>();
				for (int i = 0; i < inNames.Length; i++)
				{
					SequenceFile.Sorter.SegmentDescriptor s = new SequenceFile.Sorter.SegmentDescriptor
						(this, 0, fs.GetFileStatus(inNames[i]).GetLen(), inNames[i]);
					s.PreserveInput(!deleteInputs);
					s.DoSync();
					a.AddItem(s);
				}
				factor = (inNames.Length < factor) ? inNames.Length : factor;
				// pass in object to report progress, if present
				SequenceFile.Sorter.MergeQueue mQueue = new SequenceFile.Sorter.MergeQueue(this, 
					a, tempDir, progressable);
				return mQueue.Merge();
			}

			/// <summary>
			/// Clones the attributes (like compression of the input file and creates a
			/// corresponding Writer
			/// </summary>
			/// <param name="inputFile">
			/// the path of the input file whose attributes should be
			/// cloned
			/// </param>
			/// <param name="outputFile">the path of the output file</param>
			/// <param name="prog">the Progressable to report status during the file write</param>
			/// <returns>Writer</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual SequenceFile.Writer CloneFileAttributes(Path inputFile, Path outputFile
				, Progressable prog)
			{
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.File
					(inputFile), new SequenceFile.Reader.OnlyHeaderOption());
				SequenceFile.CompressionType compress = reader.GetCompressionType();
				CompressionCodec codec = reader.GetCompressionCodec();
				reader.Close();
				SequenceFile.Writer writer = CreateWriter(conf, SequenceFile.Writer.File(outputFile
					), SequenceFile.Writer.KeyClass(keyClass), SequenceFile.Writer.ValueClass(valClass
					), SequenceFile.Writer.Compression(compress, codec), SequenceFile.Writer.Progressable
					(prog));
				return writer;
			}

			/// <summary>
			/// Writes records from RawKeyValueIterator into a file represented by the
			/// passed writer
			/// </summary>
			/// <param name="records">the RawKeyValueIterator</param>
			/// <param name="writer">the Writer created earlier</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteFile(SequenceFile.Sorter.RawKeyValueIterator records, SequenceFile.Writer
				 writer)
			{
				while (records.Next())
				{
					writer.AppendRaw(records.GetKey().GetData(), 0, records.GetKey().GetLength(), records
						.GetValue());
				}
				writer.Sync();
			}

			/// <summary>Merge the provided files.</summary>
			/// <param name="inFiles">the array of input path names</param>
			/// <param name="outFile">the final output file</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Merge(Path[] inFiles, Path outFile)
			{
				if (fs.Exists(outFile))
				{
					throw new IOException("already exists: " + outFile);
				}
				SequenceFile.Sorter.RawKeyValueIterator r = Merge(inFiles, false, outFile.GetParent
					());
				SequenceFile.Writer writer = CloneFileAttributes(inFiles[0], outFile, null);
				WriteFile(r, writer);
				writer.Close();
			}

			/// <summary>sort calls this to generate the final merged output</summary>
			/// <exception cref="System.IO.IOException"/>
			private int MergePass(Path tmpDir)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("running merge pass");
				}
				SequenceFile.Writer writer = CloneFileAttributes(outFile.Suffix(".0"), outFile, null
					);
				SequenceFile.Sorter.RawKeyValueIterator r = Merge(outFile.Suffix(".0"), outFile.Suffix
					(".0.index"), tmpDir);
				WriteFile(r, writer);
				writer.Close();
				return 0;
			}

			/// <summary>Used by mergePass to merge the output of the sort</summary>
			/// <param name="inName">the name of the input file containing sorted segments</param>
			/// <param name="indexIn">the offsets of the sorted segments</param>
			/// <param name="tmpDir">the relative directory to store intermediate results in</param>
			/// <returns>RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			private SequenceFile.Sorter.RawKeyValueIterator Merge(Path inName, Path indexIn, 
				Path tmpDir)
			{
				//get the segments from indexIn
				//we create a SegmentContainer so that we can track segments belonging to
				//inName and delete inName as soon as we see that we have looked at all
				//the contained segments during the merge process & hence don't need 
				//them anymore
				SequenceFile.Sorter.SegmentContainer container = new SequenceFile.Sorter.SegmentContainer
					(this, inName, indexIn);
				SequenceFile.Sorter.MergeQueue mQueue = new SequenceFile.Sorter.MergeQueue(this, 
					container.GetSegmentList(), tmpDir, progressable);
				return mQueue.Merge();
			}

			/// <summary>This class implements the core of the merge logic</summary>
			private class MergeQueue : PriorityQueue, SequenceFile.Sorter.RawKeyValueIterator
			{
				private bool compress;

				private bool blockCompress;

				private DataOutputBuffer rawKey = new DataOutputBuffer();

				private SequenceFile.ValueBytes rawValue;

				private long totalBytesProcessed;

				private float progPerByte;

				private Progress mergeProgress = new Progress();

				private Path tmpDir;

				private Progressable progress = null;

				private SequenceFile.Sorter.SegmentDescriptor minSegment;

				private IDictionary<SequenceFile.Sorter.SegmentDescriptor, Void> sortedSegmentSizes
					 = new SortedDictionary<SequenceFile.Sorter.SegmentDescriptor, Void>();

				//handle to the progress reporting object
				//a TreeMap used to store the segments sorted by size (segment offset and
				//segment path name is used to break ties between segments of same sizes)
				/// <exception cref="System.IO.IOException"/>
				public virtual void Put(SequenceFile.Sorter.SegmentDescriptor stream)
				{
					if (this.Size() == 0)
					{
						this.compress = stream.@in.IsCompressed();
						this.blockCompress = stream.@in.IsBlockCompressed();
					}
					else
					{
						if (this.compress != stream.@in.IsCompressed() || this.blockCompress != stream.@in
							.IsBlockCompressed())
						{
							throw new IOException("All merged files must be compressed or not.");
						}
					}
					base.Put(stream);
				}

				/// <summary>A queue of file segments to merge</summary>
				/// <param name="segments">the file segments to merge</param>
				/// <param name="tmpDir">a relative local directory to save intermediate files in</param>
				/// <param name="progress">the reference to the Progressable object</param>
				public MergeQueue(Sorter _enclosing, IList<SequenceFile.Sorter.SegmentDescriptor>
					 segments, Path tmpDir, Progressable progress)
				{
					this._enclosing = _enclosing;
					int size = segments.Count;
					for (int i = 0; i < size; i++)
					{
						this.sortedSegmentSizes[segments[i]] = null;
					}
					this.tmpDir = tmpDir;
					this.progress = progress;
				}

				protected internal override bool LessThan(object a, object b)
				{
					// indicate we're making progress
					if (this.progress != null)
					{
						this.progress.Progress();
					}
					SequenceFile.Sorter.SegmentDescriptor msa = (SequenceFile.Sorter.SegmentDescriptor
						)a;
					SequenceFile.Sorter.SegmentDescriptor msb = (SequenceFile.Sorter.SegmentDescriptor
						)b;
					return this._enclosing.comparator.Compare(msa.GetKey().GetData(), 0, msa.GetKey()
						.GetLength(), msb.GetKey().GetData(), 0, msb.GetKey().GetLength()) < 0;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Close()
				{
					SequenceFile.Sorter.SegmentDescriptor ms;
					// close inputs
					while ((ms = (SequenceFile.Sorter.SegmentDescriptor)this.Pop()) != null)
					{
						ms.Cleanup();
					}
					this.minSegment = null;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual DataOutputBuffer GetKey()
				{
					return this.rawKey;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual SequenceFile.ValueBytes GetValue()
				{
					return this.rawValue;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual bool Next()
				{
					if (this.Size() == 0)
					{
						return false;
					}
					if (this.minSegment != null)
					{
						//minSegment is non-null for all invocations of next except the first
						//one. For the first invocation, the priority queue is ready for use
						//but for the subsequent invocations, first adjust the queue 
						this.AdjustPriorityQueue(this.minSegment);
						if (this.Size() == 0)
						{
							this.minSegment = null;
							return false;
						}
					}
					this.minSegment = (SequenceFile.Sorter.SegmentDescriptor)this.Top();
					long startPos = this.minSegment.@in.GetPosition();
					// Current position in stream
					//save the raw key reference
					this.rawKey = this.minSegment.GetKey();
					//load the raw value. Re-use the existing rawValue buffer
					if (this.rawValue == null)
					{
						this.rawValue = this.minSegment.@in.CreateValueBytes();
					}
					this.minSegment.NextRawValue(this.rawValue);
					long endPos = this.minSegment.@in.GetPosition();
					// End position after reading value
					this.UpdateProgress(endPos - startPos);
					return true;
				}

				public virtual Progress GetProgress()
				{
					return this.mergeProgress;
				}

				/// <exception cref="System.IO.IOException"/>
				private void AdjustPriorityQueue(SequenceFile.Sorter.SegmentDescriptor ms)
				{
					long startPos = ms.@in.GetPosition();
					// Current position in stream
					bool hasNext = ms.NextRawKey();
					long endPos = ms.@in.GetPosition();
					// End position after reading key
					this.UpdateProgress(endPos - startPos);
					if (hasNext)
					{
						this.AdjustTop();
					}
					else
					{
						this.Pop();
						ms.Cleanup();
					}
				}

				private void UpdateProgress(long bytesProcessed)
				{
					this.totalBytesProcessed += bytesProcessed;
					if (this.progPerByte > 0)
					{
						this.mergeProgress.Set(this.totalBytesProcessed * this.progPerByte);
					}
				}

				/// <summary>
				/// This is the single level merge that is called multiple times
				/// depending on the factor size and the number of segments
				/// </summary>
				/// <returns>RawKeyValueIterator</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual SequenceFile.Sorter.RawKeyValueIterator Merge()
				{
					//create the MergeStreams from the sorted map created in the constructor
					//and dump the final output to a file
					int numSegments = this.sortedSegmentSizes.Count;
					int origFactor = this._enclosing.factor;
					int passNo = 1;
					LocalDirAllocator lDirAlloc = new LocalDirAllocator("io.seqfile.local.dir");
					do
					{
						//get the factor for this pass of merge
						this._enclosing.factor = this.GetPassFactor(passNo, numSegments);
						IList<SequenceFile.Sorter.SegmentDescriptor> segmentsToMerge = new AList<SequenceFile.Sorter.SegmentDescriptor
							>();
						int segmentsConsidered = 0;
						int numSegmentsToConsider = this._enclosing.factor;
						while (true)
						{
							//extract the smallest 'factor' number of segment pointers from the 
							//TreeMap. Call cleanup on the empty segments (no key/value data)
							SequenceFile.Sorter.SegmentDescriptor[] mStream = this.GetSegmentDescriptors(numSegmentsToConsider
								);
							for (int i = 0; i < mStream.Length; i++)
							{
								if (mStream[i].NextRawKey())
								{
									segmentsToMerge.AddItem(mStream[i]);
									segmentsConsidered++;
									// Count the fact that we read some bytes in calling nextRawKey()
									this.UpdateProgress(mStream[i].@in.GetPosition());
								}
								else
								{
									mStream[i].Cleanup();
									numSegments--;
								}
							}
							//we ignore this segment for the merge
							//if we have the desired number of segments
							//or looked at all available segments, we break
							if (segmentsConsidered == this._enclosing.factor || this.sortedSegmentSizes.Count
								 == 0)
							{
								break;
							}
							numSegmentsToConsider = this._enclosing.factor - segmentsConsidered;
						}
						//feed the streams to the priority queue
						this.Initialize(segmentsToMerge.Count);
						this.Clear();
						for (int i_1 = 0; i_1 < segmentsToMerge.Count; i_1++)
						{
							this.Put(segmentsToMerge[i_1]);
						}
						//if we have lesser number of segments remaining, then just return the
						//iterator, else do another single level merge
						if (numSegments <= this._enclosing.factor)
						{
							//calculate the length of the remaining segments. Required for 
							//calculating the merge progress
							long totalBytes = 0;
							for (int i = 0; i_1 < segmentsToMerge.Count; i_1++)
							{
								totalBytes += segmentsToMerge[i_1].segmentLength;
							}
							if (totalBytes != 0)
							{
								//being paranoid
								this.progPerByte = 1.0f / (float)totalBytes;
							}
							//reset factor to what it originally was
							this._enclosing.factor = origFactor;
							return this;
						}
						else
						{
							//we want to spread the creation of temp files on multiple disks if 
							//available under the space constraints
							long approxOutputSize = 0;
							foreach (SequenceFile.Sorter.SegmentDescriptor s in segmentsToMerge)
							{
								approxOutputSize += s.segmentLength + ChecksumFileSystem.GetApproxChkSumLength(s.
									segmentLength);
							}
							Path tmpFilename = new Path(this.tmpDir, "intermediate").Suffix("." + passNo);
							Path outputFile = lDirAlloc.GetLocalPathForWrite(tmpFilename.ToString(), approxOutputSize
								, this._enclosing.conf);
							if (SequenceFile.Log.IsDebugEnabled())
							{
								SequenceFile.Log.Debug("writing intermediate results to " + outputFile);
							}
							SequenceFile.Writer writer = this._enclosing.CloneFileAttributes(this._enclosing.
								fs.MakeQualified(segmentsToMerge[0].segmentPathName), this._enclosing.fs.MakeQualified
								(outputFile), null);
							writer.sync = null;
							//disable sync for temp files
							this._enclosing.WriteFile(this, writer);
							writer.Close();
							//we finished one single level merge; now clean up the priority 
							//queue
							this.Close();
							SequenceFile.Sorter.SegmentDescriptor tempSegment = new SequenceFile.Sorter.SegmentDescriptor
								(this, 0, this._enclosing.fs.GetFileStatus(outputFile).GetLen(), outputFile);
							//put the segment back in the TreeMap
							this.sortedSegmentSizes[tempSegment] = null;
							numSegments = this.sortedSegmentSizes.Count;
							passNo++;
						}
						//we are worried about only the first pass merge factor. So reset the 
						//factor to what it originally was
						this._enclosing.factor = origFactor;
					}
					while (true);
				}

				//Hadoop-591
				public virtual int GetPassFactor(int passNo, int numSegments)
				{
					if (passNo > 1 || numSegments <= this._enclosing.factor || this._enclosing.factor
						 == 1)
					{
						return this._enclosing.factor;
					}
					int mod = (numSegments - 1) % (this._enclosing.factor - 1);
					if (mod == 0)
					{
						return this._enclosing.factor;
					}
					return mod + 1;
				}

				/// <summary>
				/// Return (& remove) the requested number of segment descriptors from the
				/// sorted map.
				/// </summary>
				public virtual SequenceFile.Sorter.SegmentDescriptor[] GetSegmentDescriptors(int 
					numDescriptors)
				{
					if (numDescriptors > this.sortedSegmentSizes.Count)
					{
						numDescriptors = this.sortedSegmentSizes.Count;
					}
					SequenceFile.Sorter.SegmentDescriptor[] SegmentDescriptors = new SequenceFile.Sorter.SegmentDescriptor
						[numDescriptors];
					IEnumerator iter = this.sortedSegmentSizes.Keys.GetEnumerator();
					int i = 0;
					while (i < numDescriptors)
					{
						SegmentDescriptors[i++] = (SequenceFile.Sorter.SegmentDescriptor)iter.Next();
						iter.Remove();
					}
					return SegmentDescriptors;
				}

				private readonly Sorter _enclosing;
			}

			/// <summary>This class defines a merge segment.</summary>
			/// <remarks>
			/// This class defines a merge segment. This class can be subclassed to
			/// provide a customized cleanup method implementation. In this
			/// implementation, cleanup closes the file handle and deletes the file
			/// </remarks>
			public class SegmentDescriptor : IComparable
			{
				internal long segmentOffset;

				internal long segmentLength;

				internal Path segmentPathName;

				internal bool ignoreSync = true;

				private SequenceFile.Reader @in = null;

				private DataOutputBuffer rawKey = null;

				private bool preserveInput = false;

				/// <summary>Constructs a segment</summary>
				/// <param name="segmentOffset">the offset of the segment in the file</param>
				/// <param name="segmentLength">the length of the segment</param>
				/// <param name="segmentPathName">the path name of the file containing the segment</param>
				public SegmentDescriptor(Sorter _enclosing, long segmentOffset, long segmentLength
					, Path segmentPathName)
				{
					this._enclosing = _enclosing;
					// SequenceFile.Sorter.MergeQueue
					//the start of the segment in the file
					//the length of the segment
					//the path name of the file containing the segment
					//set to true for temp files
					//this will hold the current key
					//delete input segment files?
					this.segmentOffset = segmentOffset;
					this.segmentLength = segmentLength;
					this.segmentPathName = segmentPathName;
				}

				/// <summary>Do the sync checks</summary>
				public virtual void DoSync()
				{
					this.ignoreSync = false;
				}

				/// <summary>Whether to delete the files when no longer needed</summary>
				public virtual void PreserveInput(bool preserve)
				{
					this.preserveInput = preserve;
				}

				public virtual bool ShouldPreserveInput()
				{
					return this.preserveInput;
				}

				public virtual int CompareTo(object o)
				{
					SequenceFile.Sorter.SegmentDescriptor that = (SequenceFile.Sorter.SegmentDescriptor
						)o;
					if (this.segmentLength != that.segmentLength)
					{
						return (this.segmentLength < that.segmentLength ? -1 : 1);
					}
					if (this.segmentOffset != that.segmentOffset)
					{
						return (this.segmentOffset < that.segmentOffset ? -1 : 1);
					}
					return string.CompareOrdinal((this.segmentPathName.ToString()), that.segmentPathName
						.ToString());
				}

				public override bool Equals(object o)
				{
					if (!(o is SequenceFile.Sorter.SegmentDescriptor))
					{
						return false;
					}
					SequenceFile.Sorter.SegmentDescriptor that = (SequenceFile.Sorter.SegmentDescriptor
						)o;
					if (this.segmentLength == that.segmentLength && this.segmentOffset == that.segmentOffset
						 && this.segmentPathName.ToString().Equals(that.segmentPathName.ToString()))
					{
						return true;
					}
					return false;
				}

				public override int GetHashCode()
				{
					return 37 * 17 + (int)(this.segmentOffset ^ ((long)(((ulong)this.segmentOffset) >>
						 32)));
				}

				/// <summary>Fills up the rawKey object with the key returned by the Reader</summary>
				/// <returns>true if there is a key returned; false, otherwise</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual bool NextRawKey()
				{
					if (this.@in == null)
					{
						int bufferSize = SequenceFile.GetBufferSize(this._enclosing.conf);
						SequenceFile.Reader reader = new SequenceFile.Reader(this._enclosing.conf, SequenceFile.Reader
							.File(this.segmentPathName), SequenceFile.Reader.BufferSize(bufferSize), SequenceFile.Reader
							.Start(this.segmentOffset), SequenceFile.Reader.Length(this.segmentLength));
						//sometimes we ignore syncs especially for temp merge files
						if (this.ignoreSync)
						{
							reader.IgnoreSync();
						}
						if (reader.GetKeyClass() != this._enclosing.keyClass)
						{
							throw new IOException("wrong key class: " + reader.GetKeyClass() + " is not " + this
								._enclosing.keyClass);
						}
						if (reader.GetValueClass() != this._enclosing.valClass)
						{
							throw new IOException("wrong value class: " + reader.GetValueClass() + " is not "
								 + this._enclosing.valClass);
						}
						this.@in = reader;
						this.rawKey = new DataOutputBuffer();
					}
					this.rawKey.Reset();
					int keyLength = this.@in.NextRawKey(this.rawKey);
					return (keyLength >= 0);
				}

				/// <summary>
				/// Fills up the passed rawValue with the value corresponding to the key
				/// read earlier
				/// </summary>
				/// <param name="rawValue"/>
				/// <returns>the length of the value</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual int NextRawValue(SequenceFile.ValueBytes rawValue)
				{
					int valLength = this.@in.NextRawValue(rawValue);
					return valLength;
				}

				/// <summary>Returns the stored rawKey</summary>
				public virtual DataOutputBuffer GetKey()
				{
					return this.rawKey;
				}

				/// <summary>closes the underlying reader</summary>
				/// <exception cref="System.IO.IOException"/>
				private void Close()
				{
					this.@in.Close();
					this.@in = null;
				}

				/// <summary>The default cleanup.</summary>
				/// <remarks>
				/// The default cleanup. Subclasses can override this with a custom
				/// cleanup
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void Cleanup()
				{
					this.Close();
					if (!this.preserveInput)
					{
						this._enclosing.fs.Delete(this.segmentPathName, true);
					}
				}

				private readonly Sorter _enclosing;
			}

			/// <summary>
			/// This class provisions multiple segments contained within a single
			/// file
			/// </summary>
			private class LinkedSegmentsDescriptor : SequenceFile.Sorter.SegmentDescriptor
			{
				internal SequenceFile.Sorter.SegmentContainer parentContainer = null;

				/// <summary>Constructs a segment</summary>
				/// <param name="segmentOffset">the offset of the segment in the file</param>
				/// <param name="segmentLength">the length of the segment</param>
				/// <param name="segmentPathName">the path name of the file containing the segment</param>
				/// <param name="parent">the parent SegmentContainer that holds the segment</param>
				public LinkedSegmentsDescriptor(Sorter _enclosing, long segmentOffset, long segmentLength
					, Path segmentPathName, SequenceFile.Sorter.SegmentContainer parent)
					: base(_enclosing)
				{
					this._enclosing = _enclosing;
					// SequenceFile.Sorter.SegmentDescriptor
					this.parentContainer = parent;
				}

				/// <summary>The default cleanup.</summary>
				/// <remarks>
				/// The default cleanup. Subclasses can override this with a custom
				/// cleanup
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public override void Cleanup()
				{
					base.Close();
					if (base.ShouldPreserveInput())
					{
						return;
					}
					this.parentContainer.Cleanup();
				}

				public override bool Equals(object o)
				{
					if (!(o is SequenceFile.Sorter.LinkedSegmentsDescriptor))
					{
						return false;
					}
					return base.Equals(o);
				}

				private readonly Sorter _enclosing;
			}

			/// <summary>The class that defines a container for segments to be merged.</summary>
			/// <remarks>
			/// The class that defines a container for segments to be merged. Primarily
			/// required to delete temp files as soon as all the contained segments
			/// have been looked at
			/// </remarks>
			private class SegmentContainer
			{
				private int numSegmentsCleanedUp = 0;

				private int numSegmentsContained;

				private Path inName;

				private AList<SequenceFile.Sorter.SegmentDescriptor> segments = new AList<SequenceFile.Sorter.SegmentDescriptor
					>();

				/// <summary>
				/// This constructor is there primarily to serve the sort routine that
				/// generates a single output file with an associated index file
				/// </summary>
				/// <exception cref="System.IO.IOException"/>
				public SegmentContainer(Sorter _enclosing, Path inName, Path indexIn)
				{
					this._enclosing = _enclosing;
					//SequenceFile.Sorter.LinkedSegmentsDescriptor
					//track the no. of segment cleanups
					//# of segments contained
					//input file from where segments are created
					//the list of segments read from the file
					//get the segments from indexIn
					FSDataInputStream fsIndexIn = this._enclosing.fs.Open(indexIn);
					long end = this._enclosing.fs.GetFileStatus(indexIn).GetLen();
					while (fsIndexIn.GetPos() < end)
					{
						long segmentOffset = WritableUtils.ReadVLong(fsIndexIn);
						long segmentLength = WritableUtils.ReadVLong(fsIndexIn);
						Path segmentName = inName;
						this.segments.AddItem(new SequenceFile.Sorter.LinkedSegmentsDescriptor(this, segmentOffset
							, segmentLength, segmentName, this));
					}
					fsIndexIn.Close();
					this._enclosing.fs.Delete(indexIn, true);
					this.numSegmentsContained = this.segments.Count;
					this.inName = inName;
				}

				public virtual IList<SequenceFile.Sorter.SegmentDescriptor> GetSegmentList()
				{
					return this.segments;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Cleanup()
				{
					this.numSegmentsCleanedUp++;
					if (this.numSegmentsCleanedUp == this.numSegmentsContained)
					{
						this._enclosing.fs.Delete(this.inName, true);
					}
				}

				private readonly Sorter _enclosing;
			}
			//SequenceFile.Sorter.SegmentContainer
		}
		// SequenceFile.Sorter
	}
}
