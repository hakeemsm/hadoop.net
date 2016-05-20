using Sharpen;

namespace org.apache.hadoop.io
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
	/// <see cref="org.apache.hadoop.io.compress.CompressionCodec"/>
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
	/// <seealso cref="org.apache.hadoop.io.compress.CompressionCodec"/>
	public class SequenceFile
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.SequenceFile
			)));

		private SequenceFile()
		{
		}

		private const byte BLOCK_COMPRESS_VERSION = unchecked((byte)4);

		private const byte CUSTOM_COMPRESS_VERSION = unchecked((byte)5);

		private const byte VERSION_WITH_METADATA = unchecked((byte)6);

		private static byte[] VERSION = new byte[] { unchecked((byte)(byte)('S')), unchecked(
			(byte)(byte)('E')), unchecked((byte)(byte)('Q')), VERSION_WITH_METADATA };

		private const int SYNC_ESCAPE = -1;

		private const int SYNC_HASH_SIZE = 16;

		private const int SYNC_SIZE = 4 + SYNC_HASH_SIZE;

		/// <summary>The number of bytes between sync points.</summary>
		public const int SYNC_INTERVAL = 100 * SYNC_SIZE;

		/// <summary>
		/// The compression type used to compress key/value pairs in the
		/// <see cref="SequenceFile"/>
		/// .
		/// </summary>
		/// <seealso cref="Writer"/>
		public enum CompressionType
		{
			NONE,
			RECORD,
			BLOCK
		}

		// no public ctor
		// "length" of sync entries
		// number of bytes in hash 
		// escape + hash
		/// <summary>Get the compression type for the reduce outputs</summary>
		/// <param name="job">the job config to look in</param>
		/// <returns>the kind of compression to use</returns>
		public static org.apache.hadoop.io.SequenceFile.CompressionType getDefaultCompressionType
			(org.apache.hadoop.conf.Configuration job)
		{
			string name = job.get("io.seqfile.compression.type");
			return name == null ? org.apache.hadoop.io.SequenceFile.CompressionType.RECORD : 
				org.apache.hadoop.io.SequenceFile.CompressionType.valueOf(name);
		}

		/// <summary>Set the default compression type for sequence files.</summary>
		/// <param name="job">the configuration to modify</param>
		/// <param name="val">the new compression type (none, block, record)</param>
		public static void setDefaultCompressionType(org.apache.hadoop.conf.Configuration
			 job, org.apache.hadoop.io.SequenceFile.CompressionType val)
		{
			job.set("io.seqfile.compression.type", val.ToString());
		}

		/// <summary>Create a new Writer with the given options.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <param name="opts">the options to create the file with</param>
		/// <returns>a new Writer</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.conf.Configuration
			 conf, params org.apache.hadoop.io.SequenceFile.Writer.Option[] opts)
		{
			org.apache.hadoop.io.SequenceFile.Writer.CompressionOption compressionOption = org.apache.hadoop.util.Options
				.getOption<org.apache.hadoop.io.SequenceFile.Writer.CompressionOption>(opts);
			org.apache.hadoop.io.SequenceFile.CompressionType kind;
			if (compressionOption != null)
			{
				kind = compressionOption.getValue();
			}
			else
			{
				kind = getDefaultCompressionType(conf);
				opts = org.apache.hadoop.util.Options.prependOptions(opts, org.apache.hadoop.io.SequenceFile.Writer
					.compression(kind));
			}
			switch (kind)
			{
				case org.apache.hadoop.io.SequenceFile.CompressionType.NONE:
				default:
				{
					return new org.apache.hadoop.io.SequenceFile.Writer(conf, opts);
				}

				case org.apache.hadoop.io.SequenceFile.CompressionType.RECORD:
				{
					return new org.apache.hadoop.io.SequenceFile.RecordCompressWriter(conf, opts);
				}

				case org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK:
				{
					return new org.apache.hadoop.io.SequenceFile.BlockCompressWriter(conf, opts);
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.filesystem(fs)
				, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer.valueClass(valClass
				));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.filesystem(fs)
				, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer.valueClass(valClass
				), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.util.Progressable progress)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.filesystem(fs), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				), org.apache.hadoop.io.SequenceFile.Writer.progressable(progress));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.filesystem(fs), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.io.SequenceFile.Metadata metadata)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.filesystem(fs), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec), org.apache.hadoop.io.SequenceFile.Writer.progressable(progress), org.apache.hadoop.io.SequenceFile.Writer
				.metadata(metadata));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, int bufferSize, short replication
			, long blockSize, org.apache.hadoop.io.SequenceFile.CompressionType compressionType
			, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.io.SequenceFile.Metadata metadata)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.filesystem(fs), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.bufferSize(bufferSize
				), org.apache.hadoop.io.SequenceFile.Writer.replication(replication), org.apache.hadoop.io.SequenceFile.Writer
				.blockSize(blockSize), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec), org.apache.hadoop.io.SequenceFile.Writer.progressable(progress), org.apache.hadoop.io.SequenceFile.Writer
				.metadata(metadata));
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
		[System.Obsolete]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, int bufferSize, short replication
			, long blockSize, bool createParent, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.io.SequenceFile.Metadata
			 metadata)
		{
			return createWriter(org.apache.hadoop.fs.FileContext.getFileContext(fs.getUri(), 
				conf), conf, name, keyClass, valClass, compressionType, codec, metadata, java.util.EnumSet
				.of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.OVERWRITE
				), org.apache.hadoop.fs.Options.CreateOpts.bufferSize(bufferSize), createParent ? 
				org.apache.hadoop.fs.Options.CreateOpts.createParent() : org.apache.hadoop.fs.Options.CreateOpts
				.donotCreateParent(), org.apache.hadoop.fs.Options.CreateOpts.repFac(replication
				), org.apache.hadoop.fs.Options.CreateOpts.blockSize(blockSize));
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
		/// <see cref="org.apache.hadoop.fs.Options.CreateOpts"/>
		/// .
		/// </param>
		/// <returns>Returns the handle to the constructed SequenceFile Writer.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileContext
			 fc, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.io.SequenceFile.Metadata
			 metadata, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, params 
			org.apache.hadoop.fs.Options.CreateOpts[] opts)
		{
			return createWriter(conf, fc.create(name, createFlag, opts), keyClass, valClass, 
				compressionType, codec, metadata).ownStream();
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.fs.FileSystem
			 fs, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.Path name, 
			java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.SequenceFile.CompressionType
			 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, org.apache.hadoop.util.Progressable
			 progress)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.file(name), org.apache.hadoop.io.SequenceFile.Writer
				.filesystem(fs), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec), org.apache.hadoop.io.SequenceFile.Writer.progressable(progress));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FSDataOutputStream @out, java.lang.Class keyClass, java.lang.Class
			 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compressionType, org.apache.hadoop.io.compress.CompressionCodec
			 codec, org.apache.hadoop.io.SequenceFile.Metadata metadata)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.stream(@out), 
				org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec), org.apache.hadoop.io.SequenceFile.Writer.metadata(metadata));
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
		[System.ObsoleteAttribute(@"Use createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
			)]
		public static org.apache.hadoop.io.SequenceFile.Writer createWriter(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.fs.FSDataOutputStream @out, java.lang.Class keyClass, java.lang.Class
			 valClass, org.apache.hadoop.io.SequenceFile.CompressionType compressionType, org.apache.hadoop.io.compress.CompressionCodec
			 codec)
		{
			return createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer.stream(@out), 
				org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), org.apache.hadoop.io.SequenceFile.Writer
				.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType
				, codec));
		}

		/// <summary>The interface to 'raw' values of SequenceFiles.</summary>
		public interface ValueBytes
		{
			/// <summary>Writes the uncompressed bytes to the outStream.</summary>
			/// <param name="outStream">: Stream to write uncompressed bytes into.</param>
			/// <exception cref="System.IO.IOException"/>
			void writeUncompressedBytes(java.io.DataOutputStream outStream);

			/// <summary>Write compressed bytes to outStream.</summary>
			/// <remarks>
			/// Write compressed bytes to outStream.
			/// Note: that it will NOT compress the bytes if they are not compressed.
			/// </remarks>
			/// <param name="outStream">: Stream to write compressed bytes into.</param>
			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			void writeCompressedBytes(java.io.DataOutputStream outStream);

			/// <summary>Size of stored data.</summary>
			int getSize();
		}

		private class UncompressedBytes : org.apache.hadoop.io.SequenceFile.ValueBytes
		{
			private int dataSize;

			private byte[] data;

			private UncompressedBytes()
			{
				data = null;
				dataSize = 0;
			}

			/// <exception cref="System.IO.IOException"/>
			private void reset(java.io.DataInputStream @in, int length)
			{
				if (data == null)
				{
					data = new byte[length];
				}
				else
				{
					if (length > data.Length)
					{
						data = new byte[System.Math.max(length, data.Length * 2)];
					}
				}
				dataSize = -1;
				@in.readFully(data, 0, length);
				dataSize = length;
			}

			public virtual int getSize()
			{
				return dataSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void writeUncompressedBytes(java.io.DataOutputStream outStream)
			{
				outStream.write(data, 0, dataSize);
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void writeCompressedBytes(java.io.DataOutputStream outStream)
			{
				throw new System.ArgumentException("UncompressedBytes cannot be compressed!");
			}
		}

		private class CompressedBytes : org.apache.hadoop.io.SequenceFile.ValueBytes
		{
			private int dataSize;

			private byte[] data;

			internal org.apache.hadoop.io.DataInputBuffer rawData = null;

			internal org.apache.hadoop.io.compress.CompressionCodec codec = null;

			internal org.apache.hadoop.io.compress.CompressionInputStream decompressedStream = 
				null;

			private CompressedBytes(org.apache.hadoop.io.compress.CompressionCodec codec)
			{
				// UncompressedBytes
				data = null;
				dataSize = 0;
				this.codec = codec;
			}

			/// <exception cref="System.IO.IOException"/>
			private void reset(java.io.DataInputStream @in, int length)
			{
				if (data == null)
				{
					data = new byte[length];
				}
				else
				{
					if (length > data.Length)
					{
						data = new byte[System.Math.max(length, data.Length * 2)];
					}
				}
				dataSize = -1;
				@in.readFully(data, 0, length);
				dataSize = length;
			}

			public virtual int getSize()
			{
				return dataSize;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void writeUncompressedBytes(java.io.DataOutputStream outStream)
			{
				if (decompressedStream == null)
				{
					rawData = new org.apache.hadoop.io.DataInputBuffer();
					decompressedStream = codec.createInputStream(rawData);
				}
				else
				{
					decompressedStream.resetState();
				}
				rawData.reset(data, 0, dataSize);
				byte[] buffer = new byte[8192];
				int bytesRead = 0;
				while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1)
				{
					outStream.write(buffer, 0, bytesRead);
				}
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void writeCompressedBytes(java.io.DataOutputStream outStream)
			{
				outStream.write(data, 0, dataSize);
			}
		}

		/// <summary>The class encapsulating with the metadata of a file.</summary>
		/// <remarks>
		/// The class encapsulating with the metadata of a file.
		/// The metadata of a file is a list of attribute name/value
		/// pairs of Text type.
		/// </remarks>
		public class Metadata : org.apache.hadoop.io.Writable
		{
			private System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text
				> theMetadata;

			public Metadata()
				: this(new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text, 
					org.apache.hadoop.io.Text>())
			{
			}

			public Metadata(System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text
				, org.apache.hadoop.io.Text> arg)
			{
				// CompressedBytes
				if (arg == null)
				{
					this.theMetadata = new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text
						, org.apache.hadoop.io.Text>();
				}
				else
				{
					this.theMetadata = arg;
				}
			}

			public virtual org.apache.hadoop.io.Text get(org.apache.hadoop.io.Text name)
			{
				return this.theMetadata[name];
			}

			public virtual void set(org.apache.hadoop.io.Text name, org.apache.hadoop.io.Text
				 value)
			{
				this.theMetadata[name] = value;
			}

			public virtual System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text
				, org.apache.hadoop.io.Text> getMetadata()
			{
				return new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text, 
					org.apache.hadoop.io.Text>(this.theMetadata);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void write(java.io.DataOutput @out)
			{
				@out.writeInt(this.theMetadata.Count);
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text
					, org.apache.hadoop.io.Text>> iter = this.theMetadata.GetEnumerator();
				while (iter.MoveNext())
				{
					System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text
						> en = iter.Current;
					en.Key.write(@out);
					en.Value.write(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void readFields(java.io.DataInput @in)
			{
				int sz = @in.readInt();
				if (sz < 0)
				{
					throw new System.IO.IOException("Invalid size: " + sz + " for file metadata object"
						);
				}
				this.theMetadata = new System.Collections.Generic.SortedDictionary<org.apache.hadoop.io.Text
					, org.apache.hadoop.io.Text>();
				for (int i = 0; i < sz; i++)
				{
					org.apache.hadoop.io.Text key = new org.apache.hadoop.io.Text();
					org.apache.hadoop.io.Text val = new org.apache.hadoop.io.Text();
					key.readFields(@in);
					val.readFields(@in);
					this.theMetadata[key] = val;
				}
			}

			public override bool Equals(object other)
			{
				if (other == null)
				{
					return false;
				}
				if (Sharpen.Runtime.getClassForObject(other) != Sharpen.Runtime.getClassForObject
					(this))
				{
					return false;
				}
				else
				{
					return equals((org.apache.hadoop.io.SequenceFile.Metadata)other);
				}
			}

			public virtual bool equals(org.apache.hadoop.io.SequenceFile.Metadata other)
			{
				if (other == null)
				{
					return false;
				}
				if (this.theMetadata.Count != other.theMetadata.Count)
				{
					return false;
				}
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text
					, org.apache.hadoop.io.Text>> iter1 = this.theMetadata.GetEnumerator();
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text
					, org.apache.hadoop.io.Text>> iter2 = other.theMetadata.GetEnumerator();
				while (iter1.MoveNext() && iter2.MoveNext())
				{
					System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text
						> en1 = iter1.Current;
					System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text
						> en2 = iter2.Current;
					if (!en1.Key.Equals(en2.Key))
					{
						return false;
					}
					if (!en1.Value.Equals(en2.Value))
					{
						return false;
					}
				}
				if (iter1.MoveNext() || iter2.MoveNext())
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
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append("size: ").Append(this.theMetadata.Count).Append("\n");
				System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text
					, org.apache.hadoop.io.Text>> iter = this.theMetadata.GetEnumerator();
				while (iter.MoveNext())
				{
					System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.io.Text
						> en = iter.Current;
					sb.Append("\t").Append(en.Key.ToString()).Append("\t").Append(en.Value.ToString()
						);
					sb.Append("\n");
				}
				return sb.ToString();
			}
		}

		/// <summary>Write key/value pairs to a sequence-format file.</summary>
		public class Writer : java.io.Closeable, org.apache.hadoop.fs.Syncable
		{
			private org.apache.hadoop.conf.Configuration conf;

			internal org.apache.hadoop.fs.FSDataOutputStream @out;

			internal bool ownOutputStream = true;

			internal org.apache.hadoop.io.DataOutputBuffer buffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			internal java.lang.Class keyClass;

			internal java.lang.Class valClass;

			private readonly org.apache.hadoop.io.SequenceFile.CompressionType compress;

			internal org.apache.hadoop.io.compress.CompressionCodec codec = null;

			internal org.apache.hadoop.io.compress.CompressionOutputStream deflateFilter = null;

			internal java.io.DataOutputStream deflateOut = null;

			internal org.apache.hadoop.io.SequenceFile.Metadata metadata = null;

			internal org.apache.hadoop.io.compress.Compressor compressor = null;

			private bool appendMode = false;

			protected internal org.apache.hadoop.io.serializer.Serializer keySerializer;

			protected internal org.apache.hadoop.io.serializer.Serializer uncompressedValSerializer;

			protected internal org.apache.hadoop.io.serializer.Serializer compressedValSerializer;

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

			internal class FileOption : org.apache.hadoop.util.Options.PathOption, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal FileOption(org.apache.hadoop.fs.Path path)
					: base(path)
				{
				}
			}

			[System.ObsoleteAttribute(@"only used for backwards-compatibility in the createWriter methods that take FileSystem."
				)]
			private class FileSystemOption : org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				private readonly org.apache.hadoop.fs.FileSystem value;

				protected internal FileSystemOption(org.apache.hadoop.fs.FileSystem value)
				{
					this.value = value;
				}

				public virtual org.apache.hadoop.fs.FileSystem getValue()
				{
					return value;
				}
			}

			internal class StreamOption : org.apache.hadoop.util.Options.FSDataOutputStreamOption
				, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal StreamOption(org.apache.hadoop.fs.FSDataOutputStream stream)
					: base(stream)
				{
				}
			}

			internal class BufferSizeOption : org.apache.hadoop.util.Options.IntegerOption, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal BufferSizeOption(int value)
					: base(value)
				{
				}
			}

			internal class BlockSizeOption : org.apache.hadoop.util.Options.LongOption, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal BlockSizeOption(long value)
					: base(value)
				{
				}
			}

			internal class ReplicationOption : org.apache.hadoop.util.Options.IntegerOption, 
				org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal ReplicationOption(int value)
					: base(value)
				{
				}
			}

			internal class AppendIfExistsOption : org.apache.hadoop.util.Options.BooleanOption
				, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal AppendIfExistsOption(bool value)
					: base(value)
				{
				}
			}

			internal class KeyClassOption : org.apache.hadoop.util.Options.ClassOption, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal KeyClassOption(java.lang.Class value)
					: base(value)
				{
				}
			}

			internal class ValueClassOption : org.apache.hadoop.util.Options.ClassOption, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal ValueClassOption(java.lang.Class value)
					: base(value)
				{
				}
			}

			internal class MetadataOption : org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				private readonly org.apache.hadoop.io.SequenceFile.Metadata value;

				internal MetadataOption(org.apache.hadoop.io.SequenceFile.Metadata value)
				{
					this.value = value;
				}

				internal virtual org.apache.hadoop.io.SequenceFile.Metadata getValue()
				{
					return value;
				}
			}

			internal class ProgressableOption : org.apache.hadoop.util.Options.ProgressableOption
				, org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				internal ProgressableOption(org.apache.hadoop.util.Progressable value)
					: base(value)
				{
				}
			}

			private class CompressionOption : org.apache.hadoop.io.SequenceFile.Writer.Option
			{
				private readonly org.apache.hadoop.io.SequenceFile.CompressionType value;

				private readonly org.apache.hadoop.io.compress.CompressionCodec codec;

				internal CompressionOption(org.apache.hadoop.io.SequenceFile.CompressionType value
					)
					: this(value, null)
				{
				}

				internal CompressionOption(org.apache.hadoop.io.SequenceFile.CompressionType value
					, org.apache.hadoop.io.compress.CompressionCodec codec)
				{
					this.value = value;
					this.codec = (org.apache.hadoop.io.SequenceFile.CompressionType.NONE != value && 
						null == codec) ? new org.apache.hadoop.io.compress.DefaultCodec() : codec;
				}

				internal virtual org.apache.hadoop.io.SequenceFile.CompressionType getValue()
				{
					return value;
				}

				internal virtual org.apache.hadoop.io.compress.CompressionCodec getCodec()
				{
					return codec;
				}
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option file(org.apache.hadoop.fs.Path
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.FileOption(value);
			}

			[System.ObsoleteAttribute(@"only used for backwards-compatibility in the createWriter methods that take FileSystem."
				)]
			private static org.apache.hadoop.io.SequenceFile.Writer.Option filesystem(org.apache.hadoop.fs.FileSystem
				 fs)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.FileSystemOption(fs);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option bufferSize(int value
				)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.BufferSizeOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option stream(org.apache.hadoop.fs.FSDataOutputStream
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.StreamOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option replication(short value
				)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.ReplicationOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option appendIfExists(bool
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.AppendIfExistsOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option blockSize(long value
				)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.BlockSizeOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option progressable(org.apache.hadoop.util.Progressable
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.ProgressableOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option keyClass(java.lang.Class
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.KeyClassOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option valueClass(java.lang.Class
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.ValueClassOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option metadata(org.apache.hadoop.io.SequenceFile.Metadata
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.MetadataOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option compression(org.apache.hadoop.io.SequenceFile.CompressionType
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.CompressionOption(value);
			}

			public static org.apache.hadoop.io.SequenceFile.Writer.Option compression(org.apache.hadoop.io.SequenceFile.CompressionType
				 value, org.apache.hadoop.io.compress.CompressionCodec codec)
			{
				return new org.apache.hadoop.io.SequenceFile.Writer.CompressionOption(value, codec
					);
			}

			/// <summary>Construct a uncompressed writer from a set of options.</summary>
			/// <param name="conf">the configuration to use</param>
			/// <param name="options">the options used when creating the writer</param>
			/// <exception cref="System.IO.IOException">if it fails</exception>
			internal Writer(org.apache.hadoop.conf.Configuration conf, params org.apache.hadoop.io.SequenceFile.Writer.Option
				[] opts)
			{
				{
					try
					{
						java.security.MessageDigest digester = java.security.MessageDigest.getInstance("MD5"
							);
						long time = org.apache.hadoop.util.Time.now();
						digester.update(Sharpen.Runtime.getBytesForString((new java.rmi.server.UID() + "@"
							 + time), org.apache.commons.io.Charsets.UTF_8));
						sync = digester.digest();
					}
					catch (System.Exception e)
					{
						throw new System.Exception(e);
					}
				}
				org.apache.hadoop.io.SequenceFile.Writer.BlockSizeOption blockSizeOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.BlockSizeOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.BufferSizeOption bufferSizeOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.BufferSizeOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.ReplicationOption replicationOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.ReplicationOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.ProgressableOption progressOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.ProgressableOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.FileOption fileOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.FileOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.AppendIfExistsOption appendIfExistsOption
					 = org.apache.hadoop.util.Options.getOption<org.apache.hadoop.io.SequenceFile.Writer.AppendIfExistsOption
					>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.FileSystemOption fsOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.FileSystemOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.StreamOption streamOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.StreamOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.KeyClassOption keyClassOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.KeyClassOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.ValueClassOption valueClassOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.ValueClassOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.MetadataOption metadataOption = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Writer.MetadataOption>(opts);
				org.apache.hadoop.io.SequenceFile.Writer.CompressionOption compressionTypeOption = 
					org.apache.hadoop.util.Options.getOption<org.apache.hadoop.io.SequenceFile.Writer.CompressionOption
					>(opts);
				// check consistency of options
				if ((fileOption == null) == (streamOption == null))
				{
					throw new System.ArgumentException("file or stream must be specified");
				}
				if (fileOption == null && (blockSizeOption != null || bufferSizeOption != null ||
					 replicationOption != null || progressOption != null))
				{
					throw new System.ArgumentException("file modifier options not " + "compatible with stream"
						);
				}
				org.apache.hadoop.fs.FSDataOutputStream @out;
				bool ownStream = fileOption != null;
				if (ownStream)
				{
					org.apache.hadoop.fs.Path p = fileOption.getValue();
					org.apache.hadoop.fs.FileSystem fs;
					if (fsOption != null)
					{
						fs = fsOption.getValue();
					}
					else
					{
						fs = p.getFileSystem(conf);
					}
					int bufferSize = bufferSizeOption == null ? getBufferSize(conf) : bufferSizeOption
						.getValue();
					short replication = replicationOption == null ? fs.getDefaultReplication(p) : (short
						)replicationOption.getValue();
					long blockSize = blockSizeOption == null ? fs.getDefaultBlockSize(p) : blockSizeOption
						.getValue();
					org.apache.hadoop.util.Progressable progress = progressOption == null ? null : progressOption
						.getValue();
					if (appendIfExistsOption != null && appendIfExistsOption.getValue() && fs.exists(
						p))
					{
						// Read the file and verify header details
						org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
							(conf, org.apache.hadoop.io.SequenceFile.Reader.file(p), new org.apache.hadoop.io.SequenceFile.Reader.OnlyHeaderOption
							());
						try
						{
							if (keyClassOption.getValue() != reader.getKeyClass() || valueClassOption.getValue
								() != reader.getValueClass())
							{
								throw new System.ArgumentException("Key/value class provided does not match the file"
									);
							}
							if (reader.getVersion() != VERSION[3])
							{
								throw new org.apache.hadoop.io.VersionMismatchException(VERSION[3], reader.getVersion
									());
							}
							if (metadataOption != null)
							{
								LOG.info("MetaData Option is ignored during append");
							}
							metadataOption = (org.apache.hadoop.io.SequenceFile.Writer.MetadataOption)org.apache.hadoop.io.SequenceFile.Writer
								.metadata(reader.getMetadata());
							org.apache.hadoop.io.SequenceFile.Writer.CompressionOption readerCompressionOption
								 = new org.apache.hadoop.io.SequenceFile.Writer.CompressionOption(reader.getCompressionType
								(), reader.getCompressionCodec());
							if (readerCompressionOption.value != compressionTypeOption.value || !Sharpen.Runtime.getClassForObject
								(readerCompressionOption.codec).getName().Equals(Sharpen.Runtime.getClassForObject
								(compressionTypeOption.codec).getName()))
							{
								throw new System.ArgumentException("Compression option provided does not match the file"
									);
							}
							sync = reader.getSync();
						}
						finally
						{
							reader.close();
						}
						@out = fs.append(p, bufferSize, progress);
						this.appendMode = true;
					}
					else
					{
						@out = fs.create(p, true, bufferSize, replication, blockSize, progress);
					}
				}
				else
				{
					@out = streamOption.getValue();
				}
				java.lang.Class keyClass = keyClassOption == null ? Sharpen.Runtime.getClassForType
					(typeof(object)) : keyClassOption.getValue();
				java.lang.Class valueClass = valueClassOption == null ? Sharpen.Runtime.getClassForType
					(typeof(object)) : valueClassOption.getValue();
				org.apache.hadoop.io.SequenceFile.Metadata metadata = metadataOption == null ? new 
					org.apache.hadoop.io.SequenceFile.Metadata() : metadataOption.getValue();
				this.compress = compressionTypeOption.getValue();
				org.apache.hadoop.io.compress.CompressionCodec codec = compressionTypeOption.getCodec
					();
				if (codec != null && (codec is org.apache.hadoop.io.compress.GzipCodec) && !org.apache.hadoop.util.NativeCodeLoader
					.isNativeCodeLoaded() && !org.apache.hadoop.io.compress.zlib.ZlibFactory.isNativeZlibLoaded
					(conf))
				{
					throw new System.ArgumentException("SequenceFile doesn't work with " + "GzipCodec without native-hadoop "
						 + "code!");
				}
				init(conf, @out, ownStream, keyClass, valueClass, codec, metadata);
			}

			/// <summary>Create the named file.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
				)]
			public Writer(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.conf.Configuration
				 conf, org.apache.hadoop.fs.Path name, java.lang.Class keyClass, java.lang.Class
				 valClass)
			{
				{
					try
					{
						java.security.MessageDigest digester = java.security.MessageDigest.getInstance("MD5"
							);
						long time = org.apache.hadoop.util.Time.now();
						digester.update(Sharpen.Runtime.getBytesForString((new java.rmi.server.UID() + "@"
							 + time), org.apache.commons.io.Charsets.UTF_8));
						sync = digester.digest();
					}
					catch (System.Exception e)
					{
						throw new System.Exception(e);
					}
				}
				this.compress = org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
				init(conf, fs.create(name), true, keyClass, valClass, null, new org.apache.hadoop.io.SequenceFile.Metadata
					());
			}

			/// <summary>Create the named file with write-progress reporter.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
				)]
			public Writer(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.conf.Configuration
				 conf, org.apache.hadoop.fs.Path name, java.lang.Class keyClass, java.lang.Class
				 valClass, org.apache.hadoop.util.Progressable progress, org.apache.hadoop.io.SequenceFile.Metadata
				 metadata)
			{
				{
					try
					{
						java.security.MessageDigest digester = java.security.MessageDigest.getInstance("MD5"
							);
						long time = org.apache.hadoop.util.Time.now();
						digester.update(Sharpen.Runtime.getBytesForString((new java.rmi.server.UID() + "@"
							 + time), org.apache.commons.io.Charsets.UTF_8));
						sync = digester.digest();
					}
					catch (System.Exception e)
					{
						throw new System.Exception(e);
					}
				}
				this.compress = org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
				init(conf, fs.create(name, progress), true, keyClass, valClass, null, metadata);
			}

			/// <summary>Create the named file with write-progress reporter.</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use SequenceFile.createWriter(org.apache.hadoop.conf.Configuration, Option[]) instead."
				)]
			public Writer(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.conf.Configuration
				 conf, org.apache.hadoop.fs.Path name, java.lang.Class keyClass, java.lang.Class
				 valClass, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.io.SequenceFile.Metadata metadata)
			{
				{
					try
					{
						java.security.MessageDigest digester = java.security.MessageDigest.getInstance("MD5"
							);
						long time = org.apache.hadoop.util.Time.now();
						digester.update(Sharpen.Runtime.getBytesForString((new java.rmi.server.UID() + "@"
							 + time), org.apache.commons.io.Charsets.UTF_8));
						sync = digester.digest();
					}
					catch (System.Exception e)
					{
						throw new System.Exception(e);
					}
				}
				this.compress = org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
				init(conf, fs.create(name, true, bufferSize, replication, blockSize, progress), true
					, keyClass, valClass, null, metadata);
			}

			internal virtual bool isCompressed()
			{
				return compress != org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
			}

			internal virtual bool isBlockCompressed()
			{
				return compress == org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;
			}

			internal virtual org.apache.hadoop.io.SequenceFile.Writer ownStream()
			{
				this.ownOutputStream = true;
				return this;
			}

			/// <summary>Write and flush the file header.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void writeFileHeader()
			{
				@out.write(VERSION);
				org.apache.hadoop.io.Text.writeString(@out, keyClass.getName());
				org.apache.hadoop.io.Text.writeString(@out, valClass.getName());
				@out.writeBoolean(this.isCompressed());
				@out.writeBoolean(this.isBlockCompressed());
				if (this.isCompressed())
				{
					org.apache.hadoop.io.Text.writeString(@out, (Sharpen.Runtime.getClassForObject(codec
						)).getName());
				}
				this.metadata.write(@out);
				@out.write(sync);
				// write the sync bytes
				@out.flush();
			}

			// flush header
			/// <summary>Initialize.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void init(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.FSDataOutputStream
				 @out, bool ownStream, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.io.compress.CompressionCodec
				 codec, org.apache.hadoop.io.SequenceFile.Metadata metadata)
			{
				this.conf = conf;
				this.@out = @out;
				this.ownOutputStream = ownStream;
				this.keyClass = keyClass;
				this.valClass = valClass;
				this.codec = codec;
				this.metadata = metadata;
				org.apache.hadoop.io.serializer.SerializationFactory serializationFactory = new org.apache.hadoop.io.serializer.SerializationFactory
					(conf);
				this.keySerializer = serializationFactory.getSerializer(keyClass);
				if (this.keySerializer == null)
				{
					throw new System.IO.IOException("Could not find a serializer for the Key class: '"
						 + keyClass.getCanonicalName() + "'. " + "Please ensure that the configuration '"
						 + org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is " 
						+ "properly configured, if you're using" + "custom serialization.");
				}
				this.keySerializer.open(buffer);
				this.uncompressedValSerializer = serializationFactory.getSerializer(valClass);
				if (this.uncompressedValSerializer == null)
				{
					throw new System.IO.IOException("Could not find a serializer for the Value class: '"
						 + valClass.getCanonicalName() + "'. " + "Please ensure that the configuration '"
						 + org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is " 
						+ "properly configured, if you're using" + "custom serialization.");
				}
				this.uncompressedValSerializer.open(buffer);
				if (this.codec != null)
				{
					org.apache.hadoop.util.ReflectionUtils.setConf(this.codec, this.conf);
					this.compressor = org.apache.hadoop.io.compress.CodecPool.getCompressor(this.codec
						);
					this.deflateFilter = this.codec.createOutputStream(buffer, compressor);
					this.deflateOut = new java.io.DataOutputStream(new java.io.BufferedOutputStream(deflateFilter
						));
					this.compressedValSerializer = serializationFactory.getSerializer(valClass);
					if (this.compressedValSerializer == null)
					{
						throw new System.IO.IOException("Could not find a serializer for the Value class: '"
							 + valClass.getCanonicalName() + "'. " + "Please ensure that the configuration '"
							 + org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is " 
							+ "properly configured, if you're using" + "custom serialization.");
					}
					this.compressedValSerializer.open(deflateOut);
				}
				if (appendMode)
				{
					sync();
				}
				else
				{
					writeFileHeader();
				}
			}

			/// <summary>Returns the class of keys in this file.</summary>
			public virtual java.lang.Class getKeyClass()
			{
				return keyClass;
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual java.lang.Class getValueClass()
			{
				return valClass;
			}

			/// <summary>Returns the compression codec of data in this file.</summary>
			public virtual org.apache.hadoop.io.compress.CompressionCodec getCompressionCodec
				()
			{
				return codec;
			}

			/// <summary>create a sync point</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void sync()
			{
				if (sync != null && lastSyncPos != @out.getPos())
				{
					@out.writeInt(SYNC_ESCAPE);
					// mark the start of the sync
					@out.write(sync);
					// write sync
					lastSyncPos = @out.getPos();
				}
			}

			// update lastSyncPos
			/// <summary>flush all currently written data to the file system</summary>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use hsync() or hflush() instead")]
			public virtual void syncFs()
			{
				if (@out != null)
				{
					@out.sync();
				}
			}

			// flush contents to file system
			/// <exception cref="System.IO.IOException"/>
			public virtual void hsync()
			{
				if (@out != null)
				{
					@out.hsync();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void hflush()
			{
				if (@out != null)
				{
					@out.hflush();
				}
			}

			/// <summary>Returns the configuration of this file.</summary>
			internal virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				lock (this)
				{
					keySerializer.close();
					uncompressedValSerializer.close();
					if (compressedValSerializer != null)
					{
						compressedValSerializer.close();
					}
					org.apache.hadoop.io.compress.CodecPool.returnCompressor(compressor);
					compressor = null;
					if (@out != null)
					{
						// Close the underlying stream iff we own it...
						if (ownOutputStream)
						{
							@out.close();
						}
						else
						{
							@out.flush();
						}
						@out = null;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void checkAndWriteSync()
			{
				lock (this)
				{
					if (sync != null && @out.getPos() >= lastSyncPos + SYNC_INTERVAL)
					{
						// time to emit sync
						sync();
					}
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void append(org.apache.hadoop.io.Writable key, org.apache.hadoop.io.Writable
				 val)
			{
				append((object)key, (object)val);
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void append(object key, object val)
			{
				lock (this)
				{
					if (Sharpen.Runtime.getClassForObject(key) != keyClass)
					{
						throw new System.IO.IOException("wrong key class: " + Sharpen.Runtime.getClassForObject
							(key).getName() + " is not " + keyClass);
					}
					if (Sharpen.Runtime.getClassForObject(val) != valClass)
					{
						throw new System.IO.IOException("wrong value class: " + Sharpen.Runtime.getClassForObject
							(val).getName() + " is not " + valClass);
					}
					buffer.reset();
					// Append the 'key'
					keySerializer.serialize(key);
					int keyLength = buffer.getLength();
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed: " + key);
					}
					// Append the 'value'
					if (compress == org.apache.hadoop.io.SequenceFile.CompressionType.RECORD)
					{
						deflateFilter.resetState();
						compressedValSerializer.serialize(val);
						deflateOut.flush();
						deflateFilter.finish();
					}
					else
					{
						uncompressedValSerializer.serialize(val);
					}
					// Write the record out
					checkAndWriteSync();
					// sync
					@out.writeInt(buffer.getLength());
					// total record length
					@out.writeInt(keyLength);
					// key portion length
					@out.write(buffer.getData(), 0, buffer.getLength());
				}
			}

			// data
			/// <exception cref="System.IO.IOException"/>
			public virtual void appendRaw(byte[] keyData, int keyOffset, int keyLength, org.apache.hadoop.io.SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed: " + keyLength);
					}
					int valLength = val.getSize();
					checkAndWriteSync();
					@out.writeInt(keyLength + valLength);
					// total record length
					@out.writeInt(keyLength);
					// key portion length
					@out.write(keyData, keyOffset, keyLength);
					// key
					val.writeUncompressedBytes(@out);
				}
			}

			// value
			/// <summary>Returns the current length of the output file.</summary>
			/// <remarks>
			/// Returns the current length of the output file.
			/// <p>This always returns a synchronized position.  In other words,
			/// immediately after calling
			/// <see cref="Reader.seek(long)"/>
			/// with a position
			/// returned by this method,
			/// <see cref="Reader.next(Writable)"/>
			/// may be called.  However
			/// the key may be earlier in the file than key last written when this
			/// method was called (e.g., with block-compression, it may be the first key
			/// in the block that was being written when this method was called).
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual long getLength()
			{
				lock (this)
				{
					return @out.getPos();
				}
			}
		}

		/// <summary>Write key/compressed-value pairs to a sequence-format file.</summary>
		internal class RecordCompressWriter : org.apache.hadoop.io.SequenceFile.Writer
		{
			/// <exception cref="System.IO.IOException"/>
			internal RecordCompressWriter(org.apache.hadoop.conf.Configuration conf, params org.apache.hadoop.io.SequenceFile.Writer.Option
				[] options)
				: base(conf, options)
			{
			}

			// class Writer
			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void append(object key, object val)
			{
				lock (this)
				{
					if (Sharpen.Runtime.getClassForObject(key) != keyClass)
					{
						throw new System.IO.IOException("wrong key class: " + Sharpen.Runtime.getClassForObject
							(key).getName() + " is not " + keyClass);
					}
					if (Sharpen.Runtime.getClassForObject(val) != valClass)
					{
						throw new System.IO.IOException("wrong value class: " + Sharpen.Runtime.getClassForObject
							(val).getName() + " is not " + valClass);
					}
					buffer.reset();
					// Append the 'key'
					keySerializer.serialize(key);
					int keyLength = buffer.getLength();
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed: " + key);
					}
					// Compress 'value' and append it
					deflateFilter.resetState();
					compressedValSerializer.serialize(val);
					deflateOut.flush();
					deflateFilter.finish();
					// Write the record out
					checkAndWriteSync();
					// sync
					@out.writeInt(buffer.getLength());
					// total record length
					@out.writeInt(keyLength);
					// key portion length
					@out.write(buffer.getData(), 0, buffer.getLength());
				}
			}

			// data
			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void appendRaw(byte[] keyData, int keyOffset, int keyLength, org.apache.hadoop.io.SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed: " + keyLength);
					}
					int valLength = val.getSize();
					checkAndWriteSync();
					// sync
					@out.writeInt(keyLength + valLength);
					// total record length
					@out.writeInt(keyLength);
					// key portion length
					@out.write(keyData, keyOffset, keyLength);
					// 'key' data
					val.writeCompressedBytes(@out);
				}
			}
			// 'value' data
		}

		/// <summary>Write compressed key/value blocks to a sequence-format file.</summary>
		internal class BlockCompressWriter : org.apache.hadoop.io.SequenceFile.Writer
		{
			private int noBufferedRecords = 0;

			private org.apache.hadoop.io.DataOutputBuffer keyLenBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.io.DataOutputBuffer keyBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.io.DataOutputBuffer valLenBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			private org.apache.hadoop.io.DataOutputBuffer valBuffer = new org.apache.hadoop.io.DataOutputBuffer
				();

			private readonly int compressionBlockSize;

			/// <exception cref="System.IO.IOException"/>
			internal BlockCompressWriter(org.apache.hadoop.conf.Configuration conf, params org.apache.hadoop.io.SequenceFile.Writer.Option
				[] options)
				: base(conf, options)
			{
				// RecordCompressionWriter
				compressionBlockSize = conf.getInt("io.seqfile.compress.blocksize", 1000000);
				keySerializer.close();
				keySerializer.open(keyBuffer);
				uncompressedValSerializer.close();
				uncompressedValSerializer.open(valBuffer);
			}

			/// <summary>Workhorse to check and write out compressed data/lengths</summary>
			/// <exception cref="System.IO.IOException"/>
			private void writeBuffer(org.apache.hadoop.io.DataOutputBuffer uncompressedDataBuffer
				)
			{
				lock (this)
				{
					deflateFilter.resetState();
					buffer.reset();
					deflateOut.write(uncompressedDataBuffer.getData(), 0, uncompressedDataBuffer.getLength
						());
					deflateOut.flush();
					deflateFilter.finish();
					org.apache.hadoop.io.WritableUtils.writeVInt(@out, buffer.getLength());
					@out.write(buffer.getData(), 0, buffer.getLength());
				}
			}

			/// <summary>Compress and flush contents to dfs</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void sync()
			{
				lock (this)
				{
					if (noBufferedRecords > 0)
					{
						base.sync();
						// No. of records
						org.apache.hadoop.io.WritableUtils.writeVInt(@out, noBufferedRecords);
						// Write 'keys' and lengths
						writeBuffer(keyLenBuffer);
						writeBuffer(keyBuffer);
						// Write 'values' and lengths
						writeBuffer(valLenBuffer);
						writeBuffer(valBuffer);
						// Flush the file-stream
						@out.flush();
						// Reset internal states
						keyLenBuffer.reset();
						keyBuffer.reset();
						valLenBuffer.reset();
						valBuffer.reset();
						noBufferedRecords = 0;
					}
				}
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				lock (this)
				{
					if (@out != null)
					{
						sync();
					}
					base.close();
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void append(object key, object val)
			{
				lock (this)
				{
					if (Sharpen.Runtime.getClassForObject(key) != keyClass)
					{
						throw new System.IO.IOException("wrong key class: " + key + " is not " + keyClass
							);
					}
					if (Sharpen.Runtime.getClassForObject(val) != valClass)
					{
						throw new System.IO.IOException("wrong value class: " + val + " is not " + valClass
							);
					}
					// Save key/value into respective buffers 
					int oldKeyLength = keyBuffer.getLength();
					keySerializer.serialize(key);
					int keyLength = keyBuffer.getLength() - oldKeyLength;
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed: " + key);
					}
					org.apache.hadoop.io.WritableUtils.writeVInt(keyLenBuffer, keyLength);
					int oldValLength = valBuffer.getLength();
					uncompressedValSerializer.serialize(val);
					int valLength = valBuffer.getLength() - oldValLength;
					org.apache.hadoop.io.WritableUtils.writeVInt(valLenBuffer, valLength);
					// Added another key/value pair
					++noBufferedRecords;
					// Compress and flush?
					int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
					if (currentBlockSize >= compressionBlockSize)
					{
						sync();
					}
				}
			}

			/// <summary>Append a key/value pair.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void appendRaw(byte[] keyData, int keyOffset, int keyLength, org.apache.hadoop.io.SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (keyLength < 0)
					{
						throw new System.IO.IOException("negative length keys not allowed");
					}
					int valLength = val.getSize();
					// Save key/value data in relevant buffers
					org.apache.hadoop.io.WritableUtils.writeVInt(keyLenBuffer, keyLength);
					keyBuffer.write(keyData, keyOffset, keyLength);
					org.apache.hadoop.io.WritableUtils.writeVInt(valLenBuffer, valLength);
					val.writeUncompressedBytes(valBuffer);
					// Added another key/value pair
					++noBufferedRecords;
					// Compress and flush?
					int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
					if (currentBlockSize >= compressionBlockSize)
					{
						sync();
					}
				}
			}
		}

		// BlockCompressionWriter
		/// <summary>Get the configured buffer size</summary>
		private static int getBufferSize(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getInt("io.file.buffer.size", 4096);
		}

		/// <summary>Reads key/value pairs from a sequence-format file.</summary>
		public class Reader : java.io.Closeable
		{
			private string filename;

			private org.apache.hadoop.fs.FSDataInputStream @in;

			private org.apache.hadoop.io.DataOutputBuffer outBuf = new org.apache.hadoop.io.DataOutputBuffer
				();

			private byte version;

			private string keyClassName;

			private string valClassName;

			private java.lang.Class keyClass;

			private java.lang.Class valClass;

			private org.apache.hadoop.io.compress.CompressionCodec codec = null;

			private org.apache.hadoop.io.SequenceFile.Metadata metadata = null;

			private byte[] sync = new byte[SYNC_HASH_SIZE];

			private byte[] syncCheck = new byte[SYNC_HASH_SIZE];

			private bool syncSeen;

			private long headerEnd;

			private long end;

			private int keyLength;

			private int recordLength;

			private bool decompress;

			private bool blockCompressed;

			private org.apache.hadoop.conf.Configuration conf;

			private int noBufferedRecords = 0;

			private bool lazyDecompress = true;

			private bool valuesDecompressed = true;

			private int noBufferedKeys = 0;

			private int noBufferedValues = 0;

			private org.apache.hadoop.io.DataInputBuffer keyLenBuffer = null;

			private org.apache.hadoop.io.compress.CompressionInputStream keyLenInFilter = null;

			private java.io.DataInputStream keyLenIn = null;

			private org.apache.hadoop.io.compress.Decompressor keyLenDecompressor = null;

			private org.apache.hadoop.io.DataInputBuffer keyBuffer = null;

			private org.apache.hadoop.io.compress.CompressionInputStream keyInFilter = null;

			private java.io.DataInputStream keyIn = null;

			private org.apache.hadoop.io.compress.Decompressor keyDecompressor = null;

			private org.apache.hadoop.io.DataInputBuffer valLenBuffer = null;

			private org.apache.hadoop.io.compress.CompressionInputStream valLenInFilter = null;

			private java.io.DataInputStream valLenIn = null;

			private org.apache.hadoop.io.compress.Decompressor valLenDecompressor = null;

			private org.apache.hadoop.io.DataInputBuffer valBuffer = null;

			private org.apache.hadoop.io.compress.CompressionInputStream valInFilter = null;

			private java.io.DataInputStream valIn = null;

			private org.apache.hadoop.io.compress.Decompressor valDecompressor = null;

			private org.apache.hadoop.io.serializer.Deserializer keyDeserializer;

			private org.apache.hadoop.io.serializer.Deserializer valDeserializer;

			/// <summary>A tag interface for all of the Reader options</summary>
			public interface Option
			{
			}

			/// <summary>Create an option to specify the path name of the sequence file.</summary>
			/// <param name="value">the path to read</param>
			/// <returns>a new option</returns>
			public static org.apache.hadoop.io.SequenceFile.Reader.Option file(org.apache.hadoop.fs.Path
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Reader.FileOption(value);
			}

			/// <summary>Create an option to specify the stream with the sequence file.</summary>
			/// <param name="value">the stream to read.</param>
			/// <returns>a new option</returns>
			public static org.apache.hadoop.io.SequenceFile.Reader.Option stream(org.apache.hadoop.fs.FSDataInputStream
				 value)
			{
				return new org.apache.hadoop.io.SequenceFile.Reader.InputStreamOption(value);
			}

			/// <summary>Create an option to specify the starting byte to read.</summary>
			/// <param name="value">the number of bytes to skip over</param>
			/// <returns>a new option</returns>
			public static org.apache.hadoop.io.SequenceFile.Reader.Option start(long value)
			{
				return new org.apache.hadoop.io.SequenceFile.Reader.StartOption(value);
			}

			/// <summary>Create an option to specify the number of bytes to read.</summary>
			/// <param name="value">the number of bytes to read</param>
			/// <returns>a new option</returns>
			public static org.apache.hadoop.io.SequenceFile.Reader.Option length(long value)
			{
				return new org.apache.hadoop.io.SequenceFile.Reader.LengthOption(value);
			}

			/// <summary>Create an option with the buffer size for reading the given pathname.</summary>
			/// <param name="value">the number of bytes to buffer</param>
			/// <returns>a new option</returns>
			public static org.apache.hadoop.io.SequenceFile.Reader.Option bufferSize(int value
				)
			{
				return new org.apache.hadoop.io.SequenceFile.Reader.BufferSizeOption(value);
			}

			private class FileOption : org.apache.hadoop.util.Options.PathOption, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private FileOption(org.apache.hadoop.fs.Path value)
					: base(value)
				{
				}
			}

			private class InputStreamOption : org.apache.hadoop.util.Options.FSDataInputStreamOption
				, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private InputStreamOption(org.apache.hadoop.fs.FSDataInputStream value)
					: base(value)
				{
				}
			}

			private class StartOption : org.apache.hadoop.util.Options.LongOption, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private StartOption(long value)
					: base(value)
				{
				}
			}

			private class LengthOption : org.apache.hadoop.util.Options.LongOption, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private LengthOption(long value)
					: base(value)
				{
				}
			}

			private class BufferSizeOption : org.apache.hadoop.util.Options.IntegerOption, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private BufferSizeOption(int value)
					: base(value)
				{
				}
			}

			private class OnlyHeaderOption : org.apache.hadoop.util.Options.BooleanOption, org.apache.hadoop.io.SequenceFile.Reader.Option
			{
				private OnlyHeaderOption()
					: base(true)
				{
				}
				// only used directly
			}

			/// <exception cref="System.IO.IOException"/>
			public Reader(org.apache.hadoop.conf.Configuration conf, params org.apache.hadoop.io.SequenceFile.Reader.Option
				[] opts)
			{
				// Look up the options, these are null if not set
				org.apache.hadoop.io.SequenceFile.Reader.FileOption fileOpt = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.FileOption>(opts);
				org.apache.hadoop.io.SequenceFile.Reader.InputStreamOption streamOpt = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.InputStreamOption>(opts);
				org.apache.hadoop.io.SequenceFile.Reader.StartOption startOpt = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.StartOption>(opts);
				org.apache.hadoop.io.SequenceFile.Reader.LengthOption lenOpt = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.LengthOption>(opts);
				org.apache.hadoop.io.SequenceFile.Reader.BufferSizeOption bufOpt = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.BufferSizeOption>(opts);
				org.apache.hadoop.io.SequenceFile.Reader.OnlyHeaderOption headerOnly = org.apache.hadoop.util.Options
					.getOption<org.apache.hadoop.io.SequenceFile.Reader.OnlyHeaderOption>(opts);
				// check for consistency
				if ((fileOpt == null) == (streamOpt == null))
				{
					throw new System.ArgumentException("File or stream option must be specified");
				}
				if (fileOpt == null && bufOpt != null)
				{
					throw new System.ArgumentException("buffer size can only be set when" + " a file is specified."
						);
				}
				// figure out the real values
				org.apache.hadoop.fs.Path filename = null;
				org.apache.hadoop.fs.FSDataInputStream file;
				long len;
				if (fileOpt != null)
				{
					filename = fileOpt.getValue();
					org.apache.hadoop.fs.FileSystem fs = filename.getFileSystem(conf);
					int bufSize = bufOpt == null ? getBufferSize(conf) : bufOpt.getValue();
					len = null == lenOpt ? fs.getFileStatus(filename).getLen() : lenOpt.getValue();
					file = openFile(fs, filename, bufSize, len);
				}
				else
				{
					len = null == lenOpt ? long.MaxValue : lenOpt.getValue();
					file = streamOpt.getValue();
				}
				long start = startOpt == null ? 0 : startOpt.getValue();
				// really set up
				initialize(filename, file, start, len, conf, headerOnly != null);
			}

			/// <summary>Construct a reader by opening a file from the given file system.</summary>
			/// <param name="fs">The file system used to open the file.</param>
			/// <param name="file">The file being read.</param>
			/// <param name="conf">Configuration</param>
			/// <exception cref="System.IO.IOException"/>
			[System.ObsoleteAttribute(@"Use Reader(Configuration, Option...) instead.")]
			public Reader(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path file, 
				org.apache.hadoop.conf.Configuration conf)
				: this(conf, file(file.makeQualified(fs)))
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
			public Reader(org.apache.hadoop.fs.FSDataInputStream @in, int buffersize, long start
				, long length, org.apache.hadoop.conf.Configuration conf)
				: this(conf, stream(@in), start(start), length(length))
			{
			}

			/// <summary>Common work of the constructors.</summary>
			/// <exception cref="System.IO.IOException"/>
			private void initialize(org.apache.hadoop.fs.Path filename, org.apache.hadoop.fs.FSDataInputStream
				 @in, long start, long length, org.apache.hadoop.conf.Configuration conf, bool tempReader
				)
			{
				if (@in == null)
				{
					throw new System.ArgumentException("in == null");
				}
				this.filename = filename == null ? "<unknown>" : filename.ToString();
				this.@in = @in;
				this.conf = conf;
				bool succeeded = false;
				try
				{
					seek(start);
					this.end = this.@in.getPos() + length;
					// if it wrapped around, use the max
					if (end < length)
					{
						end = long.MaxValue;
					}
					init(tempReader);
					succeeded = true;
				}
				finally
				{
					if (!succeeded)
					{
						org.apache.hadoop.io.IOUtils.cleanup(LOG, this.@in);
					}
				}
			}

			/// <summary>
			/// Override this method to specialize the type of
			/// <see cref="org.apache.hadoop.fs.FSDataInputStream"/>
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
			protected internal virtual org.apache.hadoop.fs.FSDataInputStream openFile(org.apache.hadoop.fs.FileSystem
				 fs, org.apache.hadoop.fs.Path file, int bufferSize, long length)
			{
				return fs.open(file, bufferSize);
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
			private void init(bool tempReader)
			{
				byte[] versionBlock = new byte[VERSION.Length];
				@in.readFully(versionBlock);
				if ((versionBlock[0] != VERSION[0]) || (versionBlock[1] != VERSION[1]) || (versionBlock
					[2] != VERSION[2]))
				{
					throw new System.IO.IOException(this + " not a SequenceFile");
				}
				// Set 'version'
				version = versionBlock[3];
				if (version > VERSION[3])
				{
					throw new org.apache.hadoop.io.VersionMismatchException(VERSION[3], version);
				}
				if (((sbyte)version) < BLOCK_COMPRESS_VERSION)
				{
					org.apache.hadoop.io.UTF8 className = new org.apache.hadoop.io.UTF8();
					className.readFields(@in);
					keyClassName = className.toStringChecked();
					// key class name
					className.readFields(@in);
					valClassName = className.toStringChecked();
				}
				else
				{
					// val class name
					keyClassName = org.apache.hadoop.io.Text.readString(@in);
					valClassName = org.apache.hadoop.io.Text.readString(@in);
				}
				if (version > 2)
				{
					// if version > 2
					this.decompress = @in.readBoolean();
				}
				else
				{
					// is compressed?
					decompress = false;
				}
				if (version >= BLOCK_COMPRESS_VERSION)
				{
					// if version >= 4
					this.blockCompressed = @in.readBoolean();
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
					if (version >= CUSTOM_COMPRESS_VERSION)
					{
						string codecClassname = org.apache.hadoop.io.Text.readString(@in);
						try
						{
							java.lang.Class codecClass = conf.getClassByName(codecClassname).asSubclass<org.apache.hadoop.io.compress.CompressionCodec
								>();
							this.codec = org.apache.hadoop.util.ReflectionUtils.newInstance(codecClass, conf);
						}
						catch (java.lang.ClassNotFoundException cnfe)
						{
							throw new System.ArgumentException("Unknown codec: " + codecClassname, cnfe);
						}
					}
					else
					{
						codec = new org.apache.hadoop.io.compress.DefaultCodec();
						((org.apache.hadoop.conf.Configurable)codec).setConf(conf);
					}
				}
				this.metadata = new org.apache.hadoop.io.SequenceFile.Metadata();
				if (version >= VERSION_WITH_METADATA)
				{
					// if version >= 6
					this.metadata.readFields(@in);
				}
				if (version > 1)
				{
					// if version > 1
					@in.readFully(sync);
					// read sync bytes
					headerEnd = @in.getPos();
				}
				// record end of header
				// Initialize... *not* if this we are constructing a temporary Reader
				if (!tempReader)
				{
					valBuffer = new org.apache.hadoop.io.DataInputBuffer();
					if (decompress)
					{
						valDecompressor = org.apache.hadoop.io.compress.CodecPool.getDecompressor(codec);
						valInFilter = codec.createInputStream(valBuffer, valDecompressor);
						valIn = new java.io.DataInputStream(valInFilter);
					}
					else
					{
						valIn = valBuffer;
					}
					if (blockCompressed)
					{
						keyLenBuffer = new org.apache.hadoop.io.DataInputBuffer();
						keyBuffer = new org.apache.hadoop.io.DataInputBuffer();
						valLenBuffer = new org.apache.hadoop.io.DataInputBuffer();
						keyLenDecompressor = org.apache.hadoop.io.compress.CodecPool.getDecompressor(codec
							);
						keyLenInFilter = codec.createInputStream(keyLenBuffer, keyLenDecompressor);
						keyLenIn = new java.io.DataInputStream(keyLenInFilter);
						keyDecompressor = org.apache.hadoop.io.compress.CodecPool.getDecompressor(codec);
						keyInFilter = codec.createInputStream(keyBuffer, keyDecompressor);
						keyIn = new java.io.DataInputStream(keyInFilter);
						valLenDecompressor = org.apache.hadoop.io.compress.CodecPool.getDecompressor(codec
							);
						valLenInFilter = codec.createInputStream(valLenBuffer, valLenDecompressor);
						valLenIn = new java.io.DataInputStream(valLenInFilter);
					}
					org.apache.hadoop.io.serializer.SerializationFactory serializationFactory = new org.apache.hadoop.io.serializer.SerializationFactory
						(conf);
					this.keyDeserializer = getDeserializer(serializationFactory, getKeyClass());
					if (this.keyDeserializer == null)
					{
						throw new System.IO.IOException("Could not find a deserializer for the Key class: '"
							 + getKeyClass().getCanonicalName() + "'. " + "Please ensure that the configuration '"
							 + org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is " 
							+ "properly configured, if you're using " + "custom serialization.");
					}
					if (!blockCompressed)
					{
						this.keyDeserializer.open(valBuffer);
					}
					else
					{
						this.keyDeserializer.open(keyIn);
					}
					this.valDeserializer = getDeserializer(serializationFactory, getValueClass());
					if (this.valDeserializer == null)
					{
						throw new System.IO.IOException("Could not find a deserializer for the Value class: '"
							 + getValueClass().getCanonicalName() + "'. " + "Please ensure that the configuration '"
							 + org.apache.hadoop.fs.CommonConfigurationKeys.IO_SERIALIZATIONS_KEY + "' is " 
							+ "properly configured, if you're using " + "custom serialization.");
					}
					this.valDeserializer.open(valIn);
				}
			}

			private org.apache.hadoop.io.serializer.Deserializer getDeserializer(org.apache.hadoop.io.serializer.SerializationFactory
				 sf, java.lang.Class c)
			{
				return sf.getDeserializer(c);
			}

			/// <summary>Close the file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				lock (this)
				{
					// Return the decompressors to the pool
					org.apache.hadoop.io.compress.CodecPool.returnDecompressor(keyLenDecompressor);
					org.apache.hadoop.io.compress.CodecPool.returnDecompressor(keyDecompressor);
					org.apache.hadoop.io.compress.CodecPool.returnDecompressor(valLenDecompressor);
					org.apache.hadoop.io.compress.CodecPool.returnDecompressor(valDecompressor);
					keyLenDecompressor = keyDecompressor = null;
					valLenDecompressor = valDecompressor = null;
					if (keyDeserializer != null)
					{
						keyDeserializer.close();
					}
					if (valDeserializer != null)
					{
						valDeserializer.close();
					}
					// Close the input-stream
					@in.close();
				}
			}

			/// <summary>Returns the name of the key class.</summary>
			public virtual string getKeyClassName()
			{
				return keyClassName;
			}

			/// <summary>Returns the class of keys in this file.</summary>
			public virtual java.lang.Class getKeyClass()
			{
				lock (this)
				{
					if (null == keyClass)
					{
						try
						{
							keyClass = org.apache.hadoop.io.WritableName.getClass(getKeyClassName(), conf);
						}
						catch (System.IO.IOException e)
						{
							throw new System.Exception(e);
						}
					}
					return keyClass;
				}
			}

			/// <summary>Returns the name of the value class.</summary>
			public virtual string getValueClassName()
			{
				return valClassName;
			}

			/// <summary>Returns the class of values in this file.</summary>
			public virtual java.lang.Class getValueClass()
			{
				lock (this)
				{
					if (null == valClass)
					{
						try
						{
							valClass = org.apache.hadoop.io.WritableName.getClass(getValueClassName(), conf);
						}
						catch (System.IO.IOException e)
						{
							throw new System.Exception(e);
						}
					}
					return valClass;
				}
			}

			/// <summary>Returns true if values are compressed.</summary>
			public virtual bool isCompressed()
			{
				return decompress;
			}

			/// <summary>Returns true if records are block-compressed.</summary>
			public virtual bool isBlockCompressed()
			{
				return blockCompressed;
			}

			/// <summary>Returns the compression codec of data in this file.</summary>
			public virtual org.apache.hadoop.io.compress.CompressionCodec getCompressionCodec
				()
			{
				return codec;
			}

			private byte[] getSync()
			{
				return sync;
			}

			private byte getVersion()
			{
				return version;
			}

			/// <summary>Get the compression type for this file.</summary>
			/// <returns>the compression type</returns>
			public virtual org.apache.hadoop.io.SequenceFile.CompressionType getCompressionType
				()
			{
				if (decompress)
				{
					return blockCompressed ? org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK : 
						org.apache.hadoop.io.SequenceFile.CompressionType.RECORD;
				}
				else
				{
					return org.apache.hadoop.io.SequenceFile.CompressionType.NONE;
				}
			}

			/// <summary>Returns the metadata object of the file</summary>
			public virtual org.apache.hadoop.io.SequenceFile.Metadata getMetadata()
			{
				return this.metadata;
			}

			/// <summary>Returns the configuration used for this file.</summary>
			internal virtual org.apache.hadoop.conf.Configuration getConf()
			{
				return conf;
			}

			/// <summary>Read a compressed buffer</summary>
			/// <exception cref="System.IO.IOException"/>
			private void readBuffer(org.apache.hadoop.io.DataInputBuffer buffer, org.apache.hadoop.io.compress.CompressionInputStream
				 filter)
			{
				lock (this)
				{
					// Read data into a temporary buffer
					org.apache.hadoop.io.DataOutputBuffer dataBuffer = new org.apache.hadoop.io.DataOutputBuffer
						();
					try
					{
						int dataBufferLength = org.apache.hadoop.io.WritableUtils.readVInt(@in);
						dataBuffer.write(@in, dataBufferLength);
						// Set up 'buffer' connected to the input-stream
						buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
					}
					finally
					{
						dataBuffer.close();
					}
					// Reset the codec
					filter.resetState();
				}
			}

			/// <summary>Read the next 'compressed' block</summary>
			/// <exception cref="System.IO.IOException"/>
			private void readBlock()
			{
				lock (this)
				{
					// Check if we need to throw away a whole block of 
					// 'values' due to 'lazy decompression' 
					if (lazyDecompress && !valuesDecompressed)
					{
						@in.seek(org.apache.hadoop.io.WritableUtils.readVInt(@in) + @in.getPos());
						@in.seek(org.apache.hadoop.io.WritableUtils.readVInt(@in) + @in.getPos());
					}
					// Reset internal states
					noBufferedKeys = 0;
					noBufferedValues = 0;
					noBufferedRecords = 0;
					valuesDecompressed = false;
					//Process sync
					if (sync != null)
					{
						@in.readInt();
						@in.readFully(syncCheck);
						// read syncCheck
						if (!java.util.Arrays.equals(sync, syncCheck))
						{
							// check it
							throw new System.IO.IOException("File is corrupt!");
						}
					}
					syncSeen = true;
					// Read number of records in this block
					noBufferedRecords = org.apache.hadoop.io.WritableUtils.readVInt(@in);
					// Read key lengths and keys
					readBuffer(keyLenBuffer, keyLenInFilter);
					readBuffer(keyBuffer, keyInFilter);
					noBufferedKeys = noBufferedRecords;
					// Read value lengths and values
					if (!lazyDecompress)
					{
						readBuffer(valLenBuffer, valLenInFilter);
						readBuffer(valBuffer, valInFilter);
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
			private void seekToCurrentValue()
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						if (decompress)
						{
							valInFilter.resetState();
						}
						valBuffer.reset();
					}
					else
					{
						// Check if this is the first value in the 'block' to be read
						if (lazyDecompress && !valuesDecompressed)
						{
							// Read the value lengths and values
							readBuffer(valLenBuffer, valLenInFilter);
							readBuffer(valBuffer, valInFilter);
							noBufferedValues = noBufferedRecords;
							valuesDecompressed = true;
						}
						// Calculate the no. of bytes to skip
						// Note: 'current' key has already been read!
						int skipValBytes = 0;
						int currentKey = noBufferedKeys + 1;
						for (int i = noBufferedValues; i > currentKey; --i)
						{
							skipValBytes += org.apache.hadoop.io.WritableUtils.readVInt(valLenIn);
							--noBufferedValues;
						}
						// Skip to the 'val' corresponding to 'current' key
						if (skipValBytes > 0)
						{
							if (valIn.skipBytes(skipValBytes) != skipValBytes)
							{
								throw new System.IO.IOException("Failed to seek to " + currentKey + "(th) value!"
									);
							}
						}
					}
				}
			}

			/// <summary>Get the 'value' corresponding to the last read 'key'.</summary>
			/// <param name="val">: The 'value' to be read.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void getCurrentValue(org.apache.hadoop.io.Writable val)
			{
				lock (this)
				{
					if (val is org.apache.hadoop.conf.Configurable)
					{
						((org.apache.hadoop.conf.Configurable)val).setConf(this.conf);
					}
					// Position stream to 'current' value
					seekToCurrentValue();
					if (!blockCompressed)
					{
						val.readFields(valIn);
						if (valIn.read() > 0)
						{
							LOG.info("available bytes: " + valIn.available());
							throw new System.IO.IOException(val + " read " + (valBuffer.getPosition() - keyLength
								) + " bytes, should read " + (valBuffer.getLength() - keyLength));
						}
					}
					else
					{
						// Get the value
						int valLength = org.apache.hadoop.io.WritableUtils.readVInt(valLenIn);
						val.readFields(valIn);
						// Read another compressed 'value'
						--noBufferedValues;
						// Sanity check
						if ((valLength < 0) && LOG.isDebugEnabled())
						{
							LOG.debug(val + " is a zero-length value");
						}
					}
				}
			}

			/// <summary>Get the 'value' corresponding to the last read 'key'.</summary>
			/// <param name="val">: The 'value' to be read.</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual object getCurrentValue(object val)
			{
				lock (this)
				{
					if (val is org.apache.hadoop.conf.Configurable)
					{
						((org.apache.hadoop.conf.Configurable)val).setConf(this.conf);
					}
					// Position stream to 'current' value
					seekToCurrentValue();
					if (!blockCompressed)
					{
						val = deserializeValue(val);
						if (valIn.read() > 0)
						{
							LOG.info("available bytes: " + valIn.available());
							throw new System.IO.IOException(val + " read " + (valBuffer.getPosition() - keyLength
								) + " bytes, should read " + (valBuffer.getLength() - keyLength));
						}
					}
					else
					{
						// Get the value
						int valLength = org.apache.hadoop.io.WritableUtils.readVInt(valLenIn);
						val = deserializeValue(val);
						// Read another compressed 'value'
						--noBufferedValues;
						// Sanity check
						if ((valLength < 0) && LOG.isDebugEnabled())
						{
							LOG.debug(val + " is a zero-length value");
						}
					}
					return val;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private object deserializeValue(object val)
			{
				return valDeserializer.deserialize(val);
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
			public virtual bool next(org.apache.hadoop.io.Writable key)
			{
				lock (this)
				{
					if (Sharpen.Runtime.getClassForObject(key) != getKeyClass())
					{
						throw new System.IO.IOException("wrong key class: " + Sharpen.Runtime.getClassForObject
							(key).getName() + " is not " + keyClass);
					}
					if (!blockCompressed)
					{
						outBuf.reset();
						keyLength = next(outBuf);
						if (keyLength < 0)
						{
							return false;
						}
						valBuffer.reset(outBuf.getData(), outBuf.getLength());
						key.readFields(valBuffer);
						valBuffer.mark(0);
						if (valBuffer.getPosition() != keyLength)
						{
							throw new System.IO.IOException(key + " read " + valBuffer.getPosition() + " bytes, should read "
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
								readBlock();
							}
							catch (java.io.EOFException)
							{
								return false;
							}
						}
						int keyLength = org.apache.hadoop.io.WritableUtils.readVInt(keyLenIn);
						// Sanity check
						if (keyLength < 0)
						{
							return false;
						}
						//Read another compressed 'key'
						key.readFields(keyIn);
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
			public virtual bool next(org.apache.hadoop.io.Writable key, org.apache.hadoop.io.Writable
				 val)
			{
				lock (this)
				{
					if (Sharpen.Runtime.getClassForObject(val) != getValueClass())
					{
						throw new System.IO.IOException("wrong value class: " + val + " is not " + valClass
							);
					}
					bool more = next(key);
					if (more)
					{
						getCurrentValue(val);
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
			private int readRecordLength()
			{
				lock (this)
				{
					if (@in.getPos() >= end)
					{
						return -1;
					}
					int length = @in.readInt();
					if (version > 1 && sync != null && length == SYNC_ESCAPE)
					{
						// process a sync entry
						@in.readFully(syncCheck);
						// read syncCheck
						if (!java.util.Arrays.equals(sync, syncCheck))
						{
							// check it
							throw new System.IO.IOException("File is corrupt!");
						}
						syncSeen = true;
						if (@in.getPos() >= end)
						{
							return -1;
						}
						length = @in.readInt();
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
			[System.ObsoleteAttribute(@"Call nextRaw(DataOutputBuffer, ValueBytes) .")]
			internal virtual int next(org.apache.hadoop.io.DataOutputBuffer buffer)
			{
				lock (this)
				{
					// Unsupported for block-compressed sequence files
					if (blockCompressed)
					{
						throw new System.IO.IOException("Unsupported call for block-compressed" + " SequenceFiles - use SequenceFile.Reader.next(DataOutputStream, ValueBytes)"
							);
					}
					try
					{
						int length = readRecordLength();
						if (length == -1)
						{
							return -1;
						}
						int keyLength = @in.readInt();
						buffer.write(@in, length);
						return keyLength;
					}
					catch (org.apache.hadoop.fs.ChecksumException e)
					{
						// checksum failure
						handleChecksumException(e);
						return next(buffer);
					}
				}
			}

			public virtual org.apache.hadoop.io.SequenceFile.ValueBytes createValueBytes()
			{
				org.apache.hadoop.io.SequenceFile.ValueBytes val = null;
				if (!decompress || blockCompressed)
				{
					val = new org.apache.hadoop.io.SequenceFile.UncompressedBytes();
				}
				else
				{
					val = new org.apache.hadoop.io.SequenceFile.CompressedBytes(codec);
				}
				return val;
			}

			/// <summary>Read 'raw' records.</summary>
			/// <param name="key">- The buffer into which the key is read</param>
			/// <param name="val">- The 'raw' value</param>
			/// <returns>Returns the total record length or -1 for end of file</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int nextRaw(org.apache.hadoop.io.DataOutputBuffer key, org.apache.hadoop.io.SequenceFile.ValueBytes
				 val)
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						int length = readRecordLength();
						if (length == -1)
						{
							return -1;
						}
						int keyLength = @in.readInt();
						int valLength = length - keyLength;
						key.write(@in, keyLength);
						if (decompress)
						{
							org.apache.hadoop.io.SequenceFile.CompressedBytes value = (org.apache.hadoop.io.SequenceFile.CompressedBytes
								)val;
							value.reset(@in, valLength);
						}
						else
						{
							org.apache.hadoop.io.SequenceFile.UncompressedBytes value = (org.apache.hadoop.io.SequenceFile.UncompressedBytes
								)val;
							value.reset(@in, valLength);
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
							if (@in.getPos() >= end)
							{
								return -1;
							}
							try
							{
								readBlock();
							}
							catch (java.io.EOFException)
							{
								return -1;
							}
						}
						int keyLength = org.apache.hadoop.io.WritableUtils.readVInt(keyLenIn);
						if (keyLength < 0)
						{
							throw new System.IO.IOException("zero length key found!");
						}
						key.write(keyIn, keyLength);
						--noBufferedKeys;
						// Read raw 'value'
						seekToCurrentValue();
						int valLength = org.apache.hadoop.io.WritableUtils.readVInt(valLenIn);
						org.apache.hadoop.io.SequenceFile.UncompressedBytes rawValue = (org.apache.hadoop.io.SequenceFile.UncompressedBytes
							)val;
						rawValue.reset(valIn, valLength);
						--noBufferedValues;
						return (keyLength + valLength);
					}
				}
			}

			/// <summary>Read 'raw' keys.</summary>
			/// <param name="key">- The buffer into which the key is read</param>
			/// <returns>Returns the key length or -1 for end of file</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int nextRawKey(org.apache.hadoop.io.DataOutputBuffer key)
			{
				lock (this)
				{
					if (!blockCompressed)
					{
						recordLength = readRecordLength();
						if (recordLength == -1)
						{
							return -1;
						}
						keyLength = @in.readInt();
						key.write(@in, keyLength);
						return keyLength;
					}
					else
					{
						//Reset syncSeen
						syncSeen = false;
						// Read 'key'
						if (noBufferedKeys == 0)
						{
							if (@in.getPos() >= end)
							{
								return -1;
							}
							try
							{
								readBlock();
							}
							catch (java.io.EOFException)
							{
								return -1;
							}
						}
						int keyLength = org.apache.hadoop.io.WritableUtils.readVInt(keyLenIn);
						if (keyLength < 0)
						{
							throw new System.IO.IOException("zero length key found!");
						}
						key.write(keyIn, keyLength);
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
			public virtual object next(object key)
			{
				lock (this)
				{
					if (key != null && Sharpen.Runtime.getClassForObject(key) != getKeyClass())
					{
						throw new System.IO.IOException("wrong key class: " + Sharpen.Runtime.getClassForObject
							(key).getName() + " is not " + keyClass);
					}
					if (!blockCompressed)
					{
						outBuf.reset();
						keyLength = next(outBuf);
						if (keyLength < 0)
						{
							return null;
						}
						valBuffer.reset(outBuf.getData(), outBuf.getLength());
						key = deserializeKey(key);
						valBuffer.mark(0);
						if (valBuffer.getPosition() != keyLength)
						{
							throw new System.IO.IOException(key + " read " + valBuffer.getPosition() + " bytes, should read "
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
								readBlock();
							}
							catch (java.io.EOFException)
							{
								return null;
							}
						}
						int keyLength = org.apache.hadoop.io.WritableUtils.readVInt(keyLenIn);
						// Sanity check
						if (keyLength < 0)
						{
							return null;
						}
						//Read another compressed 'key'
						key = deserializeKey(key);
						--noBufferedKeys;
					}
					return key;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private object deserializeKey(object key)
			{
				return keyDeserializer.deserialize(key);
			}

			/// <summary>Read 'raw' values.</summary>
			/// <param name="val">- The 'raw' value</param>
			/// <returns>Returns the value length</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual int nextRawValue(org.apache.hadoop.io.SequenceFile.ValueBytes val)
			{
				lock (this)
				{
					// Position stream to current value
					seekToCurrentValue();
					if (!blockCompressed)
					{
						int valLength = recordLength - keyLength;
						if (decompress)
						{
							org.apache.hadoop.io.SequenceFile.CompressedBytes value = (org.apache.hadoop.io.SequenceFile.CompressedBytes
								)val;
							value.reset(@in, valLength);
						}
						else
						{
							org.apache.hadoop.io.SequenceFile.UncompressedBytes value = (org.apache.hadoop.io.SequenceFile.UncompressedBytes
								)val;
							value.reset(@in, valLength);
						}
						return valLength;
					}
					else
					{
						int valLength = org.apache.hadoop.io.WritableUtils.readVInt(valLenIn);
						org.apache.hadoop.io.SequenceFile.UncompressedBytes rawValue = (org.apache.hadoop.io.SequenceFile.UncompressedBytes
							)val;
						rawValue.reset(valIn, valLength);
						--noBufferedValues;
						return valLength;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void handleChecksumException(org.apache.hadoop.fs.ChecksumException e)
			{
				if (this.conf.getBoolean("io.skip.checksum.errors", false))
				{
					LOG.warn("Bad checksum at " + getPosition() + ". Skipping entries.");
					sync(getPosition() + this.conf.getInt("io.bytes.per.checksum", 512));
				}
				else
				{
					throw e;
				}
			}

			/// <summary>disables sync.</summary>
			/// <remarks>disables sync. often invoked for tmp files</remarks>
			internal virtual void ignoreSync()
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
			/// <see cref="Writer.getLength()"/>
			/// when writing this file.  To seek to an arbitrary
			/// position, use
			/// <see cref="sync(long)"/>
			/// .
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void seek(long position)
			{
				lock (this)
				{
					@in.seek(position);
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
			public virtual void sync(long position)
			{
				lock (this)
				{
					if (position + SYNC_SIZE >= end)
					{
						seek(end);
						return;
					}
					if (position < headerEnd)
					{
						// seek directly to first record
						@in.seek(headerEnd);
						// note the sync marker "seen" in the header
						syncSeen = true;
						return;
					}
					try
					{
						seek(position + 4);
						// skip escape
						@in.readFully(syncCheck);
						int syncLen = sync.Length;
						for (int i = 0; @in.getPos() < end; i++)
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
								@in.seek(@in.getPos() - SYNC_SIZE);
								// position before sync
								return;
							}
							syncCheck[i % syncLen] = @in.readByte();
						}
					}
					catch (org.apache.hadoop.fs.ChecksumException e)
					{
						// checksum failure
						handleChecksumException(e);
					}
				}
			}

			/// <summary>Returns true iff the previous call to next passed a sync mark.</summary>
			public virtual bool syncSeen()
			{
				lock (this)
				{
					return syncSeen;
				}
			}

			/// <summary>Return the current byte position in the input file.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual long getPosition()
			{
				lock (this)
				{
					return @in.getPos();
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
		/// <see cref="Writable.readFields(java.io.DataInput)"/>
		/// implementation of their keys is
		/// very efficient.  In particular, it should avoid allocating memory.
		/// </remarks>
		public class Sorter
		{
			private org.apache.hadoop.io.RawComparator comparator;

			private org.apache.hadoop.util.MergeSort mergeSort;

			private org.apache.hadoop.fs.Path[] inFiles;

			private org.apache.hadoop.fs.Path outFile;

			private int memory;

			private int factor;

			private org.apache.hadoop.fs.FileSystem fs = null;

			private java.lang.Class keyClass;

			private java.lang.Class valClass;

			private org.apache.hadoop.conf.Configuration conf;

			private org.apache.hadoop.io.SequenceFile.Metadata metadata;

			private org.apache.hadoop.util.Progressable progressable = null;

			/// <summary>Sort and merge files containing the named classes.</summary>
			public Sorter(org.apache.hadoop.fs.FileSystem fs, java.lang.Class keyClass, java.lang.Class
				 valClass, org.apache.hadoop.conf.Configuration conf)
				: this(fs, org.apache.hadoop.io.WritableComparator.get(keyClass, conf), keyClass, 
					valClass, conf)
			{
			}

			/// <summary>
			/// Sort and merge using an arbitrary
			/// <see cref="RawComparator{T}"/>
			/// .
			/// </summary>
			public Sorter(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RawComparator
				 comparator, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.conf.Configuration
				 conf)
				: this(fs, comparator, keyClass, valClass, conf, new org.apache.hadoop.io.SequenceFile.Metadata
					())
			{
			}

			/// <summary>
			/// Sort and merge using an arbitrary
			/// <see cref="RawComparator{T}"/>
			/// .
			/// </summary>
			public Sorter(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.io.RawComparator
				 comparator, java.lang.Class keyClass, java.lang.Class valClass, org.apache.hadoop.conf.Configuration
				 conf, org.apache.hadoop.io.SequenceFile.Metadata metadata)
			{
				//the implementation of merge sort
				// when merging or sorting
				// bytes
				// merged per pass
				this.fs = fs;
				this.comparator = comparator;
				this.keyClass = keyClass;
				this.valClass = valClass;
				this.memory = conf.getInt("io.sort.mb", 100) * 1024 * 1024;
				this.factor = conf.getInt("io.sort.factor", 100);
				this.conf = conf;
				this.metadata = metadata;
			}

			/// <summary>Set the number of streams to merge at once.</summary>
			public virtual void setFactor(int factor)
			{
				this.factor = factor;
			}

			/// <summary>Get the number of streams to merge at once.</summary>
			public virtual int getFactor()
			{
				return factor;
			}

			/// <summary>Set the total amount of buffer memory, in bytes.</summary>
			public virtual void setMemory(int memory)
			{
				this.memory = memory;
			}

			/// <summary>Get the total amount of buffer memory, in bytes.</summary>
			public virtual int getMemory()
			{
				return memory;
			}

			/// <summary>Set the progressable object in order to report progress.</summary>
			public virtual void setProgressable(org.apache.hadoop.util.Progressable progressable
				)
			{
				this.progressable = progressable;
			}

			/// <summary>Perform a file sort from a set of input files into an output file.</summary>
			/// <param name="inFiles">the files to be sorted</param>
			/// <param name="outFile">the sorted output file</param>
			/// <param name="deleteInput">should the input files be deleted as they are read?</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void sort(org.apache.hadoop.fs.Path[] inFiles, org.apache.hadoop.fs.Path
				 outFile, bool deleteInput)
			{
				if (fs.exists(outFile))
				{
					throw new System.IO.IOException("already exists: " + outFile);
				}
				this.inFiles = inFiles;
				this.outFile = outFile;
				int segments = sortPass(deleteInput);
				if (segments > 1)
				{
					mergePass(outFile.getParent());
				}
			}

			/// <summary>Perform a file sort from a set of input files and return an iterator.</summary>
			/// <param name="inFiles">the files to be sorted</param>
			/// <param name="tempDir">the directory where temp files are created during sort</param>
			/// <param name="deleteInput">should the input files be deleted as they are read?</param>
			/// <returns>iterator the RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator sortAndIterate
				(org.apache.hadoop.fs.Path[] inFiles, org.apache.hadoop.fs.Path tempDir, bool deleteInput
				)
			{
				org.apache.hadoop.fs.Path outFile = new org.apache.hadoop.fs.Path(tempDir + org.apache.hadoop.fs.Path
					.SEPARATOR + "all.2");
				if (fs.exists(outFile))
				{
					throw new System.IO.IOException("already exists: " + outFile);
				}
				this.inFiles = inFiles;
				//outFile will basically be used as prefix for temp files in the cases
				//where sort outputs multiple sorted segments. For the single segment
				//case, the outputFile itself will contain the sorted data for that
				//segment
				this.outFile = outFile;
				int segments = sortPass(deleteInput);
				if (segments > 1)
				{
					return merge(outFile.suffix(".0"), outFile.suffix(".0.index"), tempDir);
				}
				else
				{
					if (segments == 1)
					{
						return merge(new org.apache.hadoop.fs.Path[] { outFile }, true, tempDir);
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
			public virtual void sort(org.apache.hadoop.fs.Path inFile, org.apache.hadoop.fs.Path
				 outFile)
			{
				sort(new org.apache.hadoop.fs.Path[] { inFile }, outFile, false);
			}

			/// <exception cref="System.IO.IOException"/>
			private int sortPass(bool deleteInput)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("running sort pass");
				}
				org.apache.hadoop.io.SequenceFile.Sorter.SortPass sortPass = new org.apache.hadoop.io.SequenceFile.Sorter.SortPass
					(this);
				// make the SortPass
				sortPass.setProgressable(progressable);
				mergeSort = new org.apache.hadoop.util.MergeSort(new org.apache.hadoop.io.SequenceFile.Sorter.SortPass.SeqFileComparator
					(this));
				try
				{
					return sortPass.run(deleteInput);
				}
				finally
				{
					// run it
					sortPass.close();
				}
			}

			private class SortPass
			{
				private int memoryLimit = this._enclosing.memory / 4;

				private int recordLimit = 1000000;

				private org.apache.hadoop.io.DataOutputBuffer rawKeys = new org.apache.hadoop.io.DataOutputBuffer
					();

				private byte[] rawBuffer;

				private int[] keyOffsets = new int[1024];

				private int[] pointers = new int[this.keyOffsets.Length];

				private int[] pointersCopy = new int[this.keyOffsets.Length];

				private int[] keyLengths = new int[this.keyOffsets.Length];

				private org.apache.hadoop.io.SequenceFile.ValueBytes[] rawValues = new org.apache.hadoop.io.SequenceFile.ValueBytes
					[this.keyOffsets.Length];

				private System.Collections.ArrayList segmentLengths = new System.Collections.ArrayList
					();

				private org.apache.hadoop.io.SequenceFile.Reader @in = null;

				private org.apache.hadoop.fs.FSDataOutputStream @out = null;

				private org.apache.hadoop.fs.FSDataOutputStream indexOut = null;

				private org.apache.hadoop.fs.Path outName;

				private org.apache.hadoop.util.Progressable progressable = null;

				// close it
				/// <exception cref="System.IO.IOException"/>
				public virtual int run(bool deleteInput)
				{
					int segments = 0;
					int currentFile = 0;
					bool atEof = (currentFile >= this._enclosing.inFiles.Length);
					org.apache.hadoop.io.SequenceFile.CompressionType compressionType;
					org.apache.hadoop.io.compress.CompressionCodec codec = null;
					this.segmentLengths.clear();
					if (atEof)
					{
						return 0;
					}
					// Initialize
					this.@in = new org.apache.hadoop.io.SequenceFile.Reader(this._enclosing.fs, this.
						_enclosing.inFiles[currentFile], this._enclosing.conf);
					compressionType = this.@in.getCompressionType();
					codec = this.@in.getCompressionCodec();
					for (int i = 0; i < this.rawValues.Length; ++i)
					{
						this.rawValues[i] = null;
					}
					while (!atEof)
					{
						int count = 0;
						int bytesProcessed = 0;
						this.rawKeys.reset();
						while (!atEof && bytesProcessed < this.memoryLimit && count < this.recordLimit)
						{
							// Read a record into buffer
							// Note: Attempt to re-use 'rawValue' as far as possible
							int keyOffset = this.rawKeys.getLength();
							org.apache.hadoop.io.SequenceFile.ValueBytes rawValue = (count == this.keyOffsets
								.Length || this.rawValues[count] == null) ? this.@in.createValueBytes() : this.rawValues
								[count];
							int recordLength = this.@in.nextRaw(this.rawKeys, rawValue);
							if (recordLength == -1)
							{
								this.@in.close();
								if (deleteInput)
								{
									this._enclosing.fs.delete(this._enclosing.inFiles[currentFile], true);
								}
								currentFile += 1;
								atEof = currentFile >= this._enclosing.inFiles.Length;
								if (!atEof)
								{
									this.@in = new org.apache.hadoop.io.SequenceFile.Reader(this._enclosing.fs, this.
										_enclosing.inFiles[currentFile], this._enclosing.conf);
								}
								else
								{
									this.@in = null;
								}
								continue;
							}
							int keyLength = this.rawKeys.getLength() - keyOffset;
							if (count == this.keyOffsets.Length)
							{
								this.grow();
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
						if (org.apache.hadoop.io.SequenceFile.LOG.isDebugEnabled())
						{
							org.apache.hadoop.io.SequenceFile.LOG.debug("flushing segment " + segments);
						}
						this.rawBuffer = this.rawKeys.getData();
						this.sort(count);
						// indicate we're making progress
						if (this.progressable != null)
						{
							this.progressable.progress();
						}
						this.flush(count, bytesProcessed, compressionType, codec, segments == 0 && atEof);
						segments++;
					}
					return segments;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void close()
				{
					if (this.@in != null)
					{
						this.@in.close();
					}
					if (this.@out != null)
					{
						this.@out.close();
					}
					if (this.indexOut != null)
					{
						this.indexOut.close();
					}
				}

				private void grow()
				{
					int newLength = this.keyOffsets.Length * 3 / 2;
					this.keyOffsets = this.grow(this.keyOffsets, newLength);
					this.pointers = this.grow(this.pointers, newLength);
					this.pointersCopy = new int[newLength];
					this.keyLengths = this.grow(this.keyLengths, newLength);
					this.rawValues = this.grow(this.rawValues, newLength);
				}

				private int[] grow(int[] old, int newLength)
				{
					int[] result = new int[newLength];
					System.Array.Copy(old, 0, result, 0, old.Length);
					return result;
				}

				private org.apache.hadoop.io.SequenceFile.ValueBytes[] grow(org.apache.hadoop.io.SequenceFile.ValueBytes
					[] old, int newLength)
				{
					org.apache.hadoop.io.SequenceFile.ValueBytes[] result = new org.apache.hadoop.io.SequenceFile.ValueBytes
						[newLength];
					System.Array.Copy(old, 0, result, 0, old.Length);
					for (int i = old.Length; i < newLength; ++i)
					{
						result[i] = null;
					}
					return result;
				}

				/// <exception cref="System.IO.IOException"/>
				private void flush(int count, int bytesProcessed, org.apache.hadoop.io.SequenceFile.CompressionType
					 compressionType, org.apache.hadoop.io.compress.CompressionCodec codec, bool done
					)
				{
					if (this.@out == null)
					{
						this.outName = done ? this._enclosing.outFile : this._enclosing.outFile.suffix(".0"
							);
						this.@out = this._enclosing.fs.create(this.outName);
						if (!done)
						{
							this.indexOut = this._enclosing.fs.create(this.outName.suffix(".index"));
						}
					}
					long segmentStart = this.@out.getPos();
					org.apache.hadoop.io.SequenceFile.Writer writer = org.apache.hadoop.io.SequenceFile
						.createWriter(this._enclosing.conf, org.apache.hadoop.io.SequenceFile.Writer.stream
						(this.@out), org.apache.hadoop.io.SequenceFile.Writer.keyClass(this._enclosing.keyClass
						), org.apache.hadoop.io.SequenceFile.Writer.valueClass(this._enclosing.valClass)
						, org.apache.hadoop.io.SequenceFile.Writer.compression(compressionType, codec), 
						org.apache.hadoop.io.SequenceFile.Writer.metadata(done ? this._enclosing.metadata
						 : new org.apache.hadoop.io.SequenceFile.Metadata()));
					if (!done)
					{
						writer.sync = null;
					}
					// disable sync on temp files
					for (int i = 0; i < count; i++)
					{
						// write in sorted order
						int p = this.pointers[i];
						writer.appendRaw(this.rawBuffer, this.keyOffsets[p], this.keyLengths[p], this.rawValues
							[p]);
					}
					writer.close();
					if (!done)
					{
						// Save the segment length
						org.apache.hadoop.io.WritableUtils.writeVLong(this.indexOut, segmentStart);
						org.apache.hadoop.io.WritableUtils.writeVLong(this.indexOut, (this.@out.getPos() 
							- segmentStart));
						this.indexOut.flush();
					}
				}

				private void sort(int count)
				{
					System.Array.Copy(this.pointers, 0, this.pointersCopy, 0, count);
					this._enclosing.mergeSort.mergeSort(this.pointersCopy, this.pointers, 0, count);
				}

				internal class SeqFileComparator : java.util.Comparator<org.apache.hadoop.io.IntWritable
					>
				{
					public virtual int compare(org.apache.hadoop.io.IntWritable I, org.apache.hadoop.io.IntWritable
						 J)
					{
						return this._enclosing._enclosing.comparator.compare(this._enclosing.rawBuffer, this
							._enclosing.keyOffsets[I.get()], this._enclosing.keyLengths[I.get()], this._enclosing
							.rawBuffer, this._enclosing.keyOffsets[J.get()], this._enclosing.keyLengths[J.get
							()]);
					}

					internal SeqFileComparator(SortPass _enclosing)
					{
						this._enclosing = _enclosing;
					}

					private readonly SortPass _enclosing;
				}

				/// <summary>set the progressable object in order to report progress</summary>
				public virtual void setProgressable(org.apache.hadoop.util.Progressable progressable
					)
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
				org.apache.hadoop.io.DataOutputBuffer getKey();

				/// <summary>Gets the current raw value</summary>
				/// <returns>ValueBytes</returns>
				/// <exception cref="System.IO.IOException"/>
				org.apache.hadoop.io.SequenceFile.ValueBytes getValue();

				/// <summary>Sets up the current key and value (for getKey and getValue)</summary>
				/// <returns>true if there exists a key/value, false otherwise</returns>
				/// <exception cref="System.IO.IOException"/>
				bool next();

				/// <summary>closes the iterator so that the underlying streams can be closed</summary>
				/// <exception cref="System.IO.IOException"/>
				void close();

				/// <summary>
				/// Gets the Progress object; this has a float (0.0 - 1.0)
				/// indicating the bytes processed by the iterator so far
				/// </summary>
				org.apache.hadoop.util.Progress getProgress();
			}

			/// <summary>Merges the list of segments of type <code>SegmentDescriptor</code></summary>
			/// <param name="segments">the list of SegmentDescriptors</param>
			/// <param name="tmpDir">the directory to write temporary files into</param>
			/// <returns>RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge
				(System.Collections.Generic.IList<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
				> segments, org.apache.hadoop.fs.Path tmpDir)
			{
				// pass in object to report progress, if present
				org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue mQueue = new org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue
					(this, segments, tmpDir, progressable);
				return mQueue.merge();
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
			public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge
				(org.apache.hadoop.fs.Path[] inNames, bool deleteInputs, org.apache.hadoop.fs.Path
				 tmpDir)
			{
				return merge(inNames, deleteInputs, (inNames.Length < factor) ? inNames.Length : 
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
			public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge
				(org.apache.hadoop.fs.Path[] inNames, bool deleteInputs, int factor, org.apache.hadoop.fs.Path
				 tmpDir)
			{
				//get the segments from inNames
				System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					> a = new System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					>();
				for (int i = 0; i < inNames.Length; i++)
				{
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor s = new org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
						(this, 0, fs.getFileStatus(inNames[i]).getLen(), inNames[i]);
					s.preserveInput(!deleteInputs);
					s.doSync();
					a.add(s);
				}
				this.factor = factor;
				org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue mQueue = new org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue
					(this, a, tmpDir, progressable);
				return mQueue.merge();
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
			public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge
				(org.apache.hadoop.fs.Path[] inNames, org.apache.hadoop.fs.Path tempDir, bool deleteInputs
				)
			{
				//outFile will basically be used as prefix for temp files for the
				//intermediate merge outputs           
				this.outFile = new org.apache.hadoop.fs.Path(tempDir + org.apache.hadoop.fs.Path.
					SEPARATOR + "merged");
				//get the segments from inNames
				System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					> a = new System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					>();
				for (int i = 0; i < inNames.Length; i++)
				{
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor s = new org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
						(this, 0, fs.getFileStatus(inNames[i]).getLen(), inNames[i]);
					s.preserveInput(!deleteInputs);
					s.doSync();
					a.add(s);
				}
				factor = (inNames.Length < factor) ? inNames.Length : factor;
				// pass in object to report progress, if present
				org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue mQueue = new org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue
					(this, a, tempDir, progressable);
				return mQueue.merge();
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
			public virtual org.apache.hadoop.io.SequenceFile.Writer cloneFileAttributes(org.apache.hadoop.fs.Path
				 inputFile, org.apache.hadoop.fs.Path outputFile, org.apache.hadoop.util.Progressable
				 prog)
			{
				org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
					(conf, org.apache.hadoop.io.SequenceFile.Reader.file(inputFile), new org.apache.hadoop.io.SequenceFile.Reader.OnlyHeaderOption
					());
				org.apache.hadoop.io.SequenceFile.CompressionType compress = reader.getCompressionType
					();
				org.apache.hadoop.io.compress.CompressionCodec codec = reader.getCompressionCodec
					();
				reader.close();
				org.apache.hadoop.io.SequenceFile.Writer writer = createWriter(conf, org.apache.hadoop.io.SequenceFile.Writer
					.file(outputFile), org.apache.hadoop.io.SequenceFile.Writer.keyClass(keyClass), 
					org.apache.hadoop.io.SequenceFile.Writer.valueClass(valClass), org.apache.hadoop.io.SequenceFile.Writer
					.compression(compress, codec), org.apache.hadoop.io.SequenceFile.Writer.progressable
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
			public virtual void writeFile(org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator
				 records, org.apache.hadoop.io.SequenceFile.Writer writer)
			{
				while (records.next())
				{
					writer.appendRaw(records.getKey().getData(), 0, records.getKey().getLength(), records
						.getValue());
				}
				writer.sync();
			}

			/// <summary>Merge the provided files.</summary>
			/// <param name="inFiles">the array of input path names</param>
			/// <param name="outFile">the final output file</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void merge(org.apache.hadoop.fs.Path[] inFiles, org.apache.hadoop.fs.Path
				 outFile)
			{
				if (fs.exists(outFile))
				{
					throw new System.IO.IOException("already exists: " + outFile);
				}
				org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator r = merge(inFiles, false
					, outFile.getParent());
				org.apache.hadoop.io.SequenceFile.Writer writer = cloneFileAttributes(inFiles[0], 
					outFile, null);
				writeFile(r, writer);
				writer.close();
			}

			/// <summary>sort calls this to generate the final merged output</summary>
			/// <exception cref="System.IO.IOException"/>
			private int mergePass(org.apache.hadoop.fs.Path tmpDir)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("running merge pass");
				}
				org.apache.hadoop.io.SequenceFile.Writer writer = cloneFileAttributes(outFile.suffix
					(".0"), outFile, null);
				org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator r = merge(outFile.suffix
					(".0"), outFile.suffix(".0.index"), tmpDir);
				writeFile(r, writer);
				writer.close();
				return 0;
			}

			/// <summary>Used by mergePass to merge the output of the sort</summary>
			/// <param name="inName">the name of the input file containing sorted segments</param>
			/// <param name="indexIn">the offsets of the sorted segments</param>
			/// <param name="tmpDir">the relative directory to store intermediate results in</param>
			/// <returns>RawKeyValueIterator</returns>
			/// <exception cref="System.IO.IOException"/>
			private org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge(org.apache.hadoop.fs.Path
				 inName, org.apache.hadoop.fs.Path indexIn, org.apache.hadoop.fs.Path tmpDir)
			{
				//get the segments from indexIn
				//we create a SegmentContainer so that we can track segments belonging to
				//inName and delete inName as soon as we see that we have looked at all
				//the contained segments during the merge process & hence don't need 
				//them anymore
				org.apache.hadoop.io.SequenceFile.Sorter.SegmentContainer container = new org.apache.hadoop.io.SequenceFile.Sorter.SegmentContainer
					(this, inName, indexIn);
				org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue mQueue = new org.apache.hadoop.io.SequenceFile.Sorter.MergeQueue
					(this, container.getSegmentList(), tmpDir, progressable);
				return mQueue.merge();
			}

			/// <summary>This class implements the core of the merge logic</summary>
			private class MergeQueue : org.apache.hadoop.util.PriorityQueue, org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator
			{
				private bool compress;

				private bool blockCompress;

				private org.apache.hadoop.io.DataOutputBuffer rawKey = new org.apache.hadoop.io.DataOutputBuffer
					();

				private org.apache.hadoop.io.SequenceFile.ValueBytes rawValue;

				private long totalBytesProcessed;

				private float progPerByte;

				private org.apache.hadoop.util.Progress mergeProgress = new org.apache.hadoop.util.Progress
					();

				private org.apache.hadoop.fs.Path tmpDir;

				private org.apache.hadoop.util.Progressable progress = null;

				private org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor minSegment;

				private System.Collections.Generic.IDictionary<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					, java.lang.Void> sortedSegmentSizes = new System.Collections.Generic.SortedDictionary
					<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor, java.lang.Void>();

				//handle to the progress reporting object
				//a TreeMap used to store the segments sorted by size (segment offset and
				//segment path name is used to break ties between segments of same sizes)
				/// <exception cref="System.IO.IOException"/>
				public virtual void put(org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					 stream)
				{
					if (this.size() == 0)
					{
						this.compress = stream.@in.isCompressed();
						this.blockCompress = stream.@in.isBlockCompressed();
					}
					else
					{
						if (this.compress != stream.@in.isCompressed() || this.blockCompress != stream.@in
							.isBlockCompressed())
						{
							throw new System.IO.IOException("All merged files must be compressed or not.");
						}
					}
					base.put(stream);
				}

				/// <summary>A queue of file segments to merge</summary>
				/// <param name="segments">the file segments to merge</param>
				/// <param name="tmpDir">a relative local directory to save intermediate files in</param>
				/// <param name="progress">the reference to the Progressable object</param>
				public MergeQueue(Sorter _enclosing, System.Collections.Generic.IList<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					> segments, org.apache.hadoop.fs.Path tmpDir, org.apache.hadoop.util.Progressable
					 progress)
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

				protected internal override bool lessThan(object a, object b)
				{
					// indicate we're making progress
					if (this.progress != null)
					{
						this.progress.progress();
					}
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor msa = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
						)a;
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor msb = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
						)b;
					return this._enclosing.comparator.compare(msa.getKey().getData(), 0, msa.getKey()
						.getLength(), msb.getKey().getData(), 0, msb.getKey().getLength()) < 0;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void close()
				{
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor ms;
					// close inputs
					while ((ms = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor)this.pop
						()) != null)
					{
						ms.cleanup();
					}
					this.minSegment = null;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual org.apache.hadoop.io.DataOutputBuffer getKey()
				{
					return this.rawKey;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual org.apache.hadoop.io.SequenceFile.ValueBytes getValue()
				{
					return this.rawValue;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual bool next()
				{
					if (this.size() == 0)
					{
						return false;
					}
					if (this.minSegment != null)
					{
						//minSegment is non-null for all invocations of next except the first
						//one. For the first invocation, the priority queue is ready for use
						//but for the subsequent invocations, first adjust the queue 
						this.adjustPriorityQueue(this.minSegment);
						if (this.size() == 0)
						{
							this.minSegment = null;
							return false;
						}
					}
					this.minSegment = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor)this
						.top();
					long startPos = this.minSegment.@in.getPosition();
					// Current position in stream
					//save the raw key reference
					this.rawKey = this.minSegment.getKey();
					//load the raw value. Re-use the existing rawValue buffer
					if (this.rawValue == null)
					{
						this.rawValue = this.minSegment.@in.createValueBytes();
					}
					this.minSegment.nextRawValue(this.rawValue);
					long endPos = this.minSegment.@in.getPosition();
					// End position after reading value
					this.updateProgress(endPos - startPos);
					return true;
				}

				public virtual org.apache.hadoop.util.Progress getProgress()
				{
					return this.mergeProgress;
				}

				/// <exception cref="System.IO.IOException"/>
				private void adjustPriorityQueue(org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					 ms)
				{
					long startPos = ms.@in.getPosition();
					// Current position in stream
					bool hasNext = ms.nextRawKey();
					long endPos = ms.@in.getPosition();
					// End position after reading key
					this.updateProgress(endPos - startPos);
					if (hasNext)
					{
						this.adjustTop();
					}
					else
					{
						this.pop();
						ms.cleanup();
					}
				}

				private void updateProgress(long bytesProcessed)
				{
					this.totalBytesProcessed += bytesProcessed;
					if (this.progPerByte > 0)
					{
						this.mergeProgress.set(this.totalBytesProcessed * this.progPerByte);
					}
				}

				/// <summary>
				/// This is the single level merge that is called multiple times
				/// depending on the factor size and the number of segments
				/// </summary>
				/// <returns>RawKeyValueIterator</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator merge
					()
				{
					//create the MergeStreams from the sorted map created in the constructor
					//and dump the final output to a file
					int numSegments = this.sortedSegmentSizes.Count;
					int origFactor = this._enclosing.factor;
					int passNo = 1;
					org.apache.hadoop.fs.LocalDirAllocator lDirAlloc = new org.apache.hadoop.fs.LocalDirAllocator
						("io.seqfile.local.dir");
					do
					{
						//get the factor for this pass of merge
						this._enclosing.factor = this.getPassFactor(passNo, numSegments);
						System.Collections.Generic.IList<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
							> segmentsToMerge = new System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
							>();
						int segmentsConsidered = 0;
						int numSegmentsToConsider = this._enclosing.factor;
						while (true)
						{
							//extract the smallest 'factor' number of segment pointers from the 
							//TreeMap. Call cleanup on the empty segments (no key/value data)
							org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor[] mStream = this.getSegmentDescriptors
								(numSegmentsToConsider);
							for (int i = 0; i < mStream.Length; i++)
							{
								if (mStream[i].nextRawKey())
								{
									segmentsToMerge.add(mStream[i]);
									segmentsConsidered++;
									// Count the fact that we read some bytes in calling nextRawKey()
									this.updateProgress(mStream[i].@in.getPosition());
								}
								else
								{
									mStream[i].cleanup();
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
						this.initialize(segmentsToMerge.Count);
						this.clear();
						for (int i_1 = 0; i_1 < segmentsToMerge.Count; i_1++)
						{
							this.put(segmentsToMerge[i_1]);
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
							foreach (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor s in segmentsToMerge)
							{
								approxOutputSize += s.segmentLength + org.apache.hadoop.fs.ChecksumFileSystem.getApproxChkSumLength
									(s.segmentLength);
							}
							org.apache.hadoop.fs.Path tmpFilename = new org.apache.hadoop.fs.Path(this.tmpDir
								, "intermediate").suffix("." + passNo);
							org.apache.hadoop.fs.Path outputFile = lDirAlloc.getLocalPathForWrite(tmpFilename
								.ToString(), approxOutputSize, this._enclosing.conf);
							if (org.apache.hadoop.io.SequenceFile.LOG.isDebugEnabled())
							{
								org.apache.hadoop.io.SequenceFile.LOG.debug("writing intermediate results to " + 
									outputFile);
							}
							org.apache.hadoop.io.SequenceFile.Writer writer = this._enclosing.cloneFileAttributes
								(this._enclosing.fs.makeQualified(segmentsToMerge[0].segmentPathName), this._enclosing
								.fs.makeQualified(outputFile), null);
							writer.sync = null;
							//disable sync for temp files
							this._enclosing.writeFile(this, writer);
							writer.close();
							//we finished one single level merge; now clean up the priority 
							//queue
							this.close();
							org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor tempSegment = new org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
								(this, 0, this._enclosing.fs.getFileStatus(outputFile).getLen(), outputFile);
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
				public virtual int getPassFactor(int passNo, int numSegments)
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
				public virtual org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor[] getSegmentDescriptors
					(int numDescriptors)
				{
					if (numDescriptors > this.sortedSegmentSizes.Count)
					{
						numDescriptors = this.sortedSegmentSizes.Count;
					}
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor[] SegmentDescriptors = 
						new org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor[numDescriptors];
					System.Collections.IEnumerator iter = this.sortedSegmentSizes.Keys.GetEnumerator(
						);
					int i = 0;
					while (i < numDescriptors)
					{
						SegmentDescriptors[i++] = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
							)iter.Current;
						iter.remove();
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
			public class SegmentDescriptor : java.lang.Comparable
			{
				internal long segmentOffset;

				internal long segmentLength;

				internal org.apache.hadoop.fs.Path segmentPathName;

				internal bool ignoreSync = true;

				private org.apache.hadoop.io.SequenceFile.Reader @in = null;

				private org.apache.hadoop.io.DataOutputBuffer rawKey = null;

				private bool preserveInput = false;

				/// <summary>Constructs a segment</summary>
				/// <param name="segmentOffset">the offset of the segment in the file</param>
				/// <param name="segmentLength">the length of the segment</param>
				/// <param name="segmentPathName">the path name of the file containing the segment</param>
				public SegmentDescriptor(Sorter _enclosing, long segmentOffset, long segmentLength
					, org.apache.hadoop.fs.Path segmentPathName)
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
				public virtual void doSync()
				{
					this.ignoreSync = false;
				}

				/// <summary>Whether to delete the files when no longer needed</summary>
				public virtual void preserveInput(bool preserve)
				{
					this.preserveInput = preserve;
				}

				public virtual bool shouldPreserveInput()
				{
					return this.preserveInput;
				}

				public virtual int compareTo(object o)
				{
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor that = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
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
					if (!(o is org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor))
					{
						return false;
					}
					org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor that = (org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
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
				public virtual bool nextRawKey()
				{
					if (this.@in == null)
					{
						int bufferSize = org.apache.hadoop.io.SequenceFile.getBufferSize(this._enclosing.
							conf);
						org.apache.hadoop.io.SequenceFile.Reader reader = new org.apache.hadoop.io.SequenceFile.Reader
							(this._enclosing.conf, org.apache.hadoop.io.SequenceFile.Reader.file(this.segmentPathName
							), org.apache.hadoop.io.SequenceFile.Reader.bufferSize(bufferSize), org.apache.hadoop.io.SequenceFile.Reader
							.start(this.segmentOffset), org.apache.hadoop.io.SequenceFile.Reader.length(this
							.segmentLength));
						//sometimes we ignore syncs especially for temp merge files
						if (this.ignoreSync)
						{
							reader.ignoreSync();
						}
						if (reader.getKeyClass() != this._enclosing.keyClass)
						{
							throw new System.IO.IOException("wrong key class: " + reader.getKeyClass() + " is not "
								 + this._enclosing.keyClass);
						}
						if (reader.getValueClass() != this._enclosing.valClass)
						{
							throw new System.IO.IOException("wrong value class: " + reader.getValueClass() + 
								" is not " + this._enclosing.valClass);
						}
						this.@in = reader;
						this.rawKey = new org.apache.hadoop.io.DataOutputBuffer();
					}
					this.rawKey.reset();
					int keyLength = this.@in.nextRawKey(this.rawKey);
					return (keyLength >= 0);
				}

				/// <summary>
				/// Fills up the passed rawValue with the value corresponding to the key
				/// read earlier
				/// </summary>
				/// <param name="rawValue"/>
				/// <returns>the length of the value</returns>
				/// <exception cref="System.IO.IOException"/>
				public virtual int nextRawValue(org.apache.hadoop.io.SequenceFile.ValueBytes rawValue
					)
				{
					int valLength = this.@in.nextRawValue(rawValue);
					return valLength;
				}

				/// <summary>Returns the stored rawKey</summary>
				public virtual org.apache.hadoop.io.DataOutputBuffer getKey()
				{
					return this.rawKey;
				}

				/// <summary>closes the underlying reader</summary>
				/// <exception cref="System.IO.IOException"/>
				private void close()
				{
					this.@in.close();
					this.@in = null;
				}

				/// <summary>The default cleanup.</summary>
				/// <remarks>
				/// The default cleanup. Subclasses can override this with a custom
				/// cleanup
				/// </remarks>
				/// <exception cref="System.IO.IOException"/>
				public virtual void cleanup()
				{
					this.close();
					if (!this.preserveInput)
					{
						this._enclosing.fs.delete(this.segmentPathName, true);
					}
				}

				private readonly Sorter _enclosing;
			}

			/// <summary>
			/// This class provisions multiple segments contained within a single
			/// file
			/// </summary>
			private class LinkedSegmentsDescriptor : org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
			{
				internal org.apache.hadoop.io.SequenceFile.Sorter.SegmentContainer parentContainer
					 = null;

				/// <summary>Constructs a segment</summary>
				/// <param name="segmentOffset">the offset of the segment in the file</param>
				/// <param name="segmentLength">the length of the segment</param>
				/// <param name="segmentPathName">the path name of the file containing the segment</param>
				/// <param name="parent">the parent SegmentContainer that holds the segment</param>
				public LinkedSegmentsDescriptor(Sorter _enclosing, long segmentOffset, long segmentLength
					, org.apache.hadoop.fs.Path segmentPathName, org.apache.hadoop.io.SequenceFile.Sorter.SegmentContainer
					 parent)
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
				public override void cleanup()
				{
					base.close();
					if (base.shouldPreserveInput())
					{
						return;
					}
					this.parentContainer.cleanup();
				}

				public override bool Equals(object o)
				{
					if (!(o is org.apache.hadoop.io.SequenceFile.Sorter.LinkedSegmentsDescriptor))
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

				private org.apache.hadoop.fs.Path inName;

				private System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					> segments = new System.Collections.Generic.List<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					>();

				/// <summary>
				/// This constructor is there primarily to serve the sort routine that
				/// generates a single output file with an associated index file
				/// </summary>
				/// <exception cref="System.IO.IOException"/>
				public SegmentContainer(Sorter _enclosing, org.apache.hadoop.fs.Path inName, org.apache.hadoop.fs.Path
					 indexIn)
				{
					this._enclosing = _enclosing;
					//SequenceFile.Sorter.LinkedSegmentsDescriptor
					//track the no. of segment cleanups
					//# of segments contained
					//input file from where segments are created
					//the list of segments read from the file
					//get the segments from indexIn
					org.apache.hadoop.fs.FSDataInputStream fsIndexIn = this._enclosing.fs.open(indexIn
						);
					long end = this._enclosing.fs.getFileStatus(indexIn).getLen();
					while (fsIndexIn.getPos() < end)
					{
						long segmentOffset = org.apache.hadoop.io.WritableUtils.readVLong(fsIndexIn);
						long segmentLength = org.apache.hadoop.io.WritableUtils.readVLong(fsIndexIn);
						org.apache.hadoop.fs.Path segmentName = inName;
						this.segments.add(new org.apache.hadoop.io.SequenceFile.Sorter.LinkedSegmentsDescriptor
							(this, segmentOffset, segmentLength, segmentName, this));
					}
					fsIndexIn.close();
					this._enclosing.fs.delete(indexIn, true);
					this.numSegmentsContained = this.segments.Count;
					this.inName = inName;
				}

				public virtual System.Collections.Generic.IList<org.apache.hadoop.io.SequenceFile.Sorter.SegmentDescriptor
					> getSegmentList()
				{
					return this.segments;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void cleanup()
				{
					this.numSegmentsCleanedUp++;
					if (this.numSegmentsCleanedUp == this.numSegmentsContained)
					{
						this._enclosing.fs.delete(this.inName, true);
					}
				}

				private readonly Sorter _enclosing;
			}
			//SequenceFile.Sorter.SegmentContainer
		}
		// SequenceFile.Sorter
	}
}
