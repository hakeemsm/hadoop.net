using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Compression related stuff.</summary>
	internal sealed class Compression
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.file.tfile.Compression
			)));

		/// <summary>Prevent the instantiation of class.</summary>
		private Compression()
		{
		}

		internal class FinishOnFlushCompressionStream : java.io.FilterOutputStream
		{
			public FinishOnFlushCompressionStream(org.apache.hadoop.io.compress.CompressionOutputStream
				 cout)
				: base(cout)
			{
			}

			// nothing
			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				@out.write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				org.apache.hadoop.io.compress.CompressionOutputStream cout = (org.apache.hadoop.io.compress.CompressionOutputStream
					)@out;
				cout.finish();
				cout.flush();
				cout.resetState();
			}
		}

		/// <summary>Compression algorithms.</summary>
		[System.Serializable]
		internal sealed class Algorithm
		{
			public static readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm LZO = 
				new org.apache.hadoop.io.file.tfile.Compression.Algorithm(org.apache.hadoop.io.file.tfile.TFile
				.COMPRESSION_LZO);

			public static readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm GZ = 
				new org.apache.hadoop.io.file.tfile.Compression.Algorithm(org.apache.hadoop.io.file.tfile.TFile
				.COMPRESSION_GZ);

			public static readonly org.apache.hadoop.io.file.tfile.Compression.Algorithm NONE
				 = new org.apache.hadoop.io.file.tfile.Compression.Algorithm(org.apache.hadoop.io.file.tfile.TFile
				.COMPRESSION_NONE);

			protected internal static readonly org.apache.hadoop.conf.Configuration conf = new 
				org.apache.hadoop.conf.Configuration();

			private readonly string compressName;

			private const int DATA_IBUF_SIZE = 1 * 1024;

			private const int DATA_OBUF_SIZE = 4 * 1024;

			public const string CONF_LZO_CLASS = "io.compression.codec.lzo.class";

			internal Algorithm(string name)
			{
				// that is okay
				// Set the internal buffer size to read from down stream.
				// We require that all compression related settings are configured
				// statically in the Configuration object.
				// data input buffer size to absorb small reads from application.
				// data output buffer size to absorb small writes from application.
				this.compressName = name;
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract org.apache.hadoop.io.compress.CompressionCodec getCodec();

			/// <exception cref="System.IO.IOException"/>
			public abstract java.io.InputStream createDecompressionStream(java.io.InputStream
				 downStream, org.apache.hadoop.io.compress.Decompressor decompressor, int downStreamBufferSize
				);

			/// <exception cref="System.IO.IOException"/>
			public abstract java.io.OutputStream createCompressionStream(java.io.OutputStream
				 downStream, org.apache.hadoop.io.compress.Compressor compressor, int downStreamBufferSize
				);

			public abstract bool isSupported();

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.io.compress.Compressor getCompressor()
			{
				org.apache.hadoop.io.compress.CompressionCodec codec = getCodec();
				if (codec != null)
				{
					org.apache.hadoop.io.compress.Compressor compressor = org.apache.hadoop.io.compress.CodecPool
						.getCompressor(codec);
					if (compressor != null)
					{
						if (compressor.finished())
						{
							// Somebody returns the compressor to CodecPool but is still using
							// it.
							LOG.warn("Compressor obtained from CodecPool already finished()");
						}
						else
						{
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Got a compressor: " + compressor.GetHashCode());
							}
						}
						compressor.reset();
					}
					return compressor;
				}
				return null;
			}

			public void returnCompressor(org.apache.hadoop.io.compress.Compressor compressor)
			{
				if (compressor != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Return a compressor: " + compressor.GetHashCode());
					}
					org.apache.hadoop.io.compress.CodecPool.returnCompressor(compressor);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.io.compress.Decompressor getDecompressor()
			{
				org.apache.hadoop.io.compress.CompressionCodec codec = getCodec();
				if (codec != null)
				{
					org.apache.hadoop.io.compress.Decompressor decompressor = org.apache.hadoop.io.compress.CodecPool
						.getDecompressor(codec);
					if (decompressor != null)
					{
						if (decompressor.finished())
						{
							// Somebody returns the decompressor to CodecPool but is still using
							// it.
							LOG.warn("Deompressor obtained from CodecPool already finished()");
						}
						else
						{
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Got a decompressor: " + decompressor.GetHashCode());
							}
						}
						decompressor.reset();
					}
					return decompressor;
				}
				return null;
			}

			public void returnDecompressor(org.apache.hadoop.io.compress.Decompressor decompressor
				)
			{
				if (decompressor != null)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Returned a decompressor: " + decompressor.GetHashCode());
					}
					org.apache.hadoop.io.compress.CodecPool.returnDecompressor(decompressor);
				}
			}

			public string getName()
			{
				return org.apache.hadoop.io.file.tfile.Compression.Algorithm.compressName;
			}
		}

		internal static org.apache.hadoop.io.file.tfile.Compression.Algorithm getCompressionAlgorithmByName
			(string compressName)
		{
			org.apache.hadoop.io.file.tfile.Compression.Algorithm[] algos = Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.file.tfile.Compression.Algorithm)).getEnumConstants
				();
			foreach (org.apache.hadoop.io.file.tfile.Compression.Algorithm a in algos)
			{
				if (a.getName().Equals(compressName))
				{
					return a;
				}
			}
			throw new System.ArgumentException("Unsupported compression algorithm name: " + compressName
				);
		}

		internal static string[] getSupportedAlgorithms()
		{
			org.apache.hadoop.io.file.tfile.Compression.Algorithm[] algos = Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.file.tfile.Compression.Algorithm)).getEnumConstants
				();
			System.Collections.Generic.List<string> ret = new System.Collections.Generic.List
				<string>();
			foreach (org.apache.hadoop.io.file.tfile.Compression.Algorithm a in algos)
			{
				if (a.isSupported())
				{
					ret.add(a.getName());
				}
			}
			return Sharpen.Collections.ToArray(ret, new string[ret.Count]);
		}
	}
}
