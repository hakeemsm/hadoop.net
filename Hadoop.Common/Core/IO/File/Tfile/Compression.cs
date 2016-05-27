using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Compression related stuff.</summary>
	internal sealed class Compression
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.IO.File.Tfile.Compression
			));

		/// <summary>Prevent the instantiation of class.</summary>
		private Compression()
		{
		}

		internal class FinishOnFlushCompressionStream : FilterOutputStream
		{
			public FinishOnFlushCompressionStream(CompressionOutputStream cout)
				: base(cout)
			{
			}

			// nothing
			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				@out.Write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				CompressionOutputStream cout = (CompressionOutputStream)@out;
				cout.Finish();
				cout.Flush();
				cout.ResetState();
			}
		}

		/// <summary>Compression algorithms.</summary>
		[System.Serializable]
		internal sealed class Algorithm
		{
			public static readonly Compression.Algorithm Lzo = new Compression.Algorithm(TFile
				.CompressionLzo);

			public static readonly Compression.Algorithm Gz = new Compression.Algorithm(TFile
				.CompressionGz);

			public static readonly Compression.Algorithm None = new Compression.Algorithm(TFile
				.CompressionNone);

			protected internal static readonly Configuration conf = new Configuration();

			private readonly string compressName;

			private const int DataIbufSize = 1 * 1024;

			private const int DataObufSize = 4 * 1024;

			public const string ConfLzoClass = "io.compression.codec.lzo.class";

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
			internal abstract CompressionCodec GetCodec();

			/// <exception cref="System.IO.IOException"/>
			public abstract InputStream CreateDecompressionStream(InputStream downStream, Decompressor
				 decompressor, int downStreamBufferSize);

			/// <exception cref="System.IO.IOException"/>
			public abstract OutputStream CreateCompressionStream(OutputStream downStream, Compressor
				 compressor, int downStreamBufferSize);

			public abstract bool IsSupported();

			/// <exception cref="System.IO.IOException"/>
			public Compressor GetCompressor()
			{
				CompressionCodec codec = GetCodec();
				if (codec != null)
				{
					Compressor compressor = CodecPool.GetCompressor(codec);
					if (compressor != null)
					{
						if (compressor.Finished())
						{
							// Somebody returns the compressor to CodecPool but is still using
							// it.
							Log.Warn("Compressor obtained from CodecPool already finished()");
						}
						else
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Got a compressor: " + compressor.GetHashCode());
							}
						}
						compressor.Reset();
					}
					return compressor;
				}
				return null;
			}

			public void ReturnCompressor(Compressor compressor)
			{
				if (compressor != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Return a compressor: " + compressor.GetHashCode());
					}
					CodecPool.ReturnCompressor(compressor);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public Decompressor GetDecompressor()
			{
				CompressionCodec codec = GetCodec();
				if (codec != null)
				{
					Decompressor decompressor = CodecPool.GetDecompressor(codec);
					if (decompressor != null)
					{
						if (decompressor.Finished())
						{
							// Somebody returns the decompressor to CodecPool but is still using
							// it.
							Log.Warn("Deompressor obtained from CodecPool already finished()");
						}
						else
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Got a decompressor: " + decompressor.GetHashCode());
							}
						}
						decompressor.Reset();
					}
					return decompressor;
				}
				return null;
			}

			public void ReturnDecompressor(Decompressor decompressor)
			{
				if (decompressor != null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Returned a decompressor: " + decompressor.GetHashCode());
					}
					CodecPool.ReturnDecompressor(decompressor);
				}
			}

			public string GetName()
			{
				return Compression.Algorithm.compressName;
			}
		}

		internal static Compression.Algorithm GetCompressionAlgorithmByName(string compressName
			)
		{
			Compression.Algorithm[] algos = typeof(Compression.Algorithm).GetEnumConstants();
			foreach (Compression.Algorithm a in algos)
			{
				if (a.GetName().Equals(compressName))
				{
					return a;
				}
			}
			throw new ArgumentException("Unsupported compression algorithm name: " + compressName
				);
		}

		internal static string[] GetSupportedAlgorithms()
		{
			Compression.Algorithm[] algos = typeof(Compression.Algorithm).GetEnumConstants();
			AList<string> ret = new AList<string>();
			foreach (Compression.Algorithm a in algos)
			{
				if (a.IsSupported())
				{
					ret.AddItem(a.GetName());
				}
			}
			return Sharpen.Collections.ToArray(ret, new string[ret.Count]);
		}
	}
}
