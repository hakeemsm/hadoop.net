using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Simple container class that handles support for compressed fsimage files.
	/// 	</summary>
	internal class FSImageCompression
	{
		/// <summary>Codec to use to save or load image, or null if the image is not compressed
		/// 	</summary>
		private CompressionCodec imageCodec;

		/// <summary>Create a "noop" compression - i.e.</summary>
		/// <remarks>Create a "noop" compression - i.e. uncompressed</remarks>
		private FSImageCompression()
		{
		}

		/// <summary>Create compression using a particular codec</summary>
		private FSImageCompression(CompressionCodec codec)
		{
			imageCodec = codec;
		}

		public virtual CompressionCodec GetImageCodec()
		{
			return imageCodec;
		}

		/// <summary>Create a "noop" compression - i.e.</summary>
		/// <remarks>Create a "noop" compression - i.e. uncompressed</remarks>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression CreateNoopCompression
			()
		{
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression();
		}

		/// <summary>
		/// Create a compression instance based on the user's configuration in the given
		/// Configuration object.
		/// </summary>
		/// <exception cref="System.IO.IOException">if the specified codec is not available.</exception>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression CreateCompression
			(Configuration conf)
		{
			bool compressImage = conf.GetBoolean(DFSConfigKeys.DfsImageCompressKey, DFSConfigKeys
				.DfsImageCompressDefault);
			if (!compressImage)
			{
				return CreateNoopCompression();
			}
			string codecClassName = conf.Get(DFSConfigKeys.DfsImageCompressionCodecKey, DFSConfigKeys
				.DfsImageCompressionCodecDefault);
			return CreateCompression(conf, codecClassName);
		}

		/// <summary>
		/// Create a compression instance using the codec specified by
		/// <code>codecClassName</code>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression CreateCompression
			(Configuration conf, string codecClassName)
		{
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = factory.GetCodecByClassName(codecClassName);
			if (codec == null)
			{
				throw new IOException("Not a supported codec: " + codecClassName);
			}
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression(codec);
		}

		/// <summary>Create a compression instance based on a header read from an input stream.
		/// 	</summary>
		/// <exception cref="System.IO.IOException">
		/// if the specified codec is not available or the
		/// underlying IO fails.
		/// </exception>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImageCompression ReadCompressionHeader
			(Configuration conf, DataInput @in)
		{
			bool isCompressed = @in.ReadBoolean();
			if (!isCompressed)
			{
				return CreateNoopCompression();
			}
			else
			{
				string codecClassName = Text.ReadString(@in);
				return CreateCompression(conf, codecClassName);
			}
		}

		/// <summary>
		/// Unwrap a compressed input stream by wrapping it with a decompressor based
		/// on this codec.
		/// </summary>
		/// <remarks>
		/// Unwrap a compressed input stream by wrapping it with a decompressor based
		/// on this codec. If this instance represents no compression, simply adds
		/// buffering to the input stream.
		/// </remarks>
		/// <returns>a buffered stream that provides uncompressed data</returns>
		/// <exception cref="System.IO.IOException">
		/// If the decompressor cannot be instantiated or an IO
		/// error occurs.
		/// </exception>
		internal virtual DataInputStream UnwrapInputStream(InputStream @is)
		{
			if (imageCodec != null)
			{
				return new DataInputStream(imageCodec.CreateInputStream(@is));
			}
			else
			{
				return new DataInputStream(new BufferedInputStream(@is));
			}
		}

		/// <summary>
		/// Write out a header to the given stream that indicates the chosen
		/// compression codec, and return the same stream wrapped with that codec.
		/// </summary>
		/// <remarks>
		/// Write out a header to the given stream that indicates the chosen
		/// compression codec, and return the same stream wrapped with that codec.
		/// If no codec is specified, simply adds buffering to the stream, so that
		/// the returned stream is always buffered.
		/// </remarks>
		/// <param name="os">
		/// The stream to write header to and wrap. This stream should
		/// be unbuffered.
		/// </param>
		/// <returns>
		/// A stream wrapped with the specified compressor, or buffering
		/// if compression is not enabled.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// if an IO error occurs or the compressor cannot be
		/// instantiated
		/// </exception>
		internal virtual DataOutputStream WriteHeaderAndWrapStream(OutputStream os)
		{
			DataOutputStream dos = new DataOutputStream(os);
			dos.WriteBoolean(imageCodec != null);
			if (imageCodec != null)
			{
				string codecClassName = imageCodec.GetType().GetCanonicalName();
				Text.WriteString(dos, codecClassName);
				return new DataOutputStream(imageCodec.CreateOutputStream(os));
			}
			else
			{
				// use a buffered output stream
				return new DataOutputStream(new BufferedOutputStream(os));
			}
		}

		public override string ToString()
		{
			if (imageCodec != null)
			{
				return "codec " + imageCodec.GetType().GetCanonicalName();
			}
			else
			{
				return "no compression";
			}
		}
	}
}
