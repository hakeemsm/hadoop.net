using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;

using Sun.Misc;
using Sun.Nio.CH;

namespace Org.Apache.Hadoop.Crypto
{
	public class CryptoStreamUtils
	{
		private const int MinBufferSize = 512;

		/// <summary>Forcibly free the direct buffer.</summary>
		public static void FreeDB(ByteBuffer buffer)
		{
			if (buffer is DirectBuffer)
			{
				Cleaner bufferCleaner = ((DirectBuffer)buffer).Cleaner();
				bufferCleaner.Clean();
			}
		}

		/// <summary>Read crypto buffer size</summary>
		public static int GetBufferSize(Configuration conf)
		{
			return conf.GetInt(CommonConfigurationKeysPublic.HadoopSecurityCryptoBufferSizeKey
				, CommonConfigurationKeysPublic.HadoopSecurityCryptoBufferSizeDefault);
		}

		/// <summary>AES/CTR/NoPadding is required</summary>
		public static void CheckCodec(CryptoCodec codec)
		{
			if (codec.GetCipherSuite() != CipherSuite.AesCtrNopadding)
			{
				throw new UnsupportedCodecException("AES/CTR/NoPadding is required");
			}
		}

		/// <summary>Check and floor buffer size</summary>
		public static int CheckBufferSize(CryptoCodec codec, int bufferSize)
		{
			Preconditions.CheckArgument(bufferSize >= MinBufferSize, "Minimum value of buffer size is "
				 + MinBufferSize + ".");
			return bufferSize - bufferSize % codec.GetCipherSuite().GetAlgorithmBlockSize();
		}

		/// <summary>
		/// If input stream is
		/// <see cref="Org.Apache.Hadoop.FS.Seekable"/>
		/// , return it's
		/// current position, otherwise return 0;
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static long GetInputStreamOffset(InputStream @in)
		{
			if (@in is Seekable)
			{
				return ((Seekable)@in).GetPos();
			}
			return 0;
		}
	}
}
