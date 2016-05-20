using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class CryptoStreamUtils
	{
		private const int MIN_BUFFER_SIZE = 512;

		/// <summary>Forcibly free the direct buffer.</summary>
		public static void freeDB(java.nio.ByteBuffer buffer)
		{
			if (buffer is sun.nio.ch.DirectBuffer)
			{
				sun.misc.Cleaner bufferCleaner = ((sun.nio.ch.DirectBuffer)buffer).cleaner();
				bufferCleaner.clean();
			}
		}

		/// <summary>Read crypto buffer size</summary>
		public static int getBufferSize(org.apache.hadoop.conf.Configuration conf)
		{
			return conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT
				);
		}

		/// <summary>AES/CTR/NoPadding is required</summary>
		public static void checkCodec(org.apache.hadoop.crypto.CryptoCodec codec)
		{
			if (codec.getCipherSuite() != org.apache.hadoop.crypto.CipherSuite.AES_CTR_NOPADDING)
			{
				throw new org.apache.hadoop.crypto.UnsupportedCodecException("AES/CTR/NoPadding is required"
					);
			}
		}

		/// <summary>Check and floor buffer size</summary>
		public static int checkBufferSize(org.apache.hadoop.crypto.CryptoCodec codec, int
			 bufferSize)
		{
			com.google.common.@base.Preconditions.checkArgument(bufferSize >= MIN_BUFFER_SIZE
				, "Minimum value of buffer size is " + MIN_BUFFER_SIZE + ".");
			return bufferSize - bufferSize % codec.getCipherSuite().getAlgorithmBlockSize();
		}

		/// <summary>
		/// If input stream is
		/// <see cref="org.apache.hadoop.fs.Seekable"/>
		/// , return it's
		/// current position, otherwise return 0;
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static long getInputStreamOffset(java.io.InputStream @in)
		{
			if (@in is org.apache.hadoop.fs.Seekable)
			{
				return ((org.apache.hadoop.fs.Seekable)@in).getPos();
			}
			return 0;
		}
	}
}
