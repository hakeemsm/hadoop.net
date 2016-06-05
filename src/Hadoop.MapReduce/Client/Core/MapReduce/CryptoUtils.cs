using System.IO;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Crypto;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// This class provides utilities to make it easier to work with Cryptographic
	/// Streams.
	/// </summary>
	/// <remarks>
	/// This class provides utilities to make it easier to work with Cryptographic
	/// Streams. Specifically for dealing with encrypting intermediate data such
	/// MapReduce spill files.
	/// </remarks>
	public class CryptoUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CryptoUtils));

		public static bool IsEncryptedSpillEnabled(Configuration conf)
		{
			return conf.GetBoolean(MRJobConfig.MrEncryptedIntermediateData, MRJobConfig.DefaultMrEncryptedIntermediateData
				);
		}

		/// <summary>This method creates and initializes an IV (Initialization Vector)</summary>
		/// <param name="conf"/>
		/// <returns>byte[]</returns>
		/// <exception cref="System.IO.IOException"/>
		public static byte[] CreateIV(Configuration conf)
		{
			CryptoCodec cryptoCodec = CryptoCodec.GetInstance(conf);
			if (IsEncryptedSpillEnabled(conf))
			{
				byte[] iv = new byte[cryptoCodec.GetCipherSuite().GetAlgorithmBlockSize()];
				cryptoCodec.GenerateSecureRandom(iv);
				return iv;
			}
			else
			{
				return null;
			}
		}

		public static int CryptoPadding(Configuration conf)
		{
			// Sizeof(IV) + long(start-offset)
			return IsEncryptedSpillEnabled(conf) ? CryptoCodec.GetInstance(conf).GetCipherSuite
				().GetAlgorithmBlockSize() + 8 : 0;
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[] GetEncryptionKey()
		{
			return TokenCache.GetEncryptedSpillKey(UserGroupInformation.GetCurrentUser().GetCredentials
				());
		}

		private static int GetBufferSize(Configuration conf)
		{
			return conf.GetInt(MRJobConfig.MrEncryptedIntermediateDataBufferKb, MRJobConfig.DefaultMrEncryptedIntermediateDataBufferKb
				) * 1024;
		}

		/// <summary>Wraps a given FSDataOutputStream with a CryptoOutputStream.</summary>
		/// <remarks>
		/// Wraps a given FSDataOutputStream with a CryptoOutputStream. The size of the
		/// data buffer required for the stream is specified by the
		/// "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
		/// variable.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="out"/>
		/// <returns>FSDataOutputStream</returns>
		/// <exception cref="System.IO.IOException"/>
		public static FSDataOutputStream WrapIfNecessary(Configuration conf, FSDataOutputStream
			 @out)
		{
			if (IsEncryptedSpillEnabled(conf))
			{
				@out.Write(((byte[])ByteBuffer.Allocate(8).PutLong(@out.GetPos()).Array()));
				byte[] iv = CreateIV(conf);
				@out.Write(iv);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("IV written to Stream [" + Base64.EncodeBase64URLSafeString(iv) + "]");
				}
				return new CryptoFSDataOutputStream(@out, CryptoCodec.GetInstance(conf), GetBufferSize
					(conf), GetEncryptionKey(), iv);
			}
			else
			{
				return @out;
			}
		}

		/// <summary>Wraps a given InputStream with a CryptoInputStream.</summary>
		/// <remarks>
		/// Wraps a given InputStream with a CryptoInputStream. The size of the data
		/// buffer required for the stream is specified by the
		/// "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
		/// variable.
		/// If the value of 'length' is &gt; -1, The InputStream is additionally
		/// wrapped in a LimitInputStream. CryptoStreams are late buffering in nature.
		/// This means they will always try to read ahead if they can. The
		/// LimitInputStream will ensure that the CryptoStream does not read past the
		/// provided length from the given Input Stream.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="in"/>
		/// <param name="length"/>
		/// <returns>InputStream</returns>
		/// <exception cref="System.IO.IOException"/>
		public static InputStream WrapIfNecessary(Configuration conf, InputStream @in, long
			 length)
		{
			if (IsEncryptedSpillEnabled(conf))
			{
				int bufferSize = GetBufferSize(conf);
				if (length > -1)
				{
					@in = new LimitInputStream(@in, length);
				}
				byte[] offsetArray = new byte[8];
				IOUtils.ReadFully(@in, offsetArray, 0, 8);
				long offset = ByteBuffer.Wrap(offsetArray).GetLong();
				CryptoCodec cryptoCodec = CryptoCodec.GetInstance(conf);
				byte[] iv = new byte[cryptoCodec.GetCipherSuite().GetAlgorithmBlockSize()];
				IOUtils.ReadFully(@in, iv, 0, cryptoCodec.GetCipherSuite().GetAlgorithmBlockSize(
					));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("IV read from [" + Base64.EncodeBase64URLSafeString(iv) + "]");
				}
				return new CryptoInputStream(@in, cryptoCodec, bufferSize, GetEncryptionKey(), iv
					, offset + CryptoPadding(conf));
			}
			else
			{
				return @in;
			}
		}

		/// <summary>Wraps a given FSDataInputStream with a CryptoInputStream.</summary>
		/// <remarks>
		/// Wraps a given FSDataInputStream with a CryptoInputStream. The size of the
		/// data buffer required for the stream is specified by the
		/// "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
		/// variable.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="in"/>
		/// <returns>FSDataInputStream</returns>
		/// <exception cref="System.IO.IOException"/>
		public static FSDataInputStream WrapIfNecessary(Configuration conf, FSDataInputStream
			 @in)
		{
			if (IsEncryptedSpillEnabled(conf))
			{
				CryptoCodec cryptoCodec = CryptoCodec.GetInstance(conf);
				int bufferSize = GetBufferSize(conf);
				// Not going to be used... but still has to be read...
				// Since the O/P stream always writes it..
				IOUtils.ReadFully(@in, new byte[8], 0, 8);
				byte[] iv = new byte[cryptoCodec.GetCipherSuite().GetAlgorithmBlockSize()];
				IOUtils.ReadFully(@in, iv, 0, cryptoCodec.GetCipherSuite().GetAlgorithmBlockSize(
					));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("IV read from Stream [" + Base64.EncodeBase64URLSafeString(iv) + "]");
				}
				return new CryptoFSDataInputStream(@in, cryptoCodec, bufferSize, GetEncryptionKey
					(), iv);
			}
			else
			{
				return @in;
			}
		}
	}
}
