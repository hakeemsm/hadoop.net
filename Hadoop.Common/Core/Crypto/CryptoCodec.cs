using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Crypto codec class, encapsulates encryptor/decryptor pair.</summary>
	public abstract class CryptoCodec : Configurable
	{
		public static Logger Log = LoggerFactory.GetLogger(typeof(CryptoCodec));

		/// <summary>Get crypto codec for specified algorithm/mode/padding.</summary>
		/// <param name="conf">the configuration</param>
		/// <param name="cipherSuite">algorithm/mode/padding</param>
		/// <returns>
		/// CryptoCodec the codec object. Null value will be returned if no
		/// crypto codec classes with cipher suite configured.
		/// </returns>
		public static CryptoCodec GetInstance(Configuration conf, CipherSuite cipherSuite
			)
		{
			IList<Type> klasses = GetCodecClasses(conf, cipherSuite);
			if (klasses == null)
			{
				return null;
			}
			CryptoCodec codec = null;
			foreach (Type klass in klasses)
			{
				try
				{
					CryptoCodec c = ReflectionUtils.NewInstance(klass, conf);
					if (c.GetCipherSuite().GetName().Equals(cipherSuite.GetName()))
					{
						if (codec == null)
						{
							PerformanceAdvisory.Log.Debug("Using crypto codec {}.", klass.FullName);
							codec = c;
						}
					}
					else
					{
						PerformanceAdvisory.Log.Debug("Crypto codec {} doesn't meet the cipher suite {}."
							, klass.FullName, cipherSuite.GetName());
					}
				}
				catch (Exception)
				{
					PerformanceAdvisory.Log.Debug("Crypto codec {} is not available.", klass.FullName
						);
				}
			}
			return codec;
		}

		/// <summary>
		/// Get crypto codec for algorithm/mode/padding in config value
		/// hadoop.security.crypto.cipher.suite
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// CryptoCodec the codec object Null value will be returned if no
		/// crypto codec classes with cipher suite configured.
		/// </returns>
		public static CryptoCodec GetInstance(Configuration conf)
		{
			string name = conf.Get(CommonConfigurationKeysPublic.HadoopSecurityCryptoCipherSuiteKey
				, CommonConfigurationKeysPublic.HadoopSecurityCryptoCipherSuiteDefault);
			return GetInstance(conf, CipherSuite.Convert(name));
		}

		private static IList<Type> GetCodecClasses(Configuration conf, CipherSuite cipherSuite
			)
		{
			IList<Type> result = Lists.NewArrayList();
			string configName = CommonConfigurationKeysPublic.HadoopSecurityCryptoCodecClassesKeyPrefix
				 + cipherSuite.GetConfigSuffix();
			string codecString = conf.Get(configName);
			if (codecString == null)
			{
				PerformanceAdvisory.Log.Debug("No crypto codec classes with cipher suite configured."
					);
				return null;
			}
			foreach (string c in Splitter.On(',').TrimResults().OmitEmptyStrings().Split(codecString
				))
			{
				try
				{
					Type cls = conf.GetClassByName(c);
					result.AddItem(cls.AsSubclass<CryptoCodec>());
				}
				catch (InvalidCastException)
				{
					PerformanceAdvisory.Log.Debug("Class {} is not a CryptoCodec.", c);
				}
				catch (TypeLoadException)
				{
					PerformanceAdvisory.Log.Debug("Crypto codec {} not found.", c);
				}
			}
			return result;
		}

		/// <returns>the CipherSuite for this codec.</returns>
		public abstract CipherSuite GetCipherSuite();

		/// <summary>
		/// Create a
		/// <see cref="Encryptor"/>
		/// .
		/// </summary>
		/// <returns>Encryptor the encryptor</returns>
		/// <exception cref="GeneralSecurityException"/>
		public abstract Encryptor CreateEncryptor();

		/// <summary>
		/// Create a
		/// <see cref="Decryptor"/>
		/// .
		/// </summary>
		/// <returns>Decryptor the decryptor</returns>
		/// <exception cref="GeneralSecurityException"/>
		public abstract Decryptor CreateDecryptor();

		/// <summary>This interface is only for Counter (CTR) mode.</summary>
		/// <remarks>
		/// This interface is only for Counter (CTR) mode. Generally the Encryptor
		/// or Decryptor calculates the IV and maintain encryption context internally.
		/// For example a
		/// <see cref="Cipher"/>
		/// will maintain its encryption
		/// context internally when we do encryption/decryption using the
		/// Cipher#update interface.
		/// <p/>
		/// Encryption/Decryption is not always on the entire file. For example,
		/// in Hadoop, a node may only decrypt a portion of a file (i.e. a split).
		/// In these situations, the counter is derived from the file position.
		/// <p/>
		/// The IV can be calculated by combining the initial IV and the counter with
		/// a lossless operation (concatenation, addition, or XOR).
		/// </remarks>
		/// <seealso>http://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_.28CTR.29
		/// 	</seealso>
		/// <param name="initIV">initial IV</param>
		/// <param name="counter">counter for input stream position</param>
		/// <param name="Iv">the IV for input stream position</param>
		public abstract void CalculateIV(byte[] initIV, long counter, byte[] Iv);

		/// <summary>Generate a number of secure, random bytes suitable for cryptographic use.
		/// 	</summary>
		/// <remarks>
		/// Generate a number of secure, random bytes suitable for cryptographic use.
		/// This method needs to be thread-safe.
		/// </remarks>
		/// <param name="bytes">byte array to populate with random data</param>
		public abstract void GenerateSecureRandom(byte[] bytes);

		public abstract Configuration GetConf();

		public abstract void SetConf(Configuration arg1);
	}
}
