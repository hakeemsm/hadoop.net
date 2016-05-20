using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>Crypto codec class, encapsulates encryptor/decryptor pair.</summary>
	public abstract class CryptoCodec : org.apache.hadoop.conf.Configurable
	{
		public static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.crypto.CryptoCodec)));

		/// <summary>Get crypto codec for specified algorithm/mode/padding.</summary>
		/// <param name="conf">the configuration</param>
		/// <param name="cipherSuite">algorithm/mode/padding</param>
		/// <returns>
		/// CryptoCodec the codec object. Null value will be returned if no
		/// crypto codec classes with cipher suite configured.
		/// </returns>
		public static org.apache.hadoop.crypto.CryptoCodec getInstance(org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.crypto.CipherSuite cipherSuite)
		{
			System.Collections.Generic.IList<java.lang.Class> klasses = getCodecClasses(conf, 
				cipherSuite);
			if (klasses == null)
			{
				return null;
			}
			org.apache.hadoop.crypto.CryptoCodec codec = null;
			foreach (java.lang.Class klass in klasses)
			{
				try
				{
					org.apache.hadoop.crypto.CryptoCodec c = org.apache.hadoop.util.ReflectionUtils.newInstance
						(klass, conf);
					if (c.getCipherSuite().getName().Equals(cipherSuite.getName()))
					{
						if (codec == null)
						{
							org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Using crypto codec {}.", klass
								.getName());
							codec = c;
						}
					}
					else
					{
						org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Crypto codec {} doesn't meet the cipher suite {}."
							, klass.getName(), cipherSuite.getName());
					}
				}
				catch (System.Exception)
				{
					org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Crypto codec {} is not available."
						, klass.getName());
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
		public static org.apache.hadoop.crypto.CryptoCodec getInstance(org.apache.hadoop.conf.Configuration
			 conf)
		{
			string name = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CIPHER_SUITE_DEFAULT
				);
			return getInstance(conf, org.apache.hadoop.crypto.CipherSuite.convert(name));
		}

		private static System.Collections.Generic.IList<java.lang.Class> getCodecClasses(
			org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.crypto.CipherSuite 
			cipherSuite)
		{
			System.Collections.Generic.IList<java.lang.Class> result = com.google.common.collect.Lists
				.newArrayList();
			string configName = org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
				 + cipherSuite.getConfigSuffix();
			string codecString = conf.get(configName);
			if (codecString == null)
			{
				org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("No crypto codec classes with cipher suite configured."
					);
				return null;
			}
			foreach (string c in com.google.common.@base.Splitter.on(',').trimResults().omitEmptyStrings
				().split(codecString))
			{
				try
				{
					java.lang.Class cls = conf.getClassByName(c);
					result.add(cls.asSubclass<org.apache.hadoop.crypto.CryptoCodec>());
				}
				catch (System.InvalidCastException)
				{
					org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Class {} is not a CryptoCodec."
						, c);
				}
				catch (java.lang.ClassNotFoundException)
				{
					org.apache.hadoop.util.PerformanceAdvisory.LOG.debug("Crypto codec {} not found."
						, c);
				}
			}
			return result;
		}

		/// <returns>the CipherSuite for this codec.</returns>
		public abstract org.apache.hadoop.crypto.CipherSuite getCipherSuite();

		/// <summary>
		/// Create a
		/// <see cref="Encryptor"/>
		/// .
		/// </summary>
		/// <returns>Encryptor the encryptor</returns>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public abstract org.apache.hadoop.crypto.Encryptor createEncryptor();

		/// <summary>
		/// Create a
		/// <see cref="Decryptor"/>
		/// .
		/// </summary>
		/// <returns>Decryptor the decryptor</returns>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public abstract org.apache.hadoop.crypto.Decryptor createDecryptor();

		/// <summary>This interface is only for Counter (CTR) mode.</summary>
		/// <remarks>
		/// This interface is only for Counter (CTR) mode. Generally the Encryptor
		/// or Decryptor calculates the IV and maintain encryption context internally.
		/// For example a
		/// <see cref="javax.crypto.Cipher"/>
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
		/// <param name="IV">the IV for input stream position</param>
		public abstract void calculateIV(byte[] initIV, long counter, byte[] IV);

		/// <summary>Generate a number of secure, random bytes suitable for cryptographic use.
		/// 	</summary>
		/// <remarks>
		/// Generate a number of secure, random bytes suitable for cryptographic use.
		/// This method needs to be thread-safe.
		/// </remarks>
		/// <param name="bytes">byte array to populate with random data</param>
		public abstract void generateSecureRandom(byte[] bytes);

		public abstract org.apache.hadoop.conf.Configuration getConf();

		public abstract void setConf(org.apache.hadoop.conf.Configuration arg1);
	}
}
