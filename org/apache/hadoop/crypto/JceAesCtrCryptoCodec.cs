using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>Implement the AES-CTR crypto codec using JCE provider.</summary>
	public class JceAesCtrCryptoCodec : org.apache.hadoop.crypto.AesCtrCryptoCodec
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.JceAesCtrCryptoCodec
			)).getName());

		private org.apache.hadoop.conf.Configuration conf;

		private string provider;

		private java.security.SecureRandom random;

		public JceAesCtrCryptoCodec()
		{
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
			provider = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_JCE_PROVIDER_KEY
				);
			string secureRandomAlg = conf.get(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_KEY, org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.HADOOP_SECURITY_JAVA_SECURE_RANDOM_ALGORITHM_DEFAULT);
			try
			{
				random = (provider != null) ? java.security.SecureRandom.getInstance(secureRandomAlg
					, provider) : java.security.SecureRandom.getInstance(secureRandomAlg);
			}
			catch (java.security.GeneralSecurityException e)
			{
				LOG.warn(e.Message);
				random = new java.security.SecureRandom();
			}
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		public override org.apache.hadoop.crypto.Encryptor createEncryptor()
		{
			return new org.apache.hadoop.crypto.JceAesCtrCryptoCodec.JceAesCtrCipher(javax.crypto.Cipher
				.ENCRYPT_MODE, provider);
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		public override org.apache.hadoop.crypto.Decryptor createDecryptor()
		{
			return new org.apache.hadoop.crypto.JceAesCtrCryptoCodec.JceAesCtrCipher(javax.crypto.Cipher
				.DECRYPT_MODE, provider);
		}

		public override void generateSecureRandom(byte[] bytes)
		{
			random.nextBytes(bytes);
		}

		private class JceAesCtrCipher : org.apache.hadoop.crypto.Encryptor, org.apache.hadoop.crypto.Decryptor
		{
			private readonly javax.crypto.Cipher cipher;

			private readonly int mode;

			private bool contextReset = false;

			/// <exception cref="java.security.GeneralSecurityException"/>
			public JceAesCtrCipher(int mode, string provider)
			{
				this.mode = mode;
				if (provider == null || provider.isEmpty())
				{
					cipher = javax.crypto.Cipher.getInstance(SUITE.getName());
				}
				else
				{
					cipher = javax.crypto.Cipher.getInstance(SUITE.getName(), provider);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void init(byte[] key, byte[] iv)
			{
				com.google.common.@base.Preconditions.checkNotNull(key);
				com.google.common.@base.Preconditions.checkNotNull(iv);
				contextReset = false;
				try
				{
					cipher.init(mode, new javax.crypto.spec.SecretKeySpec(key, "AES"), new javax.crypto.spec.IvParameterSpec
						(iv));
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException(e);
				}
			}

			/// <summary>AES-CTR will consume all of the input data.</summary>
			/// <remarks>
			/// AES-CTR will consume all of the input data. It requires enough space in
			/// the destination buffer to encrypt entire input buffer.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void encrypt(java.nio.ByteBuffer inBuffer, java.nio.ByteBuffer outBuffer
				)
			{
				process(inBuffer, outBuffer);
			}

			/// <summary>AES-CTR will consume all of the input data.</summary>
			/// <remarks>
			/// AES-CTR will consume all of the input data. It requires enough space in
			/// the destination buffer to decrypt entire input buffer.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void decrypt(java.nio.ByteBuffer inBuffer, java.nio.ByteBuffer outBuffer
				)
			{
				process(inBuffer, outBuffer);
			}

			/// <exception cref="System.IO.IOException"/>
			private void process(java.nio.ByteBuffer inBuffer, java.nio.ByteBuffer outBuffer)
			{
				try
				{
					int inputSize = inBuffer.remaining();
					// Cipher#update will maintain crypto context.
					int n = cipher.update(inBuffer, outBuffer);
					if (n < inputSize)
					{
						contextReset = true;
						cipher.doFinal(inBuffer, outBuffer);
					}
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException(e);
				}
			}

			public virtual bool isContextReset()
			{
				return contextReset;
			}
		}
	}
}
