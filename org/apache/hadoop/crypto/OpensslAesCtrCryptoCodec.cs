using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>Implement the AES-CTR crypto codec using JNI into OpenSSL.</summary>
	public class OpensslAesCtrCryptoCodec : org.apache.hadoop.crypto.AesCtrCryptoCodec
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec
			)).getName());

		private org.apache.hadoop.conf.Configuration conf;

		private java.util.Random random;

		public OpensslAesCtrCryptoCodec()
		{
			string loadingFailureReason = org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				();
			if (loadingFailureReason != null)
			{
				throw new System.Exception(loadingFailureReason);
			}
		}

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
			java.lang.Class klass = conf.getClass<java.util.Random>(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.HADOOP_SECURITY_SECURE_RANDOM_IMPL_KEY, Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.crypto.random.OsSecureRandom)));
			try
			{
				random = org.apache.hadoop.util.ReflectionUtils.newInstance(klass, conf);
			}
			catch (System.Exception e)
			{
				LOG.info("Unable to use " + klass.getName() + ".  Falling back to " + "Java SecureRandom."
					, e);
				this.random = new java.security.SecureRandom();
			}
		}

		~OpensslAesCtrCryptoCodec()
		{
			try
			{
				java.io.Closeable r = (java.io.Closeable)this.random;
				r.close();
			}
			catch (System.InvalidCastException)
			{
			}
			base.finalize();
		}

		public override org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		public override org.apache.hadoop.crypto.Encryptor createEncryptor()
		{
			return new org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec.OpensslAesCtrCipher(
				org.apache.hadoop.crypto.OpensslCipher.ENCRYPT_MODE);
		}

		/// <exception cref="java.security.GeneralSecurityException"/>
		public override org.apache.hadoop.crypto.Decryptor createDecryptor()
		{
			return new org.apache.hadoop.crypto.OpensslAesCtrCryptoCodec.OpensslAesCtrCipher(
				org.apache.hadoop.crypto.OpensslCipher.DECRYPT_MODE);
		}

		public override void generateSecureRandom(byte[] bytes)
		{
			random.nextBytes(bytes);
		}

		private class OpensslAesCtrCipher : org.apache.hadoop.crypto.Encryptor, org.apache.hadoop.crypto.Decryptor
		{
			private readonly org.apache.hadoop.crypto.OpensslCipher cipher;

			private readonly int mode;

			private bool contextReset = false;

			/// <exception cref="java.security.GeneralSecurityException"/>
			public OpensslAesCtrCipher(int mode)
			{
				this.mode = mode;
				cipher = org.apache.hadoop.crypto.OpensslCipher.getInstance(SUITE.getName());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void init(byte[] key, byte[] iv)
			{
				com.google.common.@base.Preconditions.checkNotNull(key);
				com.google.common.@base.Preconditions.checkNotNull(iv);
				contextReset = false;
				cipher.init(mode, key, iv);
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
					// OpensslCipher#update will maintain crypto context.
					int n = cipher.update(inBuffer, outBuffer);
					if (n < inputSize)
					{
						contextReset = true;
						cipher.doFinal(outBuffer);
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
