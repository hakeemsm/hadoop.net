using System;
using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Random;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Implement the AES-CTR crypto codec using JNI into OpenSSL.</summary>
	public class OpensslAesCtrCryptoCodec : AesCtrCryptoCodec
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Crypto.OpensslAesCtrCryptoCodec
			).FullName);

		private Configuration conf;

		private Random random;

		public OpensslAesCtrCryptoCodec()
		{
			string loadingFailureReason = OpensslCipher.GetLoadingFailureReason();
			if (loadingFailureReason != null)
			{
				throw new RuntimeException(loadingFailureReason);
			}
		}

		public override void SetConf(Configuration conf)
		{
			this.conf = conf;
			Type klass = conf.GetClass<Sharpen.Random>(CommonConfigurationKeysPublic.HadoopSecuritySecureRandomImplKey
				, typeof(OsSecureRandom));
			try
			{
				random = ReflectionUtils.NewInstance(klass, conf);
			}
			catch (Exception e)
			{
				Log.Info("Unable to use " + klass.FullName + ".  Falling back to " + "Java SecureRandom."
					, e);
				this.random = new SecureRandom();
			}
		}

		~OpensslAesCtrCryptoCodec()
		{
			try
			{
				IDisposable r = (IDisposable)this.random;
				r.Close();
			}
			catch (InvalidCastException)
			{
			}
			base.Finalize();
		}

		public override Configuration GetConf()
		{
			return conf;
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override Encryptor CreateEncryptor()
		{
			return new OpensslAesCtrCryptoCodec.OpensslAesCtrCipher(OpensslCipher.EncryptMode
				);
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override Decryptor CreateDecryptor()
		{
			return new OpensslAesCtrCryptoCodec.OpensslAesCtrCipher(OpensslCipher.DecryptMode
				);
		}

		public override void GenerateSecureRandom(byte[] bytes)
		{
			random.NextBytes(bytes);
		}

		private class OpensslAesCtrCipher : Encryptor, Decryptor
		{
			private readonly OpensslCipher cipher;

			private readonly int mode;

			private bool contextReset = false;

			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public OpensslAesCtrCipher(int mode)
			{
				this.mode = mode;
				cipher = OpensslCipher.GetInstance(Suite.GetName());
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Init(byte[] key, byte[] iv)
			{
				Preconditions.CheckNotNull(key);
				Preconditions.CheckNotNull(iv);
				contextReset = false;
				cipher.Init(mode, key, iv);
			}

			/// <summary>AES-CTR will consume all of the input data.</summary>
			/// <remarks>
			/// AES-CTR will consume all of the input data. It requires enough space in
			/// the destination buffer to encrypt entire input buffer.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Encrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
			{
				Process(inBuffer, outBuffer);
			}

			/// <summary>AES-CTR will consume all of the input data.</summary>
			/// <remarks>
			/// AES-CTR will consume all of the input data. It requires enough space in
			/// the destination buffer to decrypt entire input buffer.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer)
			{
				Process(inBuffer, outBuffer);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Process(ByteBuffer inBuffer, ByteBuffer outBuffer)
			{
				try
				{
					int inputSize = inBuffer.Remaining();
					// OpensslCipher#update will maintain crypto context.
					int n = cipher.Update(inBuffer, outBuffer);
					if (n < inputSize)
					{
						contextReset = true;
						cipher.DoFinal(outBuffer);
					}
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			public virtual bool IsContextReset()
			{
				return contextReset;
			}
		}
	}
}
