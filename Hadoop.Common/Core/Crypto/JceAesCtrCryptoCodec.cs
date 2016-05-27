using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Implement the AES-CTR crypto codec using JCE provider.</summary>
	public class JceAesCtrCryptoCodec : AesCtrCryptoCodec
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Crypto.JceAesCtrCryptoCodec
			).FullName);

		private Configuration conf;

		private string provider;

		private SecureRandom random;

		public JceAesCtrCryptoCodec()
		{
		}

		public override Configuration GetConf()
		{
			return conf;
		}

		public override void SetConf(Configuration conf)
		{
			this.conf = conf;
			provider = conf.Get(CommonConfigurationKeysPublic.HadoopSecurityCryptoJceProviderKey
				);
			string secureRandomAlg = conf.Get(CommonConfigurationKeysPublic.HadoopSecurityJavaSecureRandomAlgorithmKey
				, CommonConfigurationKeysPublic.HadoopSecurityJavaSecureRandomAlgorithmDefault);
			try
			{
				random = (provider != null) ? SecureRandom.GetInstance(secureRandomAlg, provider)
					 : SecureRandom.GetInstance(secureRandomAlg);
			}
			catch (GeneralSecurityException e)
			{
				Log.Warn(e.Message);
				random = new SecureRandom();
			}
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override Encryptor CreateEncryptor()
		{
			return new JceAesCtrCryptoCodec.JceAesCtrCipher(Sharpen.Cipher.EncryptMode, provider
				);
		}

		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override Decryptor CreateDecryptor()
		{
			return new JceAesCtrCryptoCodec.JceAesCtrCipher(Sharpen.Cipher.DecryptMode, provider
				);
		}

		public override void GenerateSecureRandom(byte[] bytes)
		{
			random.NextBytes(bytes);
		}

		private class JceAesCtrCipher : Encryptor, Decryptor
		{
			private readonly Sharpen.Cipher cipher;

			private readonly int mode;

			private bool contextReset = false;

			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public JceAesCtrCipher(int mode, string provider)
			{
				this.mode = mode;
				if (provider == null || provider.IsEmpty())
				{
					cipher = Sharpen.Cipher.GetInstance(Suite.GetName());
				}
				else
				{
					cipher = Sharpen.Cipher.GetInstance(Suite.GetName(), provider);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Init(byte[] key, byte[] iv)
			{
				Preconditions.CheckNotNull(key);
				Preconditions.CheckNotNull(iv);
				contextReset = false;
				try
				{
					cipher.Init(mode, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
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
					// Cipher#update will maintain crypto context.
					int n = cipher.Update(inBuffer, outBuffer);
					if (n < inputSize)
					{
						contextReset = true;
						cipher.DoFinal(inBuffer, outBuffer);
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
