using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Crypto.Key.Kms;


namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Crypto.Key.KeyProviderCryptoExtension"/>
	/// that pre-generates and caches encrypted
	/// keys.
	/// </summary>
	public class EagerKeyGeneratorKeyProviderCryptoExtension : KeyProviderCryptoExtension
	{
		private const string KeyCachePrefix = "hadoop.security.kms.encrypted.key.cache.";

		public const string KmsKeyCacheSize = KeyCachePrefix + "size";

		public const int KmsKeyCacheSizeDefault = 100;

		public const string KmsKeyCacheLowWatermark = KeyCachePrefix + "low.watermark";

		public const float KmsKeyCacheLowWatermarkDefault = 0.30f;

		public const string KmsKeyCacheExpiryMs = KeyCachePrefix + "expiry";

		public const int KmsKeyCacheExpiryDefault = 43200000;

		public const string KmsKeyCacheNumRefillThreads = KeyCachePrefix + "num.fill.threads";

		public const int KmsKeyCacheNumRefillThreadsDefault = 2;

		private class CryptoExtension : KeyProviderCryptoExtension.CryptoExtension
		{
			private class EncryptedQueueRefiller : ValueQueue.QueueRefiller<KeyProviderCryptoExtension.EncryptedKeyVersion
				>
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void FillQueueForKey(string keyName, Queue<KeyProviderCryptoExtension.EncryptedKeyVersion
					> keyQueue, int numKeys)
				{
					IList<KeyProviderCryptoExtension.EncryptedKeyVersion> retEdeks = new List<KeyProviderCryptoExtension.EncryptedKeyVersion
						>();
					for (int i = 0; i < numKeys; i++)
					{
						try
						{
							retEdeks.AddItem(this._enclosing.keyProviderCryptoExtension.GenerateEncryptedKey(
								keyName));
						}
						catch (GeneralSecurityException e)
						{
							throw new IOException(e);
						}
					}
					Collections.AddAll(keyQueue, retEdeks);
				}

				internal EncryptedQueueRefiller(CryptoExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly CryptoExtension _enclosing;
			}

			private KeyProviderCryptoExtension keyProviderCryptoExtension;

			private readonly ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion> encKeyVersionQueue;

			public CryptoExtension(Configuration conf, KeyProviderCryptoExtension keyProviderCryptoExtension
				)
			{
				this.keyProviderCryptoExtension = keyProviderCryptoExtension;
				encKeyVersionQueue = new ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion
					>(conf.GetInt(KmsKeyCacheSize, KmsKeyCacheSizeDefault), conf.GetFloat(KmsKeyCacheLowWatermark
					, KmsKeyCacheLowWatermarkDefault), conf.GetInt(KmsKeyCacheExpiryMs, KmsKeyCacheExpiryDefault
					), conf.GetInt(KmsKeyCacheNumRefillThreads, KmsKeyCacheNumRefillThreadsDefault), 
					ValueQueue.SyncGenerationPolicy.LowWatermark, new EagerKeyGeneratorKeyProviderCryptoExtension.CryptoExtension.EncryptedQueueRefiller
					(this));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WarmUpEncryptedKeys(params string[] keyNames)
			{
				try
				{
					encKeyVersionQueue.InitializeQueuesForKeys(keyNames);
				}
				catch (ExecutionException e)
				{
					throw new IOException(e);
				}
			}

			public virtual void Drain(string keyName)
			{
				encKeyVersionQueue.Drain(keyName);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="GeneralSecurityException"/>
			public virtual KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
				(string encryptionKeyName)
			{
				try
				{
					return encKeyVersionQueue.GetNext(encryptionKeyName);
				}
				catch (ExecutionException e)
				{
					throw new IOException(e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="GeneralSecurityException"/>
			public virtual KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion)
			{
				return keyProviderCryptoExtension.DecryptEncryptedKey(encryptedKeyVersion);
			}
		}

		/// <summary>
		/// This class is a proxy for a <code>KeyProviderCryptoExtension</code> that
		/// decorates the underlying <code>CryptoExtension</code> with one that eagerly
		/// caches pre-generated Encrypted Keys using a <code>ValueQueue</code>
		/// </summary>
		/// <param name="conf">Configuration object to load parameters from</param>
		/// <param name="keyProviderCryptoExtension">
		/// <code>KeyProviderCryptoExtension</code>
		/// to delegate calls to.
		/// </param>
		public EagerKeyGeneratorKeyProviderCryptoExtension(Configuration conf, KeyProviderCryptoExtension
			 keyProviderCryptoExtension)
			: base(keyProviderCryptoExtension, new EagerKeyGeneratorKeyProviderCryptoExtension.CryptoExtension
				(conf, keyProviderCryptoExtension))
		{
		}

		/// <exception cref="NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			KeyProvider.KeyVersion keyVersion = base.RollNewVersion(name);
			GetExtension().Drain(name);
			return keyVersion;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			KeyProvider.KeyVersion keyVersion = base.RollNewVersion(name, material);
			GetExtension().Drain(name);
			return keyVersion;
		}
	}
}
