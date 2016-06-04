using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms
{
	/// <summary>
	/// A simple LoadBalancing KMSClientProvider that round-robins requests
	/// across a provided array of KMSClientProviders.
	/// </summary>
	/// <remarks>
	/// A simple LoadBalancing KMSClientProvider that round-robins requests
	/// across a provided array of KMSClientProviders. It also retries failed
	/// requests on the next available provider in the load balancer group. It
	/// only retries failed requests that result in an IOException, sending back
	/// all other Exceptions to the caller without retry.
	/// </remarks>
	public class LoadBalancingKMSClientProvider : KeyProvider, KeyProviderCryptoExtension.CryptoExtension
		, KeyProviderDelegationTokenExtension.DelegationTokenExtension
	{
		public static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Crypto.Key.Kms.LoadBalancingKMSClientProvider
			));

		internal interface ProviderCallable<T>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			T Call(KMSClientProvider provider);
		}

		[System.Serializable]
		internal class WrapperException : RuntimeException
		{
			public WrapperException(Exception cause)
				: base(cause)
			{
			}
		}

		private readonly KMSClientProvider[] providers;

		private readonly AtomicInteger currentIdx;

		public LoadBalancingKMSClientProvider(KMSClientProvider[] providers, Configuration
			 conf)
			: this(Shuffle(providers), Time.MonotonicNow(), conf)
		{
		}

		[VisibleForTesting]
		internal LoadBalancingKMSClientProvider(KMSClientProvider[] providers, long seed, 
			Configuration conf)
			: base(conf)
		{
			this.providers = providers;
			this.currentIdx = new AtomicInteger((int)(seed % providers.Length));
		}

		[VisibleForTesting]
		internal virtual KMSClientProvider[] GetProviders()
		{
			return providers;
		}

		/// <exception cref="System.IO.IOException"/>
		private T DoOp<T>(LoadBalancingKMSClientProvider.ProviderCallable<T> op, int currPos
			)
		{
			IOException ex = null;
			for (int i = 0; i < providers.Length; i++)
			{
				KMSClientProvider provider = providers[(currPos + i) % providers.Length];
				try
				{
					return op.Call(provider);
				}
				catch (IOException ioe)
				{
					Log.Warn("KMS provider at [{}] threw an IOException [{}]!!", provider.GetKMSUrl()
						, ioe.Message);
					ex = ioe;
				}
				catch (Exception e)
				{
					if (e is RuntimeException)
					{
						throw (RuntimeException)e;
					}
					else
					{
						throw new LoadBalancingKMSClientProvider.WrapperException(e);
					}
				}
			}
			if (ex != null)
			{
				Log.Warn("Aborting since the Request has failed with all KMS" + " providers in the group. !!"
					);
				throw ex;
			}
			throw new IOException("No providers configured !!");
		}

		private int NextIdx()
		{
			while (true)
			{
				int current = currentIdx.Get();
				int next = (current + 1) % providers.Length;
				if (currentIdx.CompareAndSet(current, next))
				{
					return current;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
			(string renewer, Credentials credentials)
		{
			return DoOp(new _ProviderCallable_129(renewer, credentials), NextIdx());
		}

		private sealed class _ProviderCallable_129 : LoadBalancingKMSClientProvider.ProviderCallable
			<Org.Apache.Hadoop.Security.Token.Token<object>[]>
		{
			public _ProviderCallable_129(string renewer, Credentials credentials)
			{
				this.renewer = renewer;
				this.credentials = credentials;
			}

			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Security.Token.Token<object>[] Call(KMSClientProvider provider
				)
			{
				return provider.AddDelegationTokens(renewer, credentials);
			}

			private readonly string renewer;

			private readonly Credentials credentials;
		}

		// This request is sent to all providers in the load-balancing group
		/// <exception cref="System.IO.IOException"/>
		public virtual void WarmUpEncryptedKeys(params string[] keyNames)
		{
			foreach (KMSClientProvider provider in providers)
			{
				try
				{
					provider.WarmUpEncryptedKeys(keyNames);
				}
				catch (IOException)
				{
					Log.Error("Error warming up keys for provider with url" + "[" + provider.GetKMSUrl
						() + "]");
				}
			}
		}

		// This request is sent to all providers in the load-balancing group
		public virtual void Drain(string keyName)
		{
			foreach (KMSClientProvider provider in providers)
			{
				provider.Drain(keyName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public virtual KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
			(string encryptionKeyName)
		{
			try
			{
				return DoOp(new _ProviderCallable_164(encryptionKeyName), NextIdx());
			}
			catch (LoadBalancingKMSClientProvider.WrapperException we)
			{
				throw (GeneralSecurityException)we.InnerException;
			}
		}

		private sealed class _ProviderCallable_164 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProviderCryptoExtension.EncryptedKeyVersion>
		{
			public _ProviderCallable_164(string encryptionKeyName)
			{
				this.encryptionKeyName = encryptionKeyName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public KeyProviderCryptoExtension.EncryptedKeyVersion Call(KMSClientProvider provider
				)
			{
				return provider.GenerateEncryptedKey(encryptionKeyName);
			}

			private readonly string encryptionKeyName;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public virtual KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
			 encryptedKeyVersion)
		{
			try
			{
				return DoOp(new _ProviderCallable_181(encryptedKeyVersion), NextIdx());
			}
			catch (LoadBalancingKMSClientProvider.WrapperException we)
			{
				throw (GeneralSecurityException)we.InnerException;
			}
		}

		private sealed class _ProviderCallable_181 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_181(KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion
				)
			{
				this.encryptedKeyVersion = encryptedKeyVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.DecryptEncryptedKey(encryptedKeyVersion);
			}

			private readonly KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			return DoOp(new _ProviderCallable_195(versionName), NextIdx());
		}

		private sealed class _ProviderCallable_195 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_195(string versionName)
			{
				this.versionName = versionName;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.GetKeyVersion(versionName);
			}

			private readonly string versionName;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
		{
			return DoOp(new _ProviderCallable_205(), NextIdx());
		}

		private sealed class _ProviderCallable_205 : LoadBalancingKMSClientProvider.ProviderCallable
			<IList<string>>
		{
			public _ProviderCallable_205()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public IList<string> Call(KMSClientProvider provider)
			{
				return provider.GetKeys();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata[] GetKeysMetadata(params string[] names)
		{
			return DoOp(new _ProviderCallable_215(names), NextIdx());
		}

		private sealed class _ProviderCallable_215 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.Metadata[]>
		{
			public _ProviderCallable_215(string[] names)
			{
				this.names = names;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.Metadata[] Call(KMSClientProvider provider)
			{
				return provider.GetKeysMetadata(names);
			}

			private readonly string[] names;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			return DoOp(new _ProviderCallable_225(name), NextIdx());
		}

		private sealed class _ProviderCallable_225 : LoadBalancingKMSClientProvider.ProviderCallable
			<IList<KeyProvider.KeyVersion>>
		{
			public _ProviderCallable_225(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public IList<KeyProvider.KeyVersion> Call(KMSClientProvider provider)
			{
				return provider.GetKeyVersions(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			return DoOp(new _ProviderCallable_236(name), NextIdx());
		}

		private sealed class _ProviderCallable_236 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_236(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.GetCurrentKey(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			return DoOp(new _ProviderCallable_245(name), NextIdx());
		}

		private sealed class _ProviderCallable_245 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.Metadata>
		{
			public _ProviderCallable_245(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.Metadata Call(KMSClientProvider provider)
			{
				return provider.GetMetadata(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			return DoOp(new _ProviderCallable_256(name, material, options), NextIdx());
		}

		private sealed class _ProviderCallable_256 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_256(string name, byte[] material, KeyProvider.Options options
				)
			{
				this.name = name;
				this.material = material;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.CreateKey(name, material, options);
			}

			private readonly string name;

			private readonly byte[] material;

			private readonly KeyProvider.Options options;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, KeyProvider.Options
			 options)
		{
			try
			{
				return DoOp(new _ProviderCallable_268(name, options), NextIdx());
			}
			catch (LoadBalancingKMSClientProvider.WrapperException e)
			{
				throw (NoSuchAlgorithmException)e.InnerException;
			}
		}

		private sealed class _ProviderCallable_268 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_268(string name, KeyProvider.Options options)
			{
				this.name = name;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.CreateKey(name, options);
			}

			private readonly string name;

			private readonly KeyProvider.Options options;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			DoOp(new _ProviderCallable_281(name), NextIdx());
		}

		private sealed class _ProviderCallable_281 : LoadBalancingKMSClientProvider.ProviderCallable
			<Void>
		{
			public _ProviderCallable_281(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call(KMSClientProvider provider)
			{
				provider.DeleteKey(name);
				return null;
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			return DoOp(new _ProviderCallable_292(name, material), NextIdx());
		}

		private sealed class _ProviderCallable_292 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_292(string name, byte[] material)
			{
				this.name = name;
				this.material = material;
			}

			/// <exception cref="System.IO.IOException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.RollNewVersion(name, material);
			}

			private readonly string name;

			private readonly byte[] material;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			try
			{
				return DoOp(new _ProviderCallable_304(name), NextIdx());
			}
			catch (LoadBalancingKMSClientProvider.WrapperException e)
			{
				throw (NoSuchAlgorithmException)e.InnerException;
			}
		}

		private sealed class _ProviderCallable_304 : LoadBalancingKMSClientProvider.ProviderCallable
			<KeyProvider.KeyVersion>
		{
			public _ProviderCallable_304(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			public KeyProvider.KeyVersion Call(KMSClientProvider provider)
			{
				return provider.RollNewVersion(name);
			}

			private readonly string name;
		}

		// Close all providers in the LB group
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			foreach (KMSClientProvider provider in providers)
			{
				try
				{
					provider.Close();
				}
				catch (IOException)
				{
					Log.Error("Error closing provider with url" + "[" + provider.GetKMSUrl() + "]");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			foreach (KMSClientProvider provider in providers)
			{
				try
				{
					provider.Flush();
				}
				catch (IOException)
				{
					Log.Error("Error flushing provider with url" + "[" + provider.GetKMSUrl() + "]");
				}
			}
		}

		private static KMSClientProvider[] Shuffle(KMSClientProvider[] providers)
		{
			IList<KMSClientProvider> list = Arrays.AsList(providers);
			Sharpen.Collections.Shuffle(list);
			return Sharpen.Collections.ToArray(list, providers);
		}
	}
}
