using Sharpen;

namespace org.apache.hadoop.crypto.key.kms
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
	public class LoadBalancingKMSClientProvider : org.apache.hadoop.crypto.key.KeyProvider
		, org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension, org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
	{
		public static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider)));

		internal interface ProviderCallable<T>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			T call(org.apache.hadoop.crypto.key.kms.KMSClientProvider provider);
		}

		[System.Serializable]
		internal class WrapperException : System.Exception
		{
			public WrapperException(System.Exception cause)
				: base(cause)
			{
			}
		}

		private readonly org.apache.hadoop.crypto.key.kms.KMSClientProvider[] providers;

		private readonly java.util.concurrent.atomic.AtomicInteger currentIdx;

		public LoadBalancingKMSClientProvider(org.apache.hadoop.crypto.key.kms.KMSClientProvider
			[] providers, org.apache.hadoop.conf.Configuration conf)
			: this(shuffle(providers), org.apache.hadoop.util.Time.monotonicNow(), conf)
		{
		}

		[com.google.common.annotations.VisibleForTesting]
		internal LoadBalancingKMSClientProvider(org.apache.hadoop.crypto.key.kms.KMSClientProvider
			[] providers, long seed, org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			this.providers = providers;
			this.currentIdx = new java.util.concurrent.atomic.AtomicInteger((int)(seed % providers
				.Length));
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.crypto.key.kms.KMSClientProvider[] getProviders
			()
		{
			return providers;
		}

		/// <exception cref="System.IO.IOException"/>
		private T doOp<T>(org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<T> op, int currPos)
		{
			System.IO.IOException ex = null;
			for (int i = 0; i < providers.Length; i++)
			{
				org.apache.hadoop.crypto.key.kms.KMSClientProvider provider = providers[(currPos 
					+ i) % providers.Length];
				try
				{
					return op.call(provider);
				}
				catch (System.IO.IOException ioe)
				{
					LOG.warn("KMS provider at [{}] threw an IOException [{}]!!", provider.getKMSUrl()
						, ioe.Message);
					ex = ioe;
				}
				catch (System.Exception e)
				{
					if (e is System.Exception)
					{
						throw (System.Exception)e;
					}
					else
					{
						throw new org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.WrapperException
							(e);
					}
				}
			}
			if (ex != null)
			{
				LOG.warn("Aborting since the Request has failed with all KMS" + " providers in the group. !!"
					);
				throw ex;
			}
			throw new System.IO.IOException("No providers configured !!");
		}

		private int nextIdx()
		{
			while (true)
			{
				int current = currentIdx.get();
				int next = (current + 1) % providers.Length;
				if (currentIdx.compareAndSet(current, next))
				{
					return current;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
			(string renewer, org.apache.hadoop.security.Credentials credentials)
		{
			return doOp(new _ProviderCallable_129(renewer, credentials), nextIdx());
		}

		private sealed class _ProviderCallable_129 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.security.token.Token<object>[]>
		{
			public _ProviderCallable_129(string renewer, org.apache.hadoop.security.Credentials
				 credentials)
			{
				this.renewer = renewer;
				this.credentials = credentials;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.security.token.Token<object>[] call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.addDelegationTokens(renewer, credentials);
			}

			private readonly string renewer;

			private readonly org.apache.hadoop.security.Credentials credentials;
		}

		// This request is sent to all providers in the load-balancing group
		/// <exception cref="System.IO.IOException"/>
		public virtual void warmUpEncryptedKeys(params string[] keyNames)
		{
			foreach (org.apache.hadoop.crypto.key.kms.KMSClientProvider provider in providers)
			{
				try
				{
					provider.warmUpEncryptedKeys(keyNames);
				}
				catch (System.IO.IOException)
				{
					LOG.error("Error warming up keys for provider with url" + "[" + provider.getKMSUrl
						() + "]");
				}
			}
		}

		// This request is sent to all providers in the load-balancing group
		public virtual void drain(string keyName)
		{
			foreach (org.apache.hadoop.crypto.key.kms.KMSClientProvider provider in providers)
			{
				provider.drain(keyName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
			 generateEncryptedKey(string encryptionKeyName)
		{
			try
			{
				return doOp(new _ProviderCallable_164(encryptionKeyName), nextIdx());
			}
			catch (org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.WrapperException
				 we)
			{
				throw (java.security.GeneralSecurityException)we.InnerException;
			}
		}

		private sealed class _ProviderCallable_164 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion>
		{
			public _ProviderCallable_164(string encryptionKeyName)
			{
				this.encryptionKeyName = encryptionKeyName;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.GeneralSecurityException"/>
			public org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 call(org.apache.hadoop.crypto.key.kms.KMSClientProvider provider)
			{
				return provider.generateEncryptedKey(encryptionKeyName);
			}

			private readonly string encryptionKeyName;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptEncryptedKey
			(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion
			)
		{
			try
			{
				return doOp(new _ProviderCallable_181(encryptedKeyVersion), nextIdx());
			}
			catch (org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.WrapperException
				 we)
			{
				throw (java.security.GeneralSecurityException)we.InnerException;
			}
		}

		private sealed class _ProviderCallable_181 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_181(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion)
			{
				this.encryptedKeyVersion = encryptedKeyVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.GeneralSecurityException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.decryptEncryptedKey(encryptedKeyVersion);
			}

			private readonly org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			return doOp(new _ProviderCallable_195(versionName), nextIdx());
		}

		private sealed class _ProviderCallable_195 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_195(string versionName)
			{
				this.versionName = versionName;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.getKeyVersion(versionName);
			}

			private readonly string versionName;
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getKeys()
		{
			return doOp(new _ProviderCallable_205(), nextIdx());
		}

		private sealed class _ProviderCallable_205 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<System.Collections.Generic.IList<string>>
		{
			public _ProviderCallable_205()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public System.Collections.Generic.IList<string> call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.getKeys();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata[] getKeysMetadata
			(params string[] names)
		{
			return doOp(new _ProviderCallable_215(names), nextIdx());
		}

		private sealed class _ProviderCallable_215 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.Metadata[]>
		{
			public _ProviderCallable_215(string[] names)
			{
				this.names = names;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.Metadata[] call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.getKeysMetadata(names);
			}

			private readonly string[] names;
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name)
		{
			return doOp(new _ProviderCallable_225(name), nextIdx());
		}

		private sealed class _ProviderCallable_225 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			>>
		{
			public _ProviderCallable_225(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> call(org.apache.hadoop.crypto.key.kms.KMSClientProvider provider)
			{
				return provider.getKeyVersions(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getCurrentKey
			(string name)
		{
			return doOp(new _ProviderCallable_236(name), nextIdx());
		}

		private sealed class _ProviderCallable_236 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_236(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.getCurrentKey(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			return doOp(new _ProviderCallable_245(name), nextIdx());
		}

		private sealed class _ProviderCallable_245 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.Metadata>
		{
			public _ProviderCallable_245(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.Metadata call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.getMetadata(name);
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			return doOp(new _ProviderCallable_256(name, material, options), nextIdx());
		}

		private sealed class _ProviderCallable_256 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_256(string name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options
				 options)
			{
				this.name = name;
				this.material = material;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.createKey(name, material, options);
			}

			private readonly string name;

			private readonly byte[] material;

			private readonly org.apache.hadoop.crypto.key.KeyProvider.Options options;
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, org.apache.hadoop.crypto.key.KeyProvider.Options options)
		{
			try
			{
				return doOp(new _ProviderCallable_268(name, options), nextIdx());
			}
			catch (org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.WrapperException
				 e)
			{
				throw (java.security.NoSuchAlgorithmException)e.InnerException;
			}
		}

		private sealed class _ProviderCallable_268 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_268(string name, org.apache.hadoop.crypto.key.KeyProvider.Options
				 options)
			{
				this.name = name;
				this.options = options;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.createKey(name, options);
			}

			private readonly string name;

			private readonly org.apache.hadoop.crypto.key.KeyProvider.Options options;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			doOp(new _ProviderCallable_281(name), nextIdx());
		}

		private sealed class _ProviderCallable_281 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<java.lang.Void>
		{
			public _ProviderCallable_281(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			public java.lang.Void call(org.apache.hadoop.crypto.key.kms.KMSClientProvider provider
				)
			{
				provider.deleteKey(name);
				return null;
			}

			private readonly string name;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			return doOp(new _ProviderCallable_292(name, material), nextIdx());
		}

		private sealed class _ProviderCallable_292 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_292(string name, byte[] material)
			{
				this.name = name;
				this.material = material;
			}

			/// <exception cref="System.IO.IOException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.rollNewVersion(name, material);
			}

			private readonly string name;

			private readonly byte[] material;
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name)
		{
			try
			{
				return doOp(new _ProviderCallable_304(name), nextIdx());
			}
			catch (org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.WrapperException
				 e)
			{
				throw (java.security.NoSuchAlgorithmException)e.InnerException;
			}
		}

		private sealed class _ProviderCallable_304 : org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider.ProviderCallable
			<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
		{
			public _ProviderCallable_304(string name)
			{
				this.name = name;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			public org.apache.hadoop.crypto.key.KeyProvider.KeyVersion call(org.apache.hadoop.crypto.key.kms.KMSClientProvider
				 provider)
			{
				return provider.rollNewVersion(name);
			}

			private readonly string name;
		}

		// Close all providers in the LB group
		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			foreach (org.apache.hadoop.crypto.key.kms.KMSClientProvider provider in providers)
			{
				try
				{
					provider.close();
				}
				catch (System.IO.IOException)
				{
					LOG.error("Error closing provider with url" + "[" + provider.getKMSUrl() + "]");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			foreach (org.apache.hadoop.crypto.key.kms.KMSClientProvider provider in providers)
			{
				try
				{
					provider.flush();
				}
				catch (System.IO.IOException)
				{
					LOG.error("Error flushing provider with url" + "[" + provider.getKMSUrl() + "]");
				}
			}
		}

		private static org.apache.hadoop.crypto.key.kms.KMSClientProvider[] shuffle(org.apache.hadoop.crypto.key.kms.KMSClientProvider
			[] providers)
		{
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.kms.KMSClientProvider
				> list = java.util.Arrays.asList(providers);
			java.util.Collections.shuffle(list);
			return Sharpen.Collections.ToArray(list, providers);
		}
	}
}
