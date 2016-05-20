using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>
	/// A <code>KeyProviderExtension</code> implementation providing a short lived
	/// cache for <code>KeyVersions</code> and <code>Metadata</code>to avoid burst
	/// of requests to hit the underlying <code>KeyProvider</code>.
	/// </summary>
	public class CachingKeyProvider : org.apache.hadoop.crypto.key.KeyProviderExtension
		<org.apache.hadoop.crypto.key.CachingKeyProvider.CacheExtension>
	{
		internal class CacheExtension : org.apache.hadoop.crypto.key.KeyProviderExtension.Extension
		{
			private readonly org.apache.hadoop.crypto.key.KeyProvider provider;

			private com.google.common.cache.LoadingCache<string, org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> keyVersionCache;

			private com.google.common.cache.LoadingCache<string, org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> currentKeyCache;

			private com.google.common.cache.LoadingCache<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
				> keyMetadataCache;

			internal CacheExtension(org.apache.hadoop.crypto.key.KeyProvider prov, long keyTimeoutMillis
				, long currKeyTimeoutMillis)
			{
				this.provider = prov;
				keyVersionCache = com.google.common.cache.CacheBuilder.newBuilder().expireAfterAccess
					(keyTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS).build(new _CacheLoader_49
					(this));
				keyMetadataCache = com.google.common.cache.CacheBuilder.newBuilder().expireAfterAccess
					(keyTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS).build(new _CacheLoader_62
					(this));
				currentKeyCache = com.google.common.cache.CacheBuilder.newBuilder().expireAfterWrite
					(currKeyTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS).build(new _CacheLoader_75
					(this));
			}

			private sealed class _CacheLoader_49 : com.google.common.cache.CacheLoader<string
				, org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
			{
				public _CacheLoader_49(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion load(string key
					)
				{
					org.apache.hadoop.crypto.key.KeyProvider.KeyVersion kv = this._enclosing.provider
						.getKeyVersion(key);
					if (kv == null)
					{
						throw new org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException();
					}
					return kv;
				}

				private readonly CacheExtension _enclosing;
			}

			private sealed class _CacheLoader_62 : com.google.common.cache.CacheLoader<string
				, org.apache.hadoop.crypto.key.KeyProvider.Metadata>
			{
				public _CacheLoader_62(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override org.apache.hadoop.crypto.key.KeyProvider.Metadata load(string key
					)
				{
					org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = this._enclosing.provider
						.getMetadata(key);
					if (meta == null)
					{
						throw new org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException();
					}
					return meta;
				}

				private readonly CacheExtension _enclosing;
			}

			private sealed class _CacheLoader_75 : com.google.common.cache.CacheLoader<string
				, org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>
			{
				public _CacheLoader_75(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion load(string key
					)
				{
					org.apache.hadoop.crypto.key.KeyProvider.KeyVersion kv = this._enclosing.provider
						.getCurrentKey(key);
					if (kv == null)
					{
						throw new org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException();
					}
					return kv;
				}

				private readonly CacheExtension _enclosing;
			}
		}

		[System.Serializable]
		private class KeyNotFoundException : System.Exception
		{
		}

		public CachingKeyProvider(org.apache.hadoop.crypto.key.KeyProvider keyProvider, long
			 keyTimeoutMillis, long currKeyTimeoutMillis)
			: base(keyProvider, new org.apache.hadoop.crypto.key.CachingKeyProvider.CacheExtension
				(keyProvider, keyTimeoutMillis, currKeyTimeoutMillis))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getCurrentKey
			(string name)
		{
			try
			{
				return getExtension().currentKeyCache.get(name);
			}
			catch (java.util.concurrent.ExecutionException ex)
			{
				System.Exception cause = ex.InnerException;
				if (cause is org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is System.IO.IOException)
					{
						throw (System.IO.IOException)cause;
					}
					else
					{
						throw new System.IO.IOException(cause);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			try
			{
				return getExtension().keyVersionCache.get(versionName);
			}
			catch (java.util.concurrent.ExecutionException ex)
			{
				System.Exception cause = ex.InnerException;
				if (cause is org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is System.IO.IOException)
					{
						throw (System.IO.IOException)cause;
					}
					else
					{
						throw new System.IO.IOException(cause);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			getKeyProvider().deleteKey(name);
			getExtension().currentKeyCache.invalidate(name);
			getExtension().keyMetadataCache.invalidate(name);
			// invalidating all key versions as we don't know
			// which ones belonged to the deleted key
			getExtension().keyVersionCache.invalidateAll();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion key = getKeyProvider().rollNewVersion
				(name, material);
			getExtension().currentKeyCache.invalidate(name);
			getExtension().keyMetadataCache.invalidate(name);
			return key;
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name)
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion key = getKeyProvider().rollNewVersion
				(name);
			getExtension().currentKeyCache.invalidate(name);
			getExtension().keyMetadataCache.invalidate(name);
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			try
			{
				return getExtension().keyMetadataCache.get(name);
			}
			catch (java.util.concurrent.ExecutionException ex)
			{
				System.Exception cause = ex.InnerException;
				if (cause is org.apache.hadoop.crypto.key.CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is System.IO.IOException)
					{
						throw (System.IO.IOException)cause;
					}
					else
					{
						throw new System.IO.IOException(cause);
					}
				}
			}
		}
	}
}
