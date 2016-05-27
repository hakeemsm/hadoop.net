using System;
using System.IO;
using Com.Google.Common.Cache;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	/// <summary>
	/// A <code>KeyProviderExtension</code> implementation providing a short lived
	/// cache for <code>KeyVersions</code> and <code>Metadata</code>to avoid burst
	/// of requests to hit the underlying <code>KeyProvider</code>.
	/// </summary>
	public class CachingKeyProvider : KeyProviderExtension<CachingKeyProvider.CacheExtension
		>
	{
		internal class CacheExtension : KeyProviderExtension.Extension
		{
			private readonly KeyProvider provider;

			private LoadingCache<string, KeyProvider.KeyVersion> keyVersionCache;

			private LoadingCache<string, KeyProvider.KeyVersion> currentKeyCache;

			private LoadingCache<string, KeyProvider.Metadata> keyMetadataCache;

			internal CacheExtension(KeyProvider prov, long keyTimeoutMillis, long currKeyTimeoutMillis
				)
			{
				this.provider = prov;
				keyVersionCache = CacheBuilder.NewBuilder().ExpireAfterAccess(keyTimeoutMillis, TimeUnit
					.Milliseconds).Build(new _CacheLoader_49(this));
				keyMetadataCache = CacheBuilder.NewBuilder().ExpireAfterAccess(keyTimeoutMillis, 
					TimeUnit.Milliseconds).Build(new _CacheLoader_62(this));
				currentKeyCache = CacheBuilder.NewBuilder().ExpireAfterWrite(currKeyTimeoutMillis
					, TimeUnit.Milliseconds).Build(new _CacheLoader_75(this));
			}

			private sealed class _CacheLoader_49 : CacheLoader<string, KeyProvider.KeyVersion
				>
			{
				public _CacheLoader_49(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override KeyProvider.KeyVersion Load(string key)
				{
					KeyProvider.KeyVersion kv = this._enclosing.provider.GetKeyVersion(key);
					if (kv == null)
					{
						throw new CachingKeyProvider.KeyNotFoundException();
					}
					return kv;
				}

				private readonly CacheExtension _enclosing;
			}

			private sealed class _CacheLoader_62 : CacheLoader<string, KeyProvider.Metadata>
			{
				public _CacheLoader_62(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override KeyProvider.Metadata Load(string key)
				{
					KeyProvider.Metadata meta = this._enclosing.provider.GetMetadata(key);
					if (meta == null)
					{
						throw new CachingKeyProvider.KeyNotFoundException();
					}
					return meta;
				}

				private readonly CacheExtension _enclosing;
			}

			private sealed class _CacheLoader_75 : CacheLoader<string, KeyProvider.KeyVersion
				>
			{
				public _CacheLoader_75(CacheExtension _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.Exception"/>
				public override KeyProvider.KeyVersion Load(string key)
				{
					KeyProvider.KeyVersion kv = this._enclosing.provider.GetCurrentKey(key);
					if (kv == null)
					{
						throw new CachingKeyProvider.KeyNotFoundException();
					}
					return kv;
				}

				private readonly CacheExtension _enclosing;
			}
		}

		[System.Serializable]
		private class KeyNotFoundException : Exception
		{
		}

		public CachingKeyProvider(KeyProvider keyProvider, long keyTimeoutMillis, long currKeyTimeoutMillis
			)
			: base(keyProvider, new CachingKeyProvider.CacheExtension(keyProvider, keyTimeoutMillis
				, currKeyTimeoutMillis))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			try
			{
				return GetExtension().currentKeyCache.Get(name);
			}
			catch (ExecutionException ex)
			{
				Exception cause = ex.InnerException;
				if (cause is CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is IOException)
					{
						throw (IOException)cause;
					}
					else
					{
						throw new IOException(cause);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			try
			{
				return GetExtension().keyVersionCache.Get(versionName);
			}
			catch (ExecutionException ex)
			{
				Exception cause = ex.InnerException;
				if (cause is CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is IOException)
					{
						throw (IOException)cause;
					}
					else
					{
						throw new IOException(cause);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			GetKeyProvider().DeleteKey(name);
			GetExtension().currentKeyCache.Invalidate(name);
			GetExtension().keyMetadataCache.Invalidate(name);
			// invalidating all key versions as we don't know
			// which ones belonged to the deleted key
			GetExtension().keyVersionCache.InvalidateAll();
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			KeyProvider.KeyVersion key = GetKeyProvider().RollNewVersion(name, material);
			GetExtension().currentKeyCache.Invalidate(name);
			GetExtension().keyMetadataCache.Invalidate(name);
			return key;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			KeyProvider.KeyVersion key = GetKeyProvider().RollNewVersion(name);
			GetExtension().currentKeyCache.Invalidate(name);
			GetExtension().keyMetadataCache.Invalidate(name);
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			try
			{
				return GetExtension().keyMetadataCache.Get(name);
			}
			catch (ExecutionException ex)
			{
				Exception cause = ex.InnerException;
				if (cause is CachingKeyProvider.KeyNotFoundException)
				{
					return null;
				}
				else
				{
					if (cause is IOException)
					{
						throw (IOException)cause;
					}
					else
					{
						throw new IOException(cause);
					}
				}
			}
		}
	}
}
