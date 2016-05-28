using System;
using Com.Google.Common.Annotations;
using Com.Google.Common.Cache;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class KeyProviderCache
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.KeyProviderCache
			));

		private readonly Com.Google.Common.Cache.Cache<URI, KeyProvider> cache;

		public KeyProviderCache(long expiryMs)
		{
			cache = CacheBuilder.NewBuilder().ExpireAfterAccess(expiryMs, TimeUnit.Milliseconds
				).RemovalListener(new _RemovalListener_47()).Build();
		}

		private sealed class _RemovalListener_47 : RemovalListener<URI, KeyProvider>
		{
			public _RemovalListener_47()
			{
			}

			public void OnRemoval(RemovalNotification<URI, KeyProvider> notification)
			{
				try
				{
					notification.Value.Close();
				}
				catch (Exception e)
				{
					Org.Apache.Hadoop.Hdfs.KeyProviderCache.Log.Error("Error closing KeyProvider with uri ["
						 + notification.Key + "]", e);
				}
			}
		}

		public virtual KeyProvider Get(Configuration conf)
		{
			URI kpURI = CreateKeyProviderURI(conf);
			if (kpURI == null)
			{
				return null;
			}
			try
			{
				return cache.Get(kpURI, new _Callable_70(conf));
			}
			catch (Exception e)
			{
				Log.Error("Could not create KeyProvider for DFSClient !!", e.InnerException);
				return null;
			}
		}

		private sealed class _Callable_70 : Callable<KeyProvider>
		{
			public _Callable_70(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider Call()
			{
				return DFSUtil.CreateKeyProvider(conf);
			}

			private readonly Configuration conf;
		}

		private URI CreateKeyProviderURI(Configuration conf)
		{
			string providerUriStr = conf.GetTrimmed(DFSConfigKeys.DfsEncryptionKeyProviderUri
				, string.Empty);
			// No provider set in conf
			if (providerUriStr.IsEmpty())
			{
				Log.Error("Could not find uri with key [" + DFSConfigKeys.DfsEncryptionKeyProviderUri
					 + "] to create a keyProvider !!");
				return null;
			}
			URI providerUri;
			try
			{
				providerUri = new URI(providerUriStr);
			}
			catch (URISyntaxException e)
			{
				Log.Error("KeyProvider URI string is invalid [" + providerUriStr + "]!!", e.InnerException
					);
				return null;
			}
			return providerUri;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual void SetKeyProvider(Configuration conf, KeyProvider keyProvider)
		{
			URI uri = CreateKeyProviderURI(conf);
			cache.Put(uri, keyProvider);
		}
	}
}
