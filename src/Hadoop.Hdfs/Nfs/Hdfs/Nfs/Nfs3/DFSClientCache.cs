using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>A cache saves DFSClient objects for different users</summary>
	internal class DFSClientCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.DFSClientCache
			));

		/// <summary>Cache that maps User id to the corresponding DFSClient.</summary>
		[VisibleForTesting]
		internal readonly LoadingCache<string, DFSClient> clientCache;

		internal const int DefaultDfsClientCacheSize = 256;

		/// <summary>
		/// Cache that maps <DFSClient, inode path> to the corresponding
		/// FSDataInputStream.
		/// </summary>
		internal readonly LoadingCache<DFSClientCache.DFSInputStreamCaheKey, FSDataInputStream
			> inputstreamCache;

		/// <summary>Time to live for a DFSClient (in seconds)</summary>
		internal const int DefaultDfsInputstreamCacheSize = 1024;

		internal const int DefaultDfsInputstreamCacheTtl = 10 * 60;

		private readonly NfsConfiguration config;

		private class DFSInputStreamCaheKey
		{
			internal readonly string userId;

			internal readonly string inodePath;

			private DFSInputStreamCaheKey(string userId, string inodePath)
				: base()
			{
				this.userId = userId;
				this.inodePath = inodePath;
			}

			public override bool Equals(object obj)
			{
				if (obj is DFSClientCache.DFSInputStreamCaheKey)
				{
					DFSClientCache.DFSInputStreamCaheKey k = (DFSClientCache.DFSInputStreamCaheKey)obj;
					return userId.Equals(k.userId) && inodePath.Equals(k.inodePath);
				}
				return false;
			}

			public override int GetHashCode()
			{
				return Objects.HashCode(userId, inodePath);
			}
		}

		internal DFSClientCache(NfsConfiguration config)
			: this(config, DefaultDfsClientCacheSize)
		{
		}

		internal DFSClientCache(NfsConfiguration config, int clientCache)
		{
			this.config = config;
			this.clientCache = CacheBuilder.NewBuilder().MaximumSize(clientCache).RemovalListener
				(ClientRemovalListener()).Build(ClientLoader());
			this.inputstreamCache = CacheBuilder.NewBuilder().MaximumSize(DefaultDfsInputstreamCacheSize
				).ExpireAfterAccess(DefaultDfsInputstreamCacheTtl, TimeUnit.Seconds).RemovalListener
				(InputStreamRemovalListener()).Build(InputStreamLoader());
			ShutdownHookManager.Get().AddShutdownHook(new DFSClientCache.CacheFinalizer(this)
				, ShutdownHookPriority);
		}

		/// <summary>Priority of the FileSystem shutdown hook.</summary>
		public const int ShutdownHookPriority = 10;

		private class CacheFinalizer : Runnable
		{
			public virtual void Run()
			{
				lock (this)
				{
					try
					{
						this._enclosing.CloseAll(true);
					}
					catch (IOException e)
					{
						DFSClientCache.Log.Info("DFSClientCache.closeAll() threw an exception:\n", e);
					}
				}
			}

			internal CacheFinalizer(DFSClientCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DFSClientCache _enclosing;
		}

		/// <summary>Close all DFSClient instances in the Cache.</summary>
		/// <param name="onlyAutomatic">only close those that are marked for automatic closing
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CloseAll(bool onlyAutomatic)
		{
			lock (this)
			{
				IList<IOException> exceptions = new AList<IOException>();
				ConcurrentMap<string, DFSClient> map = clientCache.AsMap();
				foreach (KeyValuePair<string, DFSClient> item in map)
				{
					DFSClient client = item.Value;
					if (client != null)
					{
						try
						{
							client.Close();
						}
						catch (IOException ioe)
						{
							exceptions.AddItem(ioe);
						}
					}
				}
				if (!exceptions.IsEmpty())
				{
					throw MultipleIOException.CreateIOException(exceptions);
				}
			}
		}

		private CacheLoader<string, DFSClient> ClientLoader()
		{
			return new _CacheLoader_165(this);
		}

		private sealed class _CacheLoader_165 : CacheLoader<string, DFSClient>
		{
			public _CacheLoader_165(DFSClientCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override DFSClient Load(string userName)
			{
				UserGroupInformation ugi = this._enclosing.GetUserGroupInformation(userName, UserGroupInformation
					.GetCurrentUser());
				// Guava requires CacheLoader never returns null.
				return ugi.DoAs(new _PrivilegedExceptionAction_173(this));
			}

			private sealed class _PrivilegedExceptionAction_173 : PrivilegedExceptionAction<DFSClient
				>
			{
				public _PrivilegedExceptionAction_173(_CacheLoader_165 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				public DFSClient Run()
				{
					return new DFSClient(NameNode.GetAddress(this._enclosing._enclosing.config), this
						._enclosing._enclosing.config);
				}

				private readonly _CacheLoader_165 _enclosing;
			}

			private readonly DFSClientCache _enclosing;
		}

		/// <summary>This method uses the currentUser, and real user to create a proxy</summary>
		/// <param name="effectiveUser">The user who is being proxied by the real user</param>
		/// <param name="realUser">The actual user who does the command</param>
		/// <returns>Proxy UserGroupInformation</returns>
		/// <exception cref="System.IO.IOException">If proxying fails</exception>
		internal virtual UserGroupInformation GetUserGroupInformation(string effectiveUser
			, UserGroupInformation realUser)
		{
			Preconditions.CheckNotNull(effectiveUser);
			Preconditions.CheckNotNull(realUser);
			realUser.CheckTGTAndReloginFromKeytab();
			UserGroupInformation ugi = UserGroupInformation.CreateProxyUser(effectiveUser, realUser
				);
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("Created ugi:" + " %s for username: %s", ugi, effectiveUser
					));
			}
			return ugi;
		}

		private RemovalListener<string, DFSClient> ClientRemovalListener()
		{
			return new _RemovalListener_208();
		}

		private sealed class _RemovalListener_208 : RemovalListener<string, DFSClient>
		{
			public _RemovalListener_208()
			{
			}

			public void OnRemoval(RemovalNotification<string, DFSClient> notification)
			{
				DFSClient client = notification.Value;
				try
				{
					client.Close();
				}
				catch (IOException e)
				{
					DFSClientCache.Log.Warn(string.Format("IOException when closing the DFSClient(%s), cause: %s"
						, client, e));
				}
			}
		}

		private RemovalListener<DFSClientCache.DFSInputStreamCaheKey, FSDataInputStream> 
			InputStreamRemovalListener()
		{
			return new _RemovalListener_224();
		}

		private sealed class _RemovalListener_224 : RemovalListener<DFSClientCache.DFSInputStreamCaheKey
			, FSDataInputStream>
		{
			public _RemovalListener_224()
			{
			}

			public void OnRemoval(RemovalNotification<DFSClientCache.DFSInputStreamCaheKey, FSDataInputStream
				> notification)
			{
				try
				{
					notification.Value.Close();
				}
				catch (IOException)
				{
				}
			}
		}

		private CacheLoader<DFSClientCache.DFSInputStreamCaheKey, FSDataInputStream> InputStreamLoader
			()
		{
			return new _CacheLoader_238(this);
		}

		private sealed class _CacheLoader_238 : CacheLoader<DFSClientCache.DFSInputStreamCaheKey
			, FSDataInputStream>
		{
			public _CacheLoader_238(DFSClientCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public override FSDataInputStream Load(DFSClientCache.DFSInputStreamCaheKey key)
			{
				DFSClient client = this._enclosing.GetDfsClient(key.userId);
				DFSInputStream dis = client.Open(key.inodePath);
				return client.CreateWrappedInputStream(dis);
			}

			private readonly DFSClientCache _enclosing;
		}

		internal virtual DFSClient GetDfsClient(string userName)
		{
			DFSClient client = null;
			try
			{
				client = clientCache.Get(userName);
			}
			catch (ExecutionException e)
			{
				Log.Error("Failed to create DFSClient for user:" + userName + " Cause:" + e);
			}
			return client;
		}

		internal virtual FSDataInputStream GetDfsInputStream(string userName, string inodePath
			)
		{
			DFSClientCache.DFSInputStreamCaheKey k = new DFSClientCache.DFSInputStreamCaheKey
				(userName, inodePath);
			FSDataInputStream s = null;
			try
			{
				s = inputstreamCache.Get(k);
			}
			catch (ExecutionException e)
			{
				Log.Warn("Failed to create DFSInputStream for user:" + userName + " Cause:" + e);
			}
			return s;
		}

		public virtual void InvalidateDfsInputStream(string userName, string inodePath)
		{
			DFSClientCache.DFSInputStreamCaheKey k = new DFSClientCache.DFSInputStreamCaheKey
				(userName, inodePath);
			inputstreamCache.Invalidate(k);
		}
	}
}
