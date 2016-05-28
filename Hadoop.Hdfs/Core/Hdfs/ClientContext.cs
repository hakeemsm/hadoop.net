using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>ClientContext contains context information for a client.</summary>
	/// <remarks>
	/// ClientContext contains context information for a client.
	/// This allows us to share caches such as the socket cache across
	/// DFSClient instances.
	/// </remarks>
	public class ClientContext
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.ClientContext
			));

		/// <summary>Global map of context names to caches contexts.</summary>
		private static readonly Dictionary<string, Org.Apache.Hadoop.Hdfs.ClientContext> 
			Caches = new Dictionary<string, Org.Apache.Hadoop.Hdfs.ClientContext>();

		/// <summary>Name of context.</summary>
		private readonly string name;

		/// <summary>String representation of the configuration.</summary>
		private readonly string confString;

		/// <summary>Caches short-circuit file descriptors, mmap regions.</summary>
		private readonly ShortCircuitCache shortCircuitCache;

		/// <summary>Caches TCP and UNIX domain sockets for reuse.</summary>
		private readonly PeerCache peerCache;

		/// <summary>Stores information about socket paths.</summary>
		private readonly DomainSocketFactory domainSocketFactory;

		/// <summary>Caches key Providers for the DFSClient</summary>
		private readonly KeyProviderCache keyProviderCache;

		/// <summary>True if we should use the legacy BlockReaderLocal.</summary>
		private readonly bool useLegacyBlockReaderLocal;

		/// <summary>True if the legacy BlockReaderLocal is disabled.</summary>
		/// <remarks>
		/// True if the legacy BlockReaderLocal is disabled.
		/// The legacy block reader local gets disabled completely whenever there is an
		/// error or miscommunication.  The new block reader local code handles this
		/// case more gracefully inside DomainSocketFactory.
		/// </remarks>
		private volatile bool disableLegacyBlockReaderLocal = false;

		/// <summary>
		/// Creating byte[] for
		/// <see cref="DFSOutputStream"/>
		/// .
		/// </summary>
		private readonly ByteArrayManager byteArrayManager;

		/// <summary>
		/// Whether or not we complained about a DFSClient fetching a CacheContext that
		/// didn't match its config values yet.
		/// </summary>
		private bool printedConfWarning = false;

		private ClientContext(string name, DFSClient.Conf conf)
		{
			this.name = name;
			this.confString = ConfAsString(conf);
			this.shortCircuitCache = new ShortCircuitCache(conf.shortCircuitStreamsCacheSize, 
				conf.shortCircuitStreamsCacheExpiryMs, conf.shortCircuitMmapCacheSize, conf.shortCircuitMmapCacheExpiryMs
				, conf.shortCircuitMmapCacheRetryTimeout, conf.shortCircuitCacheStaleThresholdMs
				, conf.shortCircuitSharedMemoryWatcherInterruptCheckMs);
			this.peerCache = new PeerCache(conf.socketCacheCapacity, conf.socketCacheExpiry);
			this.keyProviderCache = new KeyProviderCache(conf.keyProviderCacheExpiryMs);
			this.useLegacyBlockReaderLocal = conf.useLegacyBlockReaderLocal;
			this.domainSocketFactory = new DomainSocketFactory(conf);
			this.byteArrayManager = ByteArrayManager.NewInstance(conf.writeByteArrayManagerConf
				);
		}

		public static string ConfAsString(DFSClient.Conf conf)
		{
			StringBuilder builder = new StringBuilder();
			builder.Append("shortCircuitStreamsCacheSize = ").Append(conf.shortCircuitStreamsCacheSize
				).Append(", shortCircuitStreamsCacheExpiryMs = ").Append(conf.shortCircuitStreamsCacheExpiryMs
				).Append(", shortCircuitMmapCacheSize = ").Append(conf.shortCircuitMmapCacheSize
				).Append(", shortCircuitMmapCacheExpiryMs = ").Append(conf.shortCircuitMmapCacheExpiryMs
				).Append(", shortCircuitMmapCacheRetryTimeout = ").Append(conf.shortCircuitMmapCacheRetryTimeout
				).Append(", shortCircuitCacheStaleThresholdMs = ").Append(conf.shortCircuitCacheStaleThresholdMs
				).Append(", socketCacheCapacity = ").Append(conf.socketCacheCapacity).Append(", socketCacheExpiry = "
				).Append(conf.socketCacheExpiry).Append(", shortCircuitLocalReads = ").Append(conf
				.shortCircuitLocalReads).Append(", useLegacyBlockReaderLocal = ").Append(conf.useLegacyBlockReaderLocal
				).Append(", domainSocketDataTraffic = ").Append(conf.domainSocketDataTraffic).Append
				(", shortCircuitSharedMemoryWatcherInterruptCheckMs = ").Append(conf.shortCircuitSharedMemoryWatcherInterruptCheckMs
				).Append(", keyProviderCacheExpiryMs = ").Append(conf.keyProviderCacheExpiryMs);
			return builder.ToString();
		}

		public static Org.Apache.Hadoop.Hdfs.ClientContext Get(string name, DFSClient.Conf
			 conf)
		{
			Org.Apache.Hadoop.Hdfs.ClientContext context;
			lock (typeof(Org.Apache.Hadoop.Hdfs.ClientContext))
			{
				context = Caches[name];
				if (context == null)
				{
					context = new Org.Apache.Hadoop.Hdfs.ClientContext(name, conf);
					Caches[name] = context;
				}
				else
				{
					context.PrintConfWarningIfNeeded(conf);
				}
			}
			return context;
		}

		/// <summary>Get a client context, from a Configuration object.</summary>
		/// <remarks>
		/// Get a client context, from a Configuration object.
		/// This method is less efficient than the version which takes a DFSClient#Conf
		/// object, and should be mostly used by tests.
		/// </remarks>
		[VisibleForTesting]
		public static Org.Apache.Hadoop.Hdfs.ClientContext GetFromConf(Configuration conf
			)
		{
			return Get(conf.Get(DFSConfigKeys.DfsClientContext, DFSConfigKeys.DfsClientContextDefault
				), new DFSClient.Conf(conf));
		}

		private void PrintConfWarningIfNeeded(DFSClient.Conf conf)
		{
			string existing = this.GetConfString();
			string requested = ConfAsString(conf);
			if (!existing.Equals(requested))
			{
				if (!printedConfWarning)
				{
					printedConfWarning = true;
					Log.Warn("Existing client context '" + name + "' does not match " + "requested configuration.  Existing: "
						 + existing + ", Requested: " + requested);
				}
			}
		}

		public virtual string GetConfString()
		{
			return confString;
		}

		public virtual ShortCircuitCache GetShortCircuitCache()
		{
			return shortCircuitCache;
		}

		public virtual PeerCache GetPeerCache()
		{
			return peerCache;
		}

		public virtual KeyProviderCache GetKeyProviderCache()
		{
			return keyProviderCache;
		}

		public virtual bool GetUseLegacyBlockReaderLocal()
		{
			return useLegacyBlockReaderLocal;
		}

		public virtual bool GetDisableLegacyBlockReaderLocal()
		{
			return disableLegacyBlockReaderLocal;
		}

		public virtual void SetDisableLegacyBlockReaderLocal()
		{
			disableLegacyBlockReaderLocal = true;
		}

		public virtual DomainSocketFactory GetDomainSocketFactory()
		{
			return domainSocketFactory;
		}

		public virtual ByteArrayManager GetByteArrayManager()
		{
			return byteArrayManager;
		}
	}
}
