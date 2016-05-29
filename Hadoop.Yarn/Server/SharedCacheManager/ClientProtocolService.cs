using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Sharedcache;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// This service handles all rpc calls from the client to the shared cache
	/// manager.
	/// </summary>
	public class ClientProtocolService : AbstractService, ClientSCMProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.ClientProtocolService
			));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private Org.Apache.Hadoop.Ipc.Server server;

		internal IPEndPoint clientBindAddress;

		private readonly SCMStore store;

		private int cacheDepth;

		private string cacheRoot;

		private ClientSCMMetrics metrics;

		public ClientProtocolService(SCMStore store)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.ClientProtocolService
				).FullName)
		{
			this.store = store;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.clientBindAddress = GetBindAddress(conf);
			this.cacheDepth = SharedCacheUtil.GetCacheDepth(conf);
			this.cacheRoot = conf.Get(YarnConfiguration.SharedCacheRoot, YarnConfiguration.DefaultSharedCacheRoot
				);
			base.ServiceInit(conf);
		}

		internal virtual IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.ScmClientServerAddress, YarnConfiguration
				.DefaultScmClientServerAddress, YarnConfiguration.DefaultScmClientServerPort);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			this.metrics = ClientSCMMetrics.GetInstance();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = rpc.GetServer(typeof(ClientSCMProtocol), this, clientBindAddress, conf
				, null, conf.GetInt(YarnConfiguration.ScmClientServerThreadCount, YarnConfiguration
				.DefaultScmClientServerThreadCount));
			// Secret manager null for now (security not supported)
			// TODO (YARN-2774): Enable service authorization
			this.server.Start();
			clientBindAddress = conf.UpdateConnectAddr(YarnConfiguration.ScmClientServerAddress
				, server.GetListenerAddress());
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.server != null)
			{
				this.server.Stop();
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual UseSharedCacheResourceResponse Use(UseSharedCacheResourceRequest request
			)
		{
			UseSharedCacheResourceResponse response = recordFactory.NewRecordInstance<UseSharedCacheResourceResponse
				>();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			string fileName = this.store.AddResourceReference(request.GetResourceKey(), new SharedCacheResourceReference
				(request.GetAppId(), callerUGI.GetShortUserName()));
			if (fileName != null)
			{
				response.SetPath(GetCacheEntryFilePath(request.GetResourceKey(), fileName));
				this.metrics.IncCacheHitCount();
			}
			else
			{
				this.metrics.IncCacheMissCount();
			}
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual ReleaseSharedCacheResourceResponse Release(ReleaseSharedCacheResourceRequest
			 request)
		{
			ReleaseSharedCacheResourceResponse response = recordFactory.NewRecordInstance<ReleaseSharedCacheResourceResponse
				>();
			UserGroupInformation callerUGI;
			try
			{
				callerUGI = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ie)
			{
				Log.Info("Error getting UGI ", ie);
				throw RPCUtil.GetRemoteException(ie);
			}
			bool removed = this.store.RemoveResourceReference(request.GetResourceKey(), new SharedCacheResourceReference
				(request.GetAppId(), callerUGI.GetShortUserName()), true);
			if (removed)
			{
				this.metrics.IncCacheRelease();
			}
			return response;
		}

		private string GetCacheEntryFilePath(string checksum, string filename)
		{
			return SharedCacheUtil.GetCacheEntryPath(this.cacheDepth, this.cacheRoot, checksum
				) + Path.SeparatorChar + filename;
		}
	}
}
