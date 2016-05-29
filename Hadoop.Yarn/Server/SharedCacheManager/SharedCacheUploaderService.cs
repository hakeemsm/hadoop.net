using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// This service handles all rpc calls from the NodeManager uploader to the
	/// shared cache manager.
	/// </summary>
	public class SharedCacheUploaderService : AbstractService, SCMUploaderProtocol
	{
		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private Org.Apache.Hadoop.Ipc.Server server;

		internal IPEndPoint bindAddress;

		private readonly SCMStore store;

		private SharedCacheUploaderMetrics metrics;

		public SharedCacheUploaderService(SCMStore store)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheUploaderService
				).FullName)
		{
			this.store = store;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.bindAddress = GetBindAddress(conf);
			base.ServiceInit(conf);
		}

		internal virtual IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.ScmUploaderServerAddress, YarnConfiguration
				.DefaultScmUploaderServerAddress, YarnConfiguration.DefaultScmUploaderServerPort
				);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			this.metrics = SharedCacheUploaderMetrics.GetInstance();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = rpc.GetServer(typeof(SCMUploaderProtocol), this, bindAddress, conf, 
				null, conf.GetInt(YarnConfiguration.ScmUploaderServerThreadCount, YarnConfiguration
				.DefaultScmUploaderServerThreadCount));
			// Secret manager null for now (security not supported)
			// TODO (YARN-2774): Enable service authorization
			this.server.Start();
			bindAddress = conf.UpdateConnectAddr(YarnConfiguration.ScmUploaderServerAddress, 
				server.GetListenerAddress());
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.server != null)
			{
				this.server.Stop();
				this.server = null;
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SCMUploaderNotifyResponse Notify(SCMUploaderNotifyRequest request)
		{
			SCMUploaderNotifyResponse response = recordFactory.NewRecordInstance<SCMUploaderNotifyResponse
				>();
			// TODO (YARN-2774): proper security/authorization needs to be implemented
			string filename = store.AddResource(request.GetResourceKey(), request.GetFileName
				());
			bool accepted = filename.Equals(request.GetFileName());
			if (accepted)
			{
				this.metrics.IncAcceptedUploads();
			}
			else
			{
				this.metrics.IncRejectedUploads();
			}
			response.SetAccepted(accepted);
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual SCMUploaderCanUploadResponse CanUpload(SCMUploaderCanUploadRequest
			 request)
		{
			// TODO (YARN-2781): we may want to have a more flexible policy of
			// instructing the node manager to upload only if it meets a certain
			// criteria
			// until then we return true for now
			SCMUploaderCanUploadResponse response = recordFactory.NewRecordInstance<SCMUploaderCanUploadResponse
				>();
			response.SetUploadable(true);
			return response;
		}
	}
}
