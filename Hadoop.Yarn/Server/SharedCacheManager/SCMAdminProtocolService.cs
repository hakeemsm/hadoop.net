using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// This service handles all SCMAdminProtocol rpc calls from administrators
	/// to the shared cache manager.
	/// </summary>
	public class SCMAdminProtocolService : AbstractService, SCMAdminProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SCMAdminProtocolService
			));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private Org.Apache.Hadoop.Ipc.Server server;

		internal IPEndPoint clientBindAddress;

		private readonly CleanerService cleanerService;

		private YarnAuthorizationProvider authorizer;

		public SCMAdminProtocolService(CleanerService cleanerService)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SCMAdminProtocolService
				).FullName)
		{
			this.cleanerService = cleanerService;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.clientBindAddress = GetBindAddress(conf);
			authorizer = YarnAuthorizationProvider.GetInstance(conf);
			base.ServiceInit(conf);
		}

		internal virtual IPEndPoint GetBindAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.ScmAdminAddress, YarnConfiguration.DefaultScmAdminAddress
				, YarnConfiguration.DefaultScmAdminPort);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			this.server = rpc.GetServer(typeof(SCMAdminProtocol), this, clientBindAddress, conf
				, null, conf.GetInt(YarnConfiguration.ScmAdminClientThreadCount, YarnConfiguration
				.DefaultScmAdminClientThreadCount));
			// Secret manager null for now (security not supported)
			// TODO: Enable service authorization (see YARN-2774)
			this.server.Start();
			clientBindAddress = conf.UpdateConnectAddr(YarnConfiguration.ScmAdminAddress, server
				.GetListenerAddress());
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
		private void CheckAcls(string method)
		{
			UserGroupInformation user;
			try
			{
				user = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ioe)
			{
				Log.Warn("Couldn't get current user", ioe);
				throw RPCUtil.GetRemoteException(ioe);
			}
			if (!authorizer.IsAdmin(user))
			{
				Log.Warn("User " + user.GetShortUserName() + " doesn't have permission" + " to call '"
					 + method + "'");
				throw RPCUtil.GetRemoteException(new AccessControlException("User " + user.GetShortUserName
					() + " doesn't have permission" + " to call '" + method + "'"));
			}
			Log.Info("SCM Admin: " + method + " invoked by user " + user.GetShortUserName());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual RunSharedCacheCleanerTaskResponse RunCleanerTask(RunSharedCacheCleanerTaskRequest
			 request)
		{
			CheckAcls("runCleanerTask");
			RunSharedCacheCleanerTaskResponse response = recordFactory.NewRecordInstance<RunSharedCacheCleanerTaskResponse
				>();
			this.cleanerService.RunCleanerTask();
			// if we are here, then we have submitted the request to the cleaner
			// service, ack the request to the admin client
			response.SetAccepted(true);
			return response;
		}
	}
}
