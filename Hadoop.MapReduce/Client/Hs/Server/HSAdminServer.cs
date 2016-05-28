using System;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Proto;
using Org.Apache.Hadoop.Mapreduce.V2.HS.ProtocolPB;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Security.ProtocolPB;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Tools.Proto;
using Org.Apache.Hadoop.Tools.ProtocolPB;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Server
{
	public class HSAdminServer : AbstractService, HSAdminProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.Server.HSAdminServer
			));

		private AccessControlList adminAcl;

		private AggregatedLogDeletionService aggLogDelService = null;

		/// <summary>The RPC server that listens to requests from clients</summary>
		protected internal RPC.Server clientRpcServer;

		protected internal IPEndPoint clientRpcAddress;

		private const string HistoryAdminServer = "HSAdminServer";

		private JobHistory jobHistoryService = null;

		private UserGroupInformation loginUGI;

		public HSAdminServer(AggregatedLogDeletionService aggLogDelService, JobHistory jobHistoryService
			)
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.Server.HSAdminServer).FullName)
		{
			this.aggLogDelService = aggLogDelService;
			this.jobHistoryService = jobHistoryService;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			RPC.SetProtocolEngine(conf, typeof(RefreshUserMappingsProtocolPB), typeof(ProtobufRpcEngine
				));
			RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator = new 
				RefreshUserMappingsProtocolServerSideTranslatorPB(this);
			BlockingService refreshUserMappingService = RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService
				.NewReflectiveBlockingService(refreshUserMappingXlator);
			GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = new GetUserMappingsProtocolServerSideTranslatorPB
				(this);
			BlockingService getUserMappingService = GetUserMappingsProtocolProtos.GetUserMappingsProtocolService
				.NewReflectiveBlockingService(getUserMappingXlator);
			HSAdminRefreshProtocolServerSideTranslatorPB refreshHSAdminProtocolXlator = new HSAdminRefreshProtocolServerSideTranslatorPB
				(this);
			BlockingService refreshHSAdminProtocolService = HSAdminRefreshProtocolProtos.HSAdminRefreshProtocolService
				.NewReflectiveBlockingService(refreshHSAdminProtocolXlator);
			WritableRpcEngine.EnsureInitialized();
			clientRpcAddress = conf.GetSocketAddr(JHAdminConfig.MrHistoryBindHost, JHAdminConfig
				.JhsAdminAddress, JHAdminConfig.DefaultJhsAdminAddress, JHAdminConfig.DefaultJhsAdminPort
				);
			clientRpcServer = new RPC.Builder(conf).SetProtocol(typeof(RefreshUserMappingsProtocolPB
				)).SetInstance(refreshUserMappingService).SetBindAddress(clientRpcAddress.GetHostName
				()).SetPort(clientRpcAddress.Port).SetVerbose(false).Build();
			AddProtocol(conf, typeof(GetUserMappingsProtocolPB), getUserMappingService);
			AddProtocol(conf, typeof(HSAdminRefreshProtocolPB), refreshHSAdminProtocolService
				);
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				clientRpcServer.RefreshServiceAcl(conf, new ClientHSPolicyProvider());
			}
			adminAcl = new AccessControlList(conf.Get(JHAdminConfig.JhsAdminAcl, JHAdminConfig
				.DefaultJhsAdminAcl));
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (UserGroupInformation.IsSecurityEnabled())
			{
				loginUGI = UserGroupInformation.GetLoginUser();
			}
			else
			{
				loginUGI = UserGroupInformation.GetCurrentUser();
			}
			clientRpcServer.Start();
		}

		[VisibleForTesting]
		internal virtual UserGroupInformation GetLoginUGI()
		{
			return loginUGI;
		}

		[VisibleForTesting]
		internal virtual void SetLoginUGI(UserGroupInformation ugi)
		{
			loginUGI = ugi;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (clientRpcServer != null)
			{
				clientRpcServer.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddProtocol(Configuration conf, Type protocol, BlockingService blockingService
			)
		{
			RPC.SetProtocolEngine(conf, protocol, typeof(ProtobufRpcEngine));
			clientRpcServer.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, protocol, blockingService
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private UserGroupInformation CheckAcls(string method)
		{
			UserGroupInformation user;
			try
			{
				user = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException ioe)
			{
				Log.Warn("Couldn't get current user", ioe);
				HSAuditLogger.LogFailure("UNKNOWN", method, adminAcl.ToString(), HistoryAdminServer
					, "Couldn't get current user");
				throw;
			}
			if (!adminAcl.IsUserAllowed(user))
			{
				Log.Warn("User " + user.GetShortUserName() + " doesn't have permission" + " to call '"
					 + method + "'");
				HSAuditLogger.LogFailure(user.GetShortUserName(), method, adminAcl.ToString(), HistoryAdminServer
					, HSAuditLogger.AuditConstants.UnauthorizedUser);
				throw new AccessControlException("User " + user.GetShortUserName() + " doesn't have permission"
					 + " to call '" + method + "'");
			}
			Log.Info("HS Admin: " + method + " invoked by user " + user.GetShortUserName());
			return user;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string[] GetGroupsForUser(string user)
		{
			return UserGroupInformation.CreateRemoteUser(user).GetGroupNames();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshUserToGroupsMappings()
		{
			UserGroupInformation user = CheckAcls("refreshUserToGroupsMappings");
			Groups.GetUserToGroupsMappingService().Refresh();
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshUserToGroupsMappings", 
				HistoryAdminServer);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshSuperUserGroupsConfiguration()
		{
			UserGroupInformation user = CheckAcls("refreshSuperUserGroupsConfiguration");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(CreateConf());
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshSuperUserGroupsConfiguration"
				, HistoryAdminServer);
		}

		protected internal virtual Configuration CreateConf()
		{
			return new Configuration();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshAdminAcls()
		{
			UserGroupInformation user = CheckAcls("refreshAdminAcls");
			Configuration conf = CreateConf();
			adminAcl = new AccessControlList(conf.Get(JHAdminConfig.JhsAdminAcl, JHAdminConfig
				.DefaultJhsAdminAcl));
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshAdminAcls", HistoryAdminServer
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshLoadedJobCache()
		{
			UserGroupInformation user = CheckAcls("refreshLoadedJobCache");
			try
			{
				jobHistoryService.RefreshLoadedJobCache();
			}
			catch (NotSupportedException e)
			{
				HSAuditLogger.LogFailure(user.GetShortUserName(), "refreshLoadedJobCache", adminAcl
					.ToString(), HistoryAdminServer, e.Message);
				throw;
			}
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshLoadedJobCache", HistoryAdminServer
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshLogRetentionSettings()
		{
			UserGroupInformation user = CheckAcls("refreshLogRetentionSettings");
			try
			{
				loginUGI.DoAs(new _PrivilegedExceptionAction_256(this));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshLogRetentionSettings", 
				"HSAdminServer");
		}

		private sealed class _PrivilegedExceptionAction_256 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_256(HSAdminServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				this._enclosing.aggLogDelService.RefreshLogRetentionSettings();
				return null;
			}

			private readonly HSAdminServer _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshJobRetentionSettings()
		{
			UserGroupInformation user = CheckAcls("refreshJobRetentionSettings");
			try
			{
				loginUGI.DoAs(new _PrivilegedExceptionAction_276(this));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			HSAuditLogger.LogSuccess(user.GetShortUserName(), "refreshJobRetentionSettings", 
				HistoryAdminServer);
		}

		private sealed class _PrivilegedExceptionAction_276 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_276(HSAdminServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Run()
			{
				this._enclosing.jobHistoryService.RefreshJobRetentionSettings();
				return null;
			}

			private readonly HSAdminServer _enclosing;
		}
	}
}
