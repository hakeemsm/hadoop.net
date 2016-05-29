using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class ApplicationHistoryClientService : AbstractService, ApplicationHistoryProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.ApplicationHistoryClientService
			));

		private ApplicationHistoryManager history;

		private Org.Apache.Hadoop.Ipc.Server server;

		private IPEndPoint bindAddress;

		public ApplicationHistoryClientService(ApplicationHistoryManager history)
			: base("ApplicationHistoryClientService")
		{
			this.history = history;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint address = conf.GetSocketAddr(YarnConfiguration.TimelineServiceBindHost
				, YarnConfiguration.TimelineServiceAddress, YarnConfiguration.DefaultTimelineServiceAddress
				, YarnConfiguration.DefaultTimelineServicePort);
			Preconditions.CheckArgument(conf.GetInt(YarnConfiguration.TimelineServiceHandlerThreadCount
				, YarnConfiguration.DefaultTimelineServiceClientThreadCount) > 0, "%s property value should be greater than zero"
				, YarnConfiguration.TimelineServiceHandlerThreadCount);
			server = rpc.GetServer(typeof(ApplicationHistoryProtocol), this, address, conf, null
				, conf.GetInt(YarnConfiguration.TimelineServiceHandlerThreadCount, YarnConfiguration
				.DefaultTimelineServiceClientThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				RefreshServiceAcls(conf, new TimelinePolicyProvider());
			}
			server.Start();
			this.bindAddress = conf.UpdateConnectAddr(YarnConfiguration.TimelineServiceBindHost
				, YarnConfiguration.TimelineServiceAddress, YarnConfiguration.DefaultTimelineServiceAddress
				, server.GetListenerAddress());
			Log.Info("Instantiated ApplicationHistoryClientService at " + this.bindAddress);
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (server != null)
			{
				server.Stop();
			}
			base.ServiceStop();
		}

		[InterfaceAudience.Private]
		public virtual IPEndPoint GetBindAddress()
		{
			return this.bindAddress;
		}

		private void RefreshServiceAcls(Configuration configuration, PolicyProvider policyProvider
			)
		{
			this.server.RefreshServiceAcl(configuration, policyProvider);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
			 request)
		{
			// TODO Auto-generated method stub
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptReportResponse GetApplicationAttemptReport(GetApplicationAttemptReportRequest
			 request)
		{
			ApplicationAttemptId appAttemptId = request.GetApplicationAttemptId();
			try
			{
				GetApplicationAttemptReportResponse response = GetApplicationAttemptReportResponse
					.NewInstance(history.GetApplicationAttempt(appAttemptId));
				return response;
			}
			catch (IOException e)
			{
				Log.Error(e.Message, e);
				throw;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationAttemptsResponse GetApplicationAttempts(GetApplicationAttemptsRequest
			 request)
		{
			GetApplicationAttemptsResponse response = GetApplicationAttemptsResponse.NewInstance
				(new AList<ApplicationAttemptReport>(history.GetApplicationAttempts(request.GetApplicationId
				()).Values));
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationReportResponse GetApplicationReport(GetApplicationReportRequest
			 request)
		{
			ApplicationId applicationId = request.GetApplicationId();
			try
			{
				GetApplicationReportResponse response = GetApplicationReportResponse.NewInstance(
					history.GetApplication(applicationId));
				return response;
			}
			catch (IOException e)
			{
				Log.Error(e.Message, e);
				throw;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetApplicationsResponse GetApplications(GetApplicationsRequest request
			)
		{
			GetApplicationsResponse response = GetApplicationsResponse.NewInstance(new AList<
				ApplicationReport>(history.GetApplications(request.GetLimit()).Values));
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainerReportResponse GetContainerReport(GetContainerReportRequest
			 request)
		{
			ContainerId containerId = request.GetContainerId();
			try
			{
				GetContainerReportResponse response = GetContainerReportResponse.NewInstance(history
					.GetContainer(containerId));
				return response;
			}
			catch (IOException e)
			{
				Log.Error(e.Message, e);
				throw;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetContainersResponse GetContainers(GetContainersRequest request)
		{
			GetContainersResponse response = GetContainersResponse.NewInstance(new AList<ContainerReport
				>(history.GetContainers(request.GetApplicationAttemptId()).Values));
			return response;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
			 request)
		{
			return null;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
			 request)
		{
			return null;
		}
	}
}
