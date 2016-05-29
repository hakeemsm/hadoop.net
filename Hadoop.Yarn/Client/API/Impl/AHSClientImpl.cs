using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class AHSClientImpl : AHSClient
	{
		protected internal ApplicationHistoryProtocol ahsClient;

		protected internal IPEndPoint ahsAddress;

		public AHSClientImpl()
			: base(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.AHSClientImpl).FullName)
		{
		}

		private static IPEndPoint GetAHSAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.TimelineServiceAddress, YarnConfiguration
				.DefaultTimelineServiceAddress, YarnConfiguration.DefaultTimelineServicePort);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.ahsAddress = GetAHSAddress(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				ahsClient = AHSProxy.CreateAHSProxy<ApplicationHistoryProtocol>(GetConfig(), this
					.ahsAddress);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.ahsClient != null)
			{
				RPC.StopProxy(this.ahsClient);
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationReport GetApplicationReport(ApplicationId appId)
		{
			GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appId
				);
			GetApplicationReportResponse response = ahsClient.GetApplicationReport(request);
			return response.GetApplicationReport();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications()
		{
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance(null, null);
			GetApplicationsResponse response = ahsClient.GetApplications(request);
			return response.GetApplicationList();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
			 applicationAttemptId)
		{
			GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
				(applicationAttemptId);
			GetApplicationAttemptReportResponse response = ahsClient.GetApplicationAttemptReport
				(request);
			return response.GetApplicationAttemptReport();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
			 appId)
		{
			GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest.NewInstance
				(appId);
			GetApplicationAttemptsResponse response = ahsClient.GetApplicationAttempts(request
				);
			return response.GetApplicationAttemptList();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ContainerReport GetContainerReport(ContainerId containerId)
		{
			GetContainerReportRequest request = GetContainerReportRequest.NewInstance(containerId
				);
			GetContainerReportResponse response = ahsClient.GetContainerReport(request);
			return response.GetContainerReport();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ContainerReport> GetContainers(ApplicationAttemptId applicationAttemptId
			)
		{
			GetContainersRequest request = GetContainersRequest.NewInstance(applicationAttemptId
				);
			GetContainersResponse response = ahsClient.GetContainers(request);
			return response.GetContainerList();
		}
	}
}
