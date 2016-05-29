using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class YarnClientImpl : YarnClient
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.YarnClientImpl
			));

		protected internal ApplicationClientProtocol rmClient;

		protected internal long submitPollIntervalMillis;

		private long asyncApiPollIntervalMillis;

		private long asyncApiPollTimeoutMillis;

		protected internal AHSClient historyClient;

		private bool historyServiceEnabled;

		protected internal TimelineClient timelineClient;

		[VisibleForTesting]
		internal Text timelineService;

		[VisibleForTesting]
		internal string timelineDTRenewer;

		protected internal bool timelineServiceEnabled;

		protected internal bool timelineServiceBestEffort;

		private const string Root = "root";

		public YarnClientImpl()
			: base(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.YarnClientImpl).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			asyncApiPollIntervalMillis = conf.GetLong(YarnConfiguration.YarnClientApplicationClientProtocolPollIntervalMs
				, YarnConfiguration.DefaultYarnClientApplicationClientProtocolPollIntervalMs);
			asyncApiPollTimeoutMillis = conf.GetLong(YarnConfiguration.YarnClientApplicationClientProtocolPollTimeoutMs
				, YarnConfiguration.DefaultYarnClientApplicationClientProtocolPollTimeoutMs);
			submitPollIntervalMillis = asyncApiPollIntervalMillis;
			if (conf.Get(YarnConfiguration.YarnClientAppSubmissionPollIntervalMs) != null)
			{
				submitPollIntervalMillis = conf.GetLong(YarnConfiguration.YarnClientAppSubmissionPollIntervalMs
					, YarnConfiguration.DefaultYarnClientApplicationClientProtocolPollIntervalMs);
			}
			if (conf.GetBoolean(YarnConfiguration.ApplicationHistoryEnabled, YarnConfiguration
				.DefaultApplicationHistoryEnabled))
			{
				historyServiceEnabled = true;
				historyClient = AHSClient.CreateAHSClient();
				historyClient.Init(conf);
			}
			if (conf.GetBoolean(YarnConfiguration.TimelineServiceEnabled, YarnConfiguration.DefaultTimelineServiceEnabled
				))
			{
				timelineServiceEnabled = true;
				timelineClient = CreateTimelineClient();
				timelineClient.Init(conf);
				timelineDTRenewer = GetTimelineDelegationTokenRenewer(conf);
				timelineService = TimelineUtils.BuildTimelineTokenService(conf);
			}
			timelineServiceBestEffort = conf.GetBoolean(YarnConfiguration.TimelineServiceClientBestEffort
				, YarnConfiguration.DefaultTimelineServiceClientBestEffort);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		internal virtual TimelineClient CreateTimelineClient()
		{
			return TimelineClient.CreateTimelineClient();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				rmClient = ClientRMProxy.CreateRMProxy<ApplicationClientProtocol>(GetConfig());
				if (historyServiceEnabled)
				{
					historyClient.Start();
				}
				if (timelineServiceEnabled)
				{
					timelineClient.Start();
				}
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
			if (this.rmClient != null)
			{
				RPC.StopProxy(this.rmClient);
			}
			if (historyServiceEnabled)
			{
				historyClient.Stop();
			}
			if (timelineServiceEnabled)
			{
				timelineClient.Stop();
			}
			base.ServiceStop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private GetNewApplicationResponse GetNewApplication()
		{
			GetNewApplicationRequest request = Records.NewRecord<GetNewApplicationRequest>();
			return rmClient.GetNewApplication(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override YarnClientApplication CreateApplication()
		{
			ApplicationSubmissionContext context = Records.NewRecord<ApplicationSubmissionContext
				>();
			GetNewApplicationResponse newApp = GetNewApplication();
			ApplicationId appId = newApp.GetApplicationId();
			context.SetApplicationId(appId);
			return new YarnClientApplication(newApp, context);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationId SubmitApplication(ApplicationSubmissionContext appContext
			)
		{
			ApplicationId applicationId = appContext.GetApplicationId();
			if (applicationId == null)
			{
				throw new ApplicationIdNotProvidedException("ApplicationId is not provided in ApplicationSubmissionContext"
					);
			}
			SubmitApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				SubmitApplicationRequest>();
			request.SetApplicationSubmissionContext(appContext);
			// Automatically add the timeline DT into the CLC
			// Only when the security and the timeline service are both enabled
			if (IsSecurityEnabled() && timelineServiceEnabled)
			{
				AddTimelineDelegationToken(appContext.GetAMContainerSpec());
			}
			//TODO: YARN-1763:Handle RM failovers during the submitApplication call.
			rmClient.SubmitApplication(request);
			int pollCount = 0;
			long startTime = Runtime.CurrentTimeMillis();
			EnumSet<YarnApplicationState> waitingStates = EnumSet.Of(YarnApplicationState.New
				, YarnApplicationState.NewSaving, YarnApplicationState.Submitted);
			EnumSet<YarnApplicationState> failToSubmitStates = EnumSet.Of(YarnApplicationState
				.Failed, YarnApplicationState.Killed);
			while (true)
			{
				try
				{
					ApplicationReport appReport = GetApplicationReport(applicationId);
					YarnApplicationState state = appReport.GetYarnApplicationState();
					if (!waitingStates.Contains(state))
					{
						if (failToSubmitStates.Contains(state))
						{
							throw new YarnException("Failed to submit " + applicationId + " to YARN : " + appReport
								.GetDiagnostics());
						}
						Log.Info("Submitted application " + applicationId);
						break;
					}
					long elapsedMillis = Runtime.CurrentTimeMillis() - startTime;
					if (EnforceAsyncAPITimeout() && elapsedMillis >= asyncApiPollTimeoutMillis)
					{
						throw new YarnException("Timed out while waiting for application " + applicationId
							 + " to be submitted successfully");
					}
					// Notify the client through the log every 10 poll, in case the client
					// is blocked here too long.
					if (++pollCount % 10 == 0)
					{
						Log.Info("Application submission is not finished, " + "submitted application " + 
							applicationId + " is still in " + state);
					}
					try
					{
						Sharpen.Thread.Sleep(submitPollIntervalMillis);
					}
					catch (Exception)
					{
						Log.Error("Interrupted while waiting for application " + applicationId + " to be successfully submitted."
							);
					}
				}
				catch (ApplicationNotFoundException)
				{
					// FailOver or RM restart happens before RMStateStore saves
					// ApplicationState
					Log.Info("Re-submit application " + applicationId + "with the " + "same ApplicationSubmissionContext"
						);
					rmClient.SubmitApplication(request);
				}
			}
			return applicationId;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void AddTimelineDelegationToken(ContainerLaunchContext clc)
		{
			Credentials credentials = new Credentials();
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			ByteBuffer tokens = clc.GetTokens();
			if (tokens != null)
			{
				dibb.Reset(tokens);
				credentials.ReadTokenStorageStream(dibb);
				tokens.Rewind();
			}
			// If the timeline delegation token is already in the CLC, no need to add
			// one more
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in credentials
				.GetAllTokens())
			{
				if (token.GetKind().Equals(TimelineDelegationTokenIdentifier.KindName))
				{
					return;
				}
			}
			Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> timelineDelegationToken
				 = GetTimelineDelegationToken();
			if (timelineDelegationToken == null)
			{
				return;
			}
			credentials.AddToken(timelineService, timelineDelegationToken);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Add timline delegation token into credentials: " + timelineDelegationToken
					);
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.WriteTokenStorageToStream(dob);
			tokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			clc.SetTokens(tokens);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[VisibleForTesting]
		internal virtual Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
			> GetTimelineDelegationToken()
		{
			try
			{
				return timelineClient.GetDelegationToken(timelineDTRenewer);
			}
			catch (Exception e)
			{
				if (timelineServiceBestEffort)
				{
					Log.Warn("Failed to get delegation token from the timeline server: " + e.Message);
					return null;
				}
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private static string GetTimelineDelegationTokenRenewer(Configuration conf)
		{
			// Parse the RM daemon user if it exists in the config
			string rmPrincipal = conf.Get(YarnConfiguration.RmPrincipal);
			string renewer = null;
			if (rmPrincipal != null && rmPrincipal.Length > 0)
			{
				string rmHost = conf.GetSocketAddr(YarnConfiguration.RmAddress, YarnConfiguration
					.DefaultRmAddress, YarnConfiguration.DefaultRmPort).GetHostName();
				renewer = SecurityUtil.GetServerPrincipal(rmPrincipal, rmHost);
			}
			return renewer;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal virtual bool IsSecurityEnabled()
		{
			return UserGroupInformation.IsSecurityEnabled();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void KillApplication(ApplicationId applicationId)
		{
			KillApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<KillApplicationRequest
				>();
			request.SetApplicationId(applicationId);
			try
			{
				int pollCount = 0;
				long startTime = Runtime.CurrentTimeMillis();
				while (true)
				{
					KillApplicationResponse response = rmClient.ForceKillApplication(request);
					if (response.GetIsKillCompleted())
					{
						Log.Info("Killed application " + applicationId);
						break;
					}
					long elapsedMillis = Runtime.CurrentTimeMillis() - startTime;
					if (EnforceAsyncAPITimeout() && elapsedMillis >= this.asyncApiPollTimeoutMillis)
					{
						throw new YarnException("Timed out while waiting for application " + applicationId
							 + " to be killed.");
					}
					if (++pollCount % 10 == 0)
					{
						Log.Info("Waiting for application " + applicationId + " to be killed.");
					}
					Sharpen.Thread.Sleep(asyncApiPollIntervalMillis);
				}
			}
			catch (Exception)
			{
				Log.Error("Interrupted while waiting for application " + applicationId + " to be killed."
					);
			}
		}

		[VisibleForTesting]
		internal virtual bool EnforceAsyncAPITimeout()
		{
			return asyncApiPollTimeoutMillis >= 0;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationReport GetApplicationReport(ApplicationId appId)
		{
			GetApplicationReportResponse response = null;
			try
			{
				GetApplicationReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetApplicationReportRequest>();
				request.SetApplicationId(appId);
				response = rmClient.GetApplicationReport(request);
			}
			catch (ApplicationNotFoundException e)
			{
				if (!historyServiceEnabled)
				{
					// Just throw it as usual if historyService is not enabled.
					throw;
				}
				return historyClient.GetApplicationReport(appId);
			}
			return response.GetApplicationReport();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken
			(ApplicationId appId)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Token token = GetApplicationReport(appId).GetAMRMToken
				();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = null;
			if (token != null)
			{
				amrmToken = ConverterUtils.ConvertFromYarn(token, (Text)null);
			}
			return amrmToken;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications()
		{
			return GetApplications(null, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			)
		{
			return GetApplications(applicationTypes, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(EnumSet<YarnApplicationState
			> applicationStates)
		{
			return GetApplications(null, applicationStates);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			, EnumSet<YarnApplicationState> applicationStates)
		{
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance(applicationTypes
				, applicationStates);
			GetApplicationsResponse response = rmClient.GetApplications(request);
			return response.GetApplicationList();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override YarnClusterMetrics GetYarnClusterMetrics()
		{
			GetClusterMetricsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				GetClusterMetricsRequest>();
			GetClusterMetricsResponse response = rmClient.GetClusterMetrics(request);
			return response.GetClusterMetrics();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<NodeReport> GetNodeReports(params NodeState[] states)
		{
			EnumSet<NodeState> statesSet = (states.Length == 0) ? EnumSet.AllOf<NodeState>() : 
				EnumSet.NoneOf<NodeState>();
			foreach (NodeState state in states)
			{
				statesSet.AddItem(state);
			}
			GetClusterNodesRequest request = GetClusterNodesRequest.NewInstance(statesSet);
			GetClusterNodesResponse response = rmClient.GetClusterNodes(request);
			return response.GetNodeReports();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Yarn.Api.Records.Token GetRMDelegationToken(Text
			 renewer)
		{
			/* get the token from RM */
			GetDelegationTokenRequest rmDTRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetDelegationTokenRequest>();
			rmDTRequest.SetRenewer(renewer.ToString());
			GetDelegationTokenResponse response = rmClient.GetDelegationToken(rmDTRequest);
			return response.GetRMDelegationToken();
		}

		private Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest GetQueueInfoRequest
			(string queueName, bool includeApplications, bool includeChildQueues, bool recursive
			)
		{
			Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest request = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest>();
			request.SetQueueName(queueName);
			request.SetIncludeApplications(includeApplications);
			request.SetIncludeChildQueues(includeChildQueues);
			request.SetRecursive(recursive);
			return request;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo GetQueueInfo(string queueName)
		{
			Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest request = GetQueueInfoRequest
				(queueName, true, false, false);
			Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueInfoRequest
				>();
			return rmClient.GetQueueInfo(request).GetQueueInfo();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueUserACLInfo> GetQueueAclsInfo()
		{
			GetQueueUserAclsInfoRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<GetQueueUserAclsInfoRequest>();
			return rmClient.GetQueueUserAcls(request).GetUserAclsInfoList();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetAllQueues()
		{
			IList<QueueInfo> queues = new AList<QueueInfo>();
			QueueInfo rootQueue = rmClient.GetQueueInfo(GetQueueInfoRequest(Root, false, true
				, true)).GetQueueInfo();
			GetChildQueues(rootQueue, queues, true);
			return queues;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetRootQueueInfos()
		{
			IList<QueueInfo> queues = new AList<QueueInfo>();
			QueueInfo rootQueue = rmClient.GetQueueInfo(GetQueueInfoRequest(Root, false, true
				, true)).GetQueueInfo();
			GetChildQueues(rootQueue, queues, false);
			return queues;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetChildQueueInfos(string parent)
		{
			IList<QueueInfo> queues = new AList<QueueInfo>();
			QueueInfo parentQueue = rmClient.GetQueueInfo(GetQueueInfoRequest(parent, false, 
				true, false)).GetQueueInfo();
			GetChildQueues(parentQueue, queues, true);
			return queues;
		}

		private void GetChildQueues(QueueInfo parent, IList<QueueInfo> queues, bool recursive
			)
		{
			IList<QueueInfo> childQueues = parent.GetChildQueues();
			foreach (QueueInfo child in childQueues)
			{
				queues.AddItem(child);
				if (recursive)
				{
					GetChildQueues(child, queues, recursive);
				}
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual void SetRMClient(ApplicationClientProtocol rmClient)
		{
			this.rmClient = rmClient;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
			 appAttemptId)
		{
			try
			{
				GetApplicationAttemptReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.
					NewRecord<GetApplicationAttemptReportRequest>();
				request.SetApplicationAttemptId(appAttemptId);
				GetApplicationAttemptReportResponse response = rmClient.GetApplicationAttemptReport
					(request);
				return response.GetApplicationAttemptReport();
			}
			catch (YarnException e)
			{
				if (!historyServiceEnabled)
				{
					// Just throw it as usual if historyService is not enabled.
					throw;
				}
				// Even if history-service is enabled, treat all exceptions still the same
				// except the following
				if (e.GetType() != typeof(ApplicationNotFoundException))
				{
					throw;
				}
				return historyClient.GetApplicationAttemptReport(appAttemptId);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
			 appId)
		{
			try
			{
				GetApplicationAttemptsRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetApplicationAttemptsRequest>();
				request.SetApplicationId(appId);
				GetApplicationAttemptsResponse response = rmClient.GetApplicationAttempts(request
					);
				return response.GetApplicationAttemptList();
			}
			catch (YarnException e)
			{
				if (!historyServiceEnabled)
				{
					// Just throw it as usual if historyService is not enabled.
					throw;
				}
				// Even if history-service is enabled, treat all exceptions still the same
				// except the following
				if (e.GetType() != typeof(ApplicationNotFoundException))
				{
					throw;
				}
				return historyClient.GetApplicationAttempts(appId);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ContainerReport GetContainerReport(ContainerId containerId)
		{
			try
			{
				GetContainerReportRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetContainerReportRequest>();
				request.SetContainerId(containerId);
				GetContainerReportResponse response = rmClient.GetContainerReport(request);
				return response.GetContainerReport();
			}
			catch (YarnException e)
			{
				if (!historyServiceEnabled)
				{
					// Just throw it as usual if historyService is not enabled.
					throw;
				}
				// Even if history-service is enabled, treat all exceptions still the same
				// except the following
				if (e.GetType() != typeof(ApplicationNotFoundException) && e.GetType() != typeof(
					ContainerNotFoundException))
				{
					throw;
				}
				return historyClient.GetContainerReport(containerId);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ContainerReport> GetContainers(ApplicationAttemptId applicationAttemptId
			)
		{
			IList<ContainerReport> containersForAttempt = new AList<ContainerReport>();
			bool appNotFoundInRM = false;
			try
			{
				GetContainersRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetContainersRequest
					>();
				request.SetApplicationAttemptId(applicationAttemptId);
				GetContainersResponse response = rmClient.GetContainers(request);
				Sharpen.Collections.AddAll(containersForAttempt, response.GetContainerList());
			}
			catch (YarnException e)
			{
				if (e.GetType() != typeof(ApplicationNotFoundException) || !historyServiceEnabled)
				{
					// If Application is not in RM and history service is enabled then we
					// need to check with history service else throw exception.
					throw;
				}
				appNotFoundInRM = true;
			}
			if (historyServiceEnabled)
			{
				// Check with AHS even if found in RM because to capture info of finished
				// containers also
				IList<ContainerReport> containersListFromAHS = null;
				try
				{
					containersListFromAHS = historyClient.GetContainers(applicationAttemptId);
				}
				catch (IOException e)
				{
					// History service access might be enabled but system metrics publisher
					// is disabled hence app not found exception is possible
					if (appNotFoundInRM)
					{
						// app not found in bothM and RM then propagate the exception.
						throw;
					}
				}
				if (null != containersListFromAHS && containersListFromAHS.Count > 0)
				{
					// remove duplicates
					ICollection<ContainerId> containerIdsToBeKeptFromAHS = new HashSet<ContainerId>();
					IEnumerator<ContainerReport> tmpItr = containersListFromAHS.GetEnumerator();
					while (tmpItr.HasNext())
					{
						containerIdsToBeKeptFromAHS.AddItem(tmpItr.Next().GetContainerId());
					}
					IEnumerator<ContainerReport> rmContainers = containersForAttempt.GetEnumerator();
					while (rmContainers.HasNext())
					{
						ContainerReport tmp = rmContainers.Next();
						containerIdsToBeKeptFromAHS.Remove(tmp.GetContainerId());
					}
					// Remove containers from AHS as container from RM will have latest
					// information
					if (containerIdsToBeKeptFromAHS.Count > 0 && containersListFromAHS.Count != containerIdsToBeKeptFromAHS
						.Count)
					{
						IEnumerator<ContainerReport> containersFromHS = containersListFromAHS.GetEnumerator
							();
						while (containersFromHS.HasNext())
						{
							ContainerReport containerReport = containersFromHS.Next();
							if (containerIdsToBeKeptFromAHS.Contains(containerReport.GetContainerId()))
							{
								containersForAttempt.AddItem(containerReport);
							}
						}
					}
					else
					{
						if (containersListFromAHS.Count == containerIdsToBeKeptFromAHS.Count)
						{
							Sharpen.Collections.AddAll(containersForAttempt, containersListFromAHS);
						}
					}
				}
			}
			return containersForAttempt;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void MoveApplicationAcrossQueues(ApplicationId appId, string queue
			)
		{
			MoveApplicationAcrossQueuesRequest request = MoveApplicationAcrossQueuesRequest.NewInstance
				(appId, queue);
			rmClient.MoveApplicationAcrossQueues(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
			 request)
		{
			return rmClient.SubmitReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
			 request)
		{
			return rmClient.UpdateReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
			 request)
		{
			return rmClient.DeleteReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<NodeId, ICollection<string>> GetNodeToLabels()
		{
			return rmClient.GetNodeToLabels(GetNodesToLabelsRequest.NewInstance()).GetNodeToLabels
				();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes()
		{
			return rmClient.GetLabelsToNodes(GetLabelsToNodesRequest.NewInstance()).GetLabelsToNodes
				();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes(ICollection
			<string> labels)
		{
			return rmClient.GetLabelsToNodes(GetLabelsToNodesRequest.NewInstance(labels)).GetLabelsToNodes
				();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ICollection<string> GetClusterNodeLabels()
		{
			return rmClient.GetClusterNodeLabels(GetClusterNodeLabelsRequest.NewInstance()).GetNodeLabels
				();
		}
	}
}
