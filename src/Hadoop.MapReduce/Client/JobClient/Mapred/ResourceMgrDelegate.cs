using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ResourceMgrDelegate : YarnClient
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.ResourceMgrDelegate
			));

		private YarnConfiguration conf;

		private ApplicationSubmissionContext application;

		private ApplicationId applicationId;

		[InterfaceAudience.Private]
		[VisibleForTesting]
		protected internal YarnClient client;

		private Text rmDTService;

		/// <summary>
		/// Delegate responsible for communicating with the Resource Manager's
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol"/>
		/// .
		/// </summary>
		/// <param name="conf">the configuration object.</param>
		public ResourceMgrDelegate(YarnConfiguration conf)
			: base(typeof(Org.Apache.Hadoop.Mapred.ResourceMgrDelegate).FullName)
		{
			this.conf = conf;
			this.client = YarnClient.CreateYarnClient();
			Init(conf);
			Start();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			client.Init(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			client.Start();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			client.Stop();
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskTrackerInfo[] GetActiveTrackers()
		{
			try
			{
				return TypeConverter.FromYarnNodes(client.GetNodeReports(NodeState.Running));
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobStatus[] GetAllJobs()
		{
			try
			{
				ICollection<string> appTypes = new HashSet<string>(1);
				appTypes.AddItem(MRJobConfig.MrApplicationType);
				EnumSet<YarnApplicationState> appStates = EnumSet.NoneOf<YarnApplicationState>();
				return TypeConverter.FromYarnApps(client.GetApplications(appTypes, appStates), this
					.conf);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskTrackerInfo[] GetBlacklistedTrackers()
		{
			// TODO: Implement getBlacklistedTrackers
			Log.Warn("getBlacklistedTrackers - Not implemented yet");
			return new TaskTrackerInfo[0];
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual ClusterMetrics GetClusterMetrics()
		{
			try
			{
				YarnClusterMetrics metrics = client.GetYarnClusterMetrics();
				ClusterMetrics oldMetrics = new ClusterMetrics(1, 1, 1, 1, 1, 1, metrics.GetNumNodeManagers
					() * 10, metrics.GetNumNodeManagers() * 2, 1, metrics.GetNumNodeManagers(), 0, 0
					);
				return oldMetrics;
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		public virtual Text GetRMDelegationTokenService()
		{
			if (rmDTService == null)
			{
				rmDTService = ClientRMProxy.GetRMDelegationTokenService(conf);
			}
			return rmDTService;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token GetDelegationToken(Text renewer
			)
		{
			try
			{
				return ConverterUtils.ConvertFromYarn(client.GetRMDelegationToken(renewer), GetRMDelegationTokenService
					());
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetFilesystemName()
		{
			return FileSystem.Get(conf).GetUri().ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual JobID GetNewJobID()
		{
			try
			{
				this.application = client.CreateApplication().GetApplicationSubmissionContext();
				this.applicationId = this.application.GetApplicationId();
				return TypeConverter.FromYarn(applicationId);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo GetQueue(string queueName)
		{
			try
			{
				QueueInfo queueInfo = client.GetQueueInfo(queueName);
				return (queueInfo == null) ? null : TypeConverter.FromYarn(queueInfo, conf);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueAclsInfo[] GetQueueAclsForCurrentUser()
		{
			try
			{
				return TypeConverter.FromYarnQueueUserAclsInfo(client.GetQueueAclsInfo());
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetQueues()
		{
			try
			{
				return TypeConverter.FromYarnQueueInfo(client.GetAllQueues(), this.conf);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetRootQueues()
		{
			try
			{
				return TypeConverter.FromYarnQueueInfo(client.GetRootQueueInfos(), this.conf);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual QueueInfo[] GetChildQueues(string parent)
		{
			try
			{
				return TypeConverter.FromYarnQueueInfo(client.GetChildQueueInfos(parent), this.conf
					);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetStagingAreaDir()
		{
			//    Path path = new Path(MRJobConstants.JOB_SUBMIT_DIR);
			string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Path path = MRApps.GetStagingAreaDir(conf, user);
			Log.Debug("getStagingAreaDir: dir=" + path);
			return path.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string GetSystemDir()
		{
			Path sysDir = new Path(MRJobConfig.JobSubmitDir);
			//FileContext.getFileContext(conf).delete(sysDir, true);
			return sysDir.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual long GetTaskTrackerExpiryInterval()
		{
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void SetJobPriority(JobID arg0, string arg1)
		{
			return;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string arg0, long arg1)
		{
			return 0;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return applicationId;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override YarnClientApplication CreateApplication()
		{
			return client.CreateApplication();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationId SubmitApplication(ApplicationSubmissionContext appContext
			)
		{
			return client.SubmitApplication(appContext);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void KillApplication(ApplicationId applicationId)
		{
			client.KillApplication(applicationId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationReport GetApplicationReport(ApplicationId appId)
		{
			return client.GetApplicationReport(appId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken
			(ApplicationId appId)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications()
		{
			return client.GetApplications();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			)
		{
			return client.GetApplications(applicationTypes);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(EnumSet<YarnApplicationState
			> applicationStates)
		{
			return client.GetApplications(applicationStates);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationReport> GetApplications(ICollection<string> applicationTypes
			, EnumSet<YarnApplicationState> applicationStates)
		{
			return client.GetApplications(applicationTypes, applicationStates);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override YarnClusterMetrics GetYarnClusterMetrics()
		{
			return client.GetYarnClusterMetrics();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<NodeReport> GetNodeReports(params NodeState[] states)
		{
			return client.GetNodeReports(states);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override Org.Apache.Hadoop.Yarn.Api.Records.Token GetRMDelegationToken(Text
			 renewer)
		{
			return client.GetRMDelegationToken(renewer);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo GetQueueInfo(string queueName)
		{
			return client.GetQueueInfo(queueName);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetAllQueues()
		{
			return client.GetAllQueues();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetRootQueueInfos()
		{
			return client.GetRootQueueInfos();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueInfo> GetChildQueueInfos(string parent)
		{
			return client.GetChildQueueInfos(parent);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<QueueUserACLInfo> GetQueueAclsInfo()
		{
			return client.GetQueueAclsInfo();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ApplicationAttemptReport GetApplicationAttemptReport(ApplicationAttemptId
			 appAttemptId)
		{
			return client.GetApplicationAttemptReport(appAttemptId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ApplicationAttemptReport> GetApplicationAttempts(ApplicationId
			 appId)
		{
			return client.GetApplicationAttempts(appId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ContainerReport GetContainerReport(ContainerId containerId)
		{
			return client.GetContainerReport(containerId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IList<ContainerReport> GetContainers(ApplicationAttemptId applicationAttemptId
			)
		{
			return client.GetContainers(applicationAttemptId);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override void MoveApplicationAcrossQueues(ApplicationId appId, string queue
			)
		{
			client.MoveApplicationAcrossQueues(appId, queue);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationSubmissionResponse SubmitReservation(ReservationSubmissionRequest
			 request)
		{
			return client.SubmitReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationUpdateResponse UpdateReservation(ReservationUpdateRequest
			 request)
		{
			return client.UpdateReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ReservationDeleteResponse DeleteReservation(ReservationDeleteRequest
			 request)
		{
			return client.DeleteReservation(request);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<NodeId, ICollection<string>> GetNodeToLabels()
		{
			return client.GetNodeToLabels();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes()
		{
			return client.GetLabelsToNodes();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override IDictionary<string, ICollection<NodeId>> GetLabelsToNodes(ICollection
			<string> labels)
		{
			return client.GetLabelsToNodes(labels);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public override ICollection<string> GetClusterNodeLabels()
		{
			return client.GetClusterNodeLabels();
		}
	}
}
