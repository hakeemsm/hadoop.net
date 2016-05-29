using System.Collections.Generic;
using Com.Google.Common.Base;
using Javax.Xml.Bind.Annotation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class AppInfo
	{
		[XmlTransient]
		protected internal string appIdNum;

		[XmlTransient]
		protected internal bool trackingUrlIsNotReady;

		[XmlTransient]
		protected internal string trackingUrlPretty;

		[XmlTransient]
		protected internal bool amContainerLogsExist = false;

		[XmlTransient]
		protected internal ApplicationId applicationId;

		[XmlTransient]
		private string schemePrefix;

		protected internal string id;

		protected internal string user;

		protected internal string name;

		protected internal string queue;

		protected internal YarnApplicationState state;

		protected internal FinalApplicationStatus finalStatus;

		protected internal float progress;

		protected internal string trackingUI;

		protected internal string trackingUrl;

		protected internal string diagnostics;

		protected internal long clusterId;

		protected internal string applicationType;

		protected internal string applicationTags = string.Empty;

		protected internal long startedTime;

		protected internal long finishedTime;

		protected internal long elapsedTime;

		protected internal string amContainerLogs;

		protected internal string amHostHttpAddress;

		protected internal int allocatedMB;

		protected internal int allocatedVCores;

		protected internal int runningContainers;

		protected internal long memorySeconds;

		protected internal long vcoreSeconds;

		protected internal int preemptedResourceMB;

		protected internal int preemptedResourceVCores;

		protected internal int numNonAMContainerPreempted;

		protected internal int numAMContainerPreempted;

		protected internal IList<ResourceRequest> resourceRequests;

		public AppInfo()
		{
		}

		public AppInfo(ResourceManager rm, RMApp app, bool hasAccess, string schemePrefix
			)
		{
			// these are ok for any user to see
			// these are only allowed if acls allow
			// preemption info fields
			// JAXB needs this
			this.schemePrefix = schemePrefix;
			if (app != null)
			{
				string trackingUrl = app.GetTrackingUrl();
				this.state = app.CreateApplicationState();
				this.trackingUrlIsNotReady = trackingUrl == null || trackingUrl.IsEmpty() || YarnApplicationState
					.New == this.state || YarnApplicationState.NewSaving == this.state || YarnApplicationState
					.Submitted == this.state || YarnApplicationState.Accepted == this.state;
				this.trackingUI = this.trackingUrlIsNotReady ? "UNASSIGNED" : (app.GetFinishTime(
					) == 0 ? "ApplicationMaster" : "History");
				if (!trackingUrlIsNotReady)
				{
					this.trackingUrl = WebAppUtils.GetURLWithScheme(schemePrefix, trackingUrl);
					this.trackingUrlPretty = this.trackingUrl;
				}
				else
				{
					this.trackingUrlPretty = "UNASSIGNED";
				}
				this.applicationId = app.GetApplicationId();
				this.applicationType = app.GetApplicationType();
				this.appIdNum = app.GetApplicationId().GetId().ToString();
				this.id = app.GetApplicationId().ToString();
				this.user = app.GetUser().ToString();
				this.name = app.GetName().ToString();
				this.queue = app.GetQueue().ToString();
				this.progress = app.GetProgress() * 100;
				this.diagnostics = app.GetDiagnostics().ToString();
				if (diagnostics == null || diagnostics.IsEmpty())
				{
					this.diagnostics = string.Empty;
				}
				if (app.GetApplicationTags() != null && !app.GetApplicationTags().IsEmpty())
				{
					this.applicationTags = Joiner.On(',').Join(app.GetApplicationTags());
				}
				this.finalStatus = app.GetFinalApplicationStatus();
				this.clusterId = ResourceManager.GetClusterTimeStamp();
				if (hasAccess)
				{
					this.startedTime = app.GetStartTime();
					this.finishedTime = app.GetFinishTime();
					this.elapsedTime = Times.Elapsed(app.GetStartTime(), app.GetFinishTime());
					RMAppAttempt attempt = app.GetCurrentAppAttempt();
					if (attempt != null)
					{
						Container masterContainer = attempt.GetMasterContainer();
						if (masterContainer != null)
						{
							this.amContainerLogsExist = true;
							this.amContainerLogs = WebAppUtils.GetRunningLogURL(schemePrefix + masterContainer
								.GetNodeHttpAddress(), ConverterUtils.ToString(masterContainer.GetId()), app.GetUser
								());
							this.amHostHttpAddress = masterContainer.GetNodeHttpAddress();
						}
						ApplicationResourceUsageReport resourceReport = attempt.GetApplicationResourceUsageReport
							();
						if (resourceReport != null)
						{
							Resource usedResources = resourceReport.GetUsedResources();
							allocatedMB = usedResources.GetMemory();
							allocatedVCores = usedResources.GetVirtualCores();
							runningContainers = resourceReport.GetNumUsedContainers();
						}
						resourceRequests = ((AbstractYarnScheduler)rm.GetRMContext().GetScheduler()).GetPendingResourceRequestsForAttempt
							(attempt.GetAppAttemptId());
					}
				}
				// copy preemption info fields
				RMAppMetrics appMetrics = app.GetRMAppMetrics();
				numAMContainerPreempted = appMetrics.GetNumAMContainersPreempted();
				preemptedResourceMB = appMetrics.GetResourcePreempted().GetMemory();
				numNonAMContainerPreempted = appMetrics.GetNumNonAMContainersPreempted();
				preemptedResourceVCores = appMetrics.GetResourcePreempted().GetVirtualCores();
				memorySeconds = appMetrics.GetMemorySeconds();
				vcoreSeconds = appMetrics.GetVcoreSeconds();
			}
		}

		public virtual bool IsTrackingUrlReady()
		{
			return !this.trackingUrlIsNotReady;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}

		public virtual string GetAppId()
		{
			return this.id;
		}

		public virtual string GetAppIdNum()
		{
			return this.appIdNum;
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual string GetQueue()
		{
			return this.queue;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual YarnApplicationState GetState()
		{
			return this.state;
		}

		public virtual float GetProgress()
		{
			return this.progress;
		}

		public virtual string GetTrackingUI()
		{
			return this.trackingUI;
		}

		public virtual string GetNote()
		{
			return this.diagnostics;
		}

		public virtual FinalApplicationStatus GetFinalStatus()
		{
			return this.finalStatus;
		}

		public virtual string GetTrackingUrl()
		{
			return this.trackingUrl;
		}

		public virtual string GetTrackingUrlPretty()
		{
			return this.trackingUrlPretty;
		}

		public virtual long GetStartTime()
		{
			return this.startedTime;
		}

		public virtual long GetFinishTime()
		{
			return this.finishedTime;
		}

		public virtual long GetElapsedTime()
		{
			return this.elapsedTime;
		}

		public virtual string GetAMContainerLogs()
		{
			return this.amContainerLogs;
		}

		public virtual string GetAMHostHttpAddress()
		{
			return this.amHostHttpAddress;
		}

		public virtual bool AmContainerLogsExist()
		{
			return this.amContainerLogsExist;
		}

		public virtual long GetClusterId()
		{
			return this.clusterId;
		}

		public virtual string GetApplicationType()
		{
			return this.applicationType;
		}

		public virtual string GetApplicationTags()
		{
			return this.applicationTags;
		}

		public virtual int GetRunningContainers()
		{
			return this.runningContainers;
		}

		public virtual int GetAllocatedMB()
		{
			return this.allocatedMB;
		}

		public virtual int GetAllocatedVCores()
		{
			return this.allocatedVCores;
		}

		public virtual int GetPreemptedMB()
		{
			return preemptedResourceMB;
		}

		public virtual int GetPreemptedVCores()
		{
			return preemptedResourceVCores;
		}

		public virtual int GetNumNonAMContainersPreempted()
		{
			return numNonAMContainerPreempted;
		}

		public virtual int GetNumAMContainersPreempted()
		{
			return numAMContainerPreempted;
		}

		public virtual long GetMemorySeconds()
		{
			return memorySeconds;
		}

		public virtual long GetVcoreSeconds()
		{
			return vcoreSeconds;
		}

		public virtual IList<ResourceRequest> GetResourceRequests()
		{
			return this.resourceRequests;
		}
	}
}
