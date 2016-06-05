using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains all the fields that are stored persistently for
	/// <code>RMApp</code>.
	/// </summary>
	public class ApplicationHistoryData
	{
		private ApplicationId applicationId;

		private string applicationName;

		private string applicationType;

		private string user;

		private string queue;

		private long submitTime;

		private long startTime;

		private long finishTime;

		private string diagnosticsInfo;

		private FinalApplicationStatus finalApplicationStatus;

		private YarnApplicationState yarnApplicationState;

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationHistoryData NewInstance(ApplicationId applicationId, string
			 applicationName, string applicationType, string queue, string user, long submitTime
			, long startTime, long finishTime, string diagnosticsInfo, FinalApplicationStatus
			 finalApplicationStatus, YarnApplicationState yarnApplicationState)
		{
			ApplicationHistoryData appHD = new ApplicationHistoryData();
			appHD.SetApplicationId(applicationId);
			appHD.SetApplicationName(applicationName);
			appHD.SetApplicationType(applicationType);
			appHD.SetQueue(queue);
			appHD.SetUser(user);
			appHD.SetSubmitTime(submitTime);
			appHD.SetStartTime(startTime);
			appHD.SetFinishTime(finishTime);
			appHD.SetDiagnosticsInfo(diagnosticsInfo);
			appHD.SetFinalApplicationStatus(finalApplicationStatus);
			appHD.SetYarnApplicationState(yarnApplicationState);
			return appHD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual ApplicationId GetApplicationId()
		{
			return applicationId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetApplicationId(ApplicationId applicationId)
		{
			this.applicationId = applicationId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetApplicationName()
		{
			return applicationName;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetApplicationName(string applicationName)
		{
			this.applicationName = applicationName;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetApplicationType()
		{
			return applicationType;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetApplicationType(string applicationType)
		{
			this.applicationType = applicationType;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetUser()
		{
			return user;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetUser(string user)
		{
			this.user = user;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetQueue()
		{
			return queue;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual long GetSubmitTime()
		{
			return submitTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetSubmitTime(long submitTime)
		{
			this.submitTime = submitTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual long GetStartTime()
		{
			return startTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetStartTime(long startTime)
		{
			this.startTime = startTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetFinishTime(long finishTime)
		{
			this.finishTime = finishTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetDiagnosticsInfo(string diagnosticsInfo)
		{
			this.diagnosticsInfo = diagnosticsInfo;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual FinalApplicationStatus GetFinalApplicationStatus()
		{
			return finalApplicationStatus;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus
			)
		{
			this.finalApplicationStatus = finalApplicationStatus;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual YarnApplicationState GetYarnApplicationState()
		{
			return this.yarnApplicationState;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetYarnApplicationState(YarnApplicationState yarnApplicationState
			)
		{
			this.yarnApplicationState = yarnApplicationState;
		}
	}
}
