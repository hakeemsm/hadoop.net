using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp.Dao
{
	public class AppInfo
	{
		protected internal string appId;

		protected internal string currentAppAttemptId;

		protected internal string user;

		protected internal string name;

		protected internal string queue;

		protected internal string type;

		protected internal string host;

		protected internal int rpcPort;

		protected internal YarnApplicationState appState;

		protected internal float progress;

		protected internal string diagnosticsInfo;

		protected internal string originalTrackingUrl;

		protected internal string trackingUrl;

		protected internal FinalApplicationStatus finalAppStatus;

		protected internal long submittedTime;

		protected internal long startedTime;

		protected internal long finishedTime;

		protected internal long elapsedTime;

		protected internal string applicationTags;

		public AppInfo()
		{
		}

		public AppInfo(ApplicationReport app)
		{
			// JAXB needs this
			appId = app.GetApplicationId().ToString();
			if (app.GetCurrentApplicationAttemptId() != null)
			{
				currentAppAttemptId = app.GetCurrentApplicationAttemptId().ToString();
			}
			user = app.GetUser();
			queue = app.GetQueue();
			name = app.GetName();
			type = app.GetApplicationType();
			host = app.GetHost();
			rpcPort = app.GetRpcPort();
			appState = app.GetYarnApplicationState();
			diagnosticsInfo = app.GetDiagnostics();
			trackingUrl = app.GetTrackingUrl();
			originalTrackingUrl = app.GetOriginalTrackingUrl();
			submittedTime = app.GetStartTime();
			startedTime = app.GetStartTime();
			finishedTime = app.GetFinishTime();
			elapsedTime = Times.Elapsed(startedTime, finishedTime);
			finalAppStatus = app.GetFinalApplicationStatus();
			progress = app.GetProgress() * 100;
			// in percent
			if (app.GetApplicationTags() != null && !app.GetApplicationTags().IsEmpty())
			{
				this.applicationTags = StringHelper.CsvJoiner.Join(app.GetApplicationTags());
			}
		}

		public virtual string GetAppId()
		{
			return appId;
		}

		public virtual string GetCurrentAppAttemptId()
		{
			return currentAppAttemptId;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual string GetType()
		{
			return type;
		}

		public virtual string GetHost()
		{
			return host;
		}

		public virtual int GetRpcPort()
		{
			return rpcPort;
		}

		public virtual YarnApplicationState GetAppState()
		{
			return appState;
		}

		public virtual float GetProgress()
		{
			return progress;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual string GetOriginalTrackingUrl()
		{
			return originalTrackingUrl;
		}

		public virtual string GetTrackingUrl()
		{
			return trackingUrl;
		}

		public virtual FinalApplicationStatus GetFinalAppStatus()
		{
			return finalAppStatus;
		}

		public virtual long GetSubmittedTime()
		{
			return submittedTime;
		}

		public virtual long GetStartedTime()
		{
			return startedTime;
		}

		public virtual long GetFinishedTime()
		{
			return finishedTime;
		}

		public virtual long GetElapsedTime()
		{
			return elapsedTime;
		}

		public virtual string GetApplicationTags()
		{
			return applicationTags;
		}
	}
}
