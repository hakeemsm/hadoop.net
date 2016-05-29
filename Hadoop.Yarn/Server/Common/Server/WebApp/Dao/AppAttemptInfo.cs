using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp.Dao
{
	public class AppAttemptInfo
	{
		protected internal string appAttemptId;

		protected internal string host;

		protected internal int rpcPort;

		protected internal string trackingUrl;

		protected internal string originalTrackingUrl;

		protected internal string diagnosticsInfo;

		protected internal YarnApplicationAttemptState appAttemptState;

		protected internal string amContainerId;

		public AppAttemptInfo()
		{
		}

		public AppAttemptInfo(ApplicationAttemptReport appAttempt)
		{
			// JAXB needs this
			appAttemptId = appAttempt.GetApplicationAttemptId().ToString();
			host = appAttempt.GetHost();
			rpcPort = appAttempt.GetRpcPort();
			trackingUrl = appAttempt.GetTrackingUrl();
			originalTrackingUrl = appAttempt.GetOriginalTrackingUrl();
			diagnosticsInfo = appAttempt.GetDiagnostics();
			appAttemptState = appAttempt.GetYarnApplicationAttemptState();
			if (appAttempt.GetAMContainerId() != null)
			{
				amContainerId = appAttempt.GetAMContainerId().ToString();
			}
		}

		public virtual string GetAppAttemptId()
		{
			return appAttemptId;
		}

		public virtual string GetHost()
		{
			return host;
		}

		public virtual int GetRpcPort()
		{
			return rpcPort;
		}

		public virtual string GetTrackingUrl()
		{
			return trackingUrl;
		}

		public virtual string GetOriginalTrackingUrl()
		{
			return originalTrackingUrl;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual YarnApplicationAttemptState GetAppAttemptState()
		{
			return appAttemptState;
		}

		public virtual string GetAmContainerId()
		{
			return amContainerId;
		}
	}
}
