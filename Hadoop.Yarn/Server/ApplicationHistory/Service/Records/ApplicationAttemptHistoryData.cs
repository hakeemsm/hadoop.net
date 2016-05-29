using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains all the fields that are stored persistently for
	/// <code>RMAppAttempt</code>.
	/// </summary>
	public class ApplicationAttemptHistoryData
	{
		private ApplicationAttemptId applicationAttemptId;

		private string host;

		private int rpcPort;

		private string trackingURL;

		private string diagnosticsInfo;

		private FinalApplicationStatus finalApplicationStatus;

		private ContainerId masterContainerId;

		private YarnApplicationAttemptState yarnApplicationAttemptState;

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationAttemptHistoryData NewInstance(ApplicationAttemptId appAttemptId
			, string host, int rpcPort, ContainerId masterContainerId, string diagnosticsInfo
			, string trackingURL, FinalApplicationStatus finalApplicationStatus, YarnApplicationAttemptState
			 yarnApplicationAttemptState)
		{
			ApplicationAttemptHistoryData appAttemptHD = new ApplicationAttemptHistoryData();
			appAttemptHD.SetApplicationAttemptId(appAttemptId);
			appAttemptHD.SetHost(host);
			appAttemptHD.SetRPCPort(rpcPort);
			appAttemptHD.SetMasterContainerId(masterContainerId);
			appAttemptHD.SetDiagnosticsInfo(diagnosticsInfo);
			appAttemptHD.SetTrackingURL(trackingURL);
			appAttemptHD.SetFinalApplicationStatus(finalApplicationStatus);
			appAttemptHD.SetYarnApplicationAttemptState(yarnApplicationAttemptState);
			return appAttemptHD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return applicationAttemptId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			)
		{
			this.applicationAttemptId = applicationAttemptId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetHost()
		{
			return host;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetHost(string host)
		{
			this.host = host;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual int GetRPCPort()
		{
			return rpcPort;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetRPCPort(int rpcPort)
		{
			this.rpcPort = rpcPort;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetTrackingURL()
		{
			return trackingURL;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetTrackingURL(string trackingURL)
		{
			this.trackingURL = trackingURL;
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
		public virtual ContainerId GetMasterContainerId()
		{
			return masterContainerId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetMasterContainerId(ContainerId masterContainerId)
		{
			this.masterContainerId = masterContainerId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual YarnApplicationAttemptState GetYarnApplicationAttemptState()
		{
			return yarnApplicationAttemptState;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetYarnApplicationAttemptState(YarnApplicationAttemptState yarnApplicationAttemptState
			)
		{
			this.yarnApplicationAttemptState = yarnApplicationAttemptState;
		}
	}
}
