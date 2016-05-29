using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when
	/// <code>RMAppAttempt</code> finishes, and that need to be stored persistently.
	/// </summary>
	public abstract class ApplicationAttemptFinishData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationAttemptFinishData NewInstance(ApplicationAttemptId appAttemptId
			, string diagnosticsInfo, string trackingURL, FinalApplicationStatus finalApplicationStatus
			, YarnApplicationAttemptState yarnApplicationAttemptState)
		{
			ApplicationAttemptFinishData appAttemptFD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationAttemptFinishData>();
			appAttemptFD.SetApplicationAttemptId(appAttemptId);
			appAttemptFD.SetDiagnosticsInfo(diagnosticsInfo);
			appAttemptFD.SetTrackingURL(trackingURL);
			appAttemptFD.SetFinalApplicationStatus(finalApplicationStatus);
			appAttemptFD.SetYarnApplicationAttemptState(yarnApplicationAttemptState);
			return appAttemptFD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationAttemptId GetApplicationAttemptId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationAttemptId(ApplicationAttemptId applicationAttemptId
			);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetTrackingURL();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetTrackingURL(string trackingURL);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetDiagnosticsInfo();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnosticsInfo(string diagnosticsInfo);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract FinalApplicationStatus GetFinalApplicationStatus();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetFinalApplicationStatus(FinalApplicationStatus finalApplicationStatus
			);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract YarnApplicationAttemptState GetYarnApplicationAttemptState();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetYarnApplicationAttemptState(YarnApplicationAttemptState yarnApplicationAttemptState
			);
	}
}
