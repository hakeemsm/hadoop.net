using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when <code>RMApp</code>
	/// finishes, and that need to be stored persistently.
	/// </summary>
	public abstract class ApplicationFinishData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationFinishData NewInstance(ApplicationId applicationId, long
			 finishTime, string diagnosticsInfo, FinalApplicationStatus finalApplicationStatus
			, YarnApplicationState yarnApplicationState)
		{
			ApplicationFinishData appFD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationFinishData
				>();
			appFD.SetApplicationId(applicationId);
			appFD.SetFinishTime(finishTime);
			appFD.SetDiagnosticsInfo(diagnosticsInfo);
			appFD.SetFinalApplicationStatus(finalApplicationStatus);
			appFD.SetYarnApplicationState(yarnApplicationState);
			return appFD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationId GetApplicationId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationId(ApplicationId applicationId);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetFinishTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetFinishTime(long finishTime);

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
		public abstract YarnApplicationState GetYarnApplicationState();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetYarnApplicationState(YarnApplicationState yarnApplicationState
			);
	}
}
