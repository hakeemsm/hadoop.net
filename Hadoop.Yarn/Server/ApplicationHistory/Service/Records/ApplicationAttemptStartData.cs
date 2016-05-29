using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when
	/// <code>RMAppAttempt</code> starts, and that need to be stored persistently.
	/// </summary>
	public abstract class ApplicationAttemptStartData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationAttemptStartData NewInstance(ApplicationAttemptId appAttemptId
			, string host, int rpcPort, ContainerId masterContainerId)
		{
			ApplicationAttemptStartData appAttemptSD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationAttemptStartData>();
			appAttemptSD.SetApplicationAttemptId(appAttemptId);
			appAttemptSD.SetHost(host);
			appAttemptSD.SetRPCPort(rpcPort);
			appAttemptSD.SetMasterContainerId(masterContainerId);
			return appAttemptSD;
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
		public abstract string GetHost();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetHost(string host);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetRPCPort();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetRPCPort(int rpcPort);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetMasterContainerId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetMasterContainerId(ContainerId masterContainerId);
	}
}
