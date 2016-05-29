using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains the fields that can be determined when <code>RMApp</code>
	/// starts, and that need to be stored persistently.
	/// </summary>
	public abstract class ApplicationStartData
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ApplicationStartData NewInstance(ApplicationId applicationId, string
			 applicationName, string applicationType, string queue, string user, long submitTime
			, long startTime)
		{
			ApplicationStartData appSD = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ApplicationStartData
				>();
			appSD.SetApplicationId(applicationId);
			appSD.SetApplicationName(applicationName);
			appSD.SetApplicationType(applicationType);
			appSD.SetQueue(queue);
			appSD.SetUser(user);
			appSD.SetSubmitTime(submitTime);
			appSD.SetStartTime(startTime);
			return appSD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ApplicationId GetApplicationId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationId(ApplicationId applicationId);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetApplicationName();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationName(string applicationName);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetApplicationType();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetApplicationType(string applicationType);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetUser();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetUser(string user);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetQueue();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetQueue(string queue);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetSubmitTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetSubmitTime(long submitTime);

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetStartTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetStartTime(long startTime);
	}
}
