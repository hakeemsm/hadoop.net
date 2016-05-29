using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Container exit statuses indicating special exit circumstances.</summary>
	public class ContainerExitStatus
	{
		public const int Success = 0;

		public const int Invalid = -1000;

		/// <summary>
		/// Containers killed by the framework, either due to being released by
		/// the application or being 'lost' due to node failures etc.
		/// </summary>
		public const int Aborted = -100;

		/// <summary>
		/// When threshold number of the nodemanager-local-directories or
		/// threshold number of the nodemanager-log-directories become bad.
		/// </summary>
		public const int DisksFailed = -101;

		/// <summary>Containers preempted by the framework.</summary>
		public const int Preempted = -102;

		/// <summary>Container terminated because of exceeding allocated virtual memory.</summary>
		public const int KilledExceededVmem = -103;

		/// <summary>Container terminated because of exceeding allocated physical memory.</summary>
		public const int KilledExceededPmem = -104;

		/// <summary>Container was terminated by stop request by the app master.</summary>
		public const int KilledByAppmaster = -105;

		/// <summary>Container was terminated by the resource manager.</summary>
		public const int KilledByResourcemanager = -106;

		/// <summary>Container was terminated after the application finished.</summary>
		public const int KilledAfterAppCompletion = -107;
	}
}
