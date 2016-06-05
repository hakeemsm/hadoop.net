using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Contains various scheduling metrics to be reported by UI and CLI.</summary>
	public abstract class ApplicationResourceUsageReport
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ApplicationResourceUsageReport NewInstance(int numUsedContainers, int
			 numReservedContainers, Resource usedResources, Resource reservedResources, Resource
			 neededResources, long memorySeconds, long vcoreSeconds)
		{
			ApplicationResourceUsageReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ApplicationResourceUsageReport>();
			report.SetNumUsedContainers(numUsedContainers);
			report.SetNumReservedContainers(numReservedContainers);
			report.SetUsedResources(usedResources);
			report.SetReservedResources(reservedResources);
			report.SetNeededResources(neededResources);
			report.SetMemorySeconds(memorySeconds);
			report.SetVcoreSeconds(vcoreSeconds);
			return report;
		}

		/// <summary>Get the number of used containers.</summary>
		/// <remarks>Get the number of used containers.  -1 for invalid/inaccessible reports.
		/// 	</remarks>
		/// <returns>the number of used containers</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetNumUsedContainers();

		/// <summary>Set the number of used containers</summary>
		/// <param name="num_containers">the number of used containers</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNumUsedContainers(int num_containers);

		/// <summary>Get the number of reserved containers.</summary>
		/// <remarks>Get the number of reserved containers.  -1 for invalid/inaccessible reports.
		/// 	</remarks>
		/// <returns>the number of reserved containers</returns>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract int GetNumReservedContainers();

		/// <summary>Set the number of reserved containers</summary>
		/// <param name="num_reserved_containers">the number of reserved containers</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNumReservedContainers(int num_reserved_containers);

		/// <summary>Get the used <code>Resource</code>.</summary>
		/// <remarks>Get the used <code>Resource</code>.  -1 for invalid/inaccessible reports.
		/// 	</remarks>
		/// <returns>the used <code>Resource</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetUsedResources();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUsedResources(Resource resources);

		/// <summary>Get the reserved <code>Resource</code>.</summary>
		/// <remarks>Get the reserved <code>Resource</code>.  -1 for invalid/inaccessible reports.
		/// 	</remarks>
		/// <returns>the reserved <code>Resource</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetReservedResources();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetReservedResources(Resource reserved_resources);

		/// <summary>Get the needed <code>Resource</code>.</summary>
		/// <remarks>Get the needed <code>Resource</code>.  -1 for invalid/inaccessible reports.
		/// 	</remarks>
		/// <returns>the needed <code>Resource</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetNeededResources();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNeededResources(Resource needed_resources);

		/// <summary>
		/// Set the aggregated amount of memory (in megabytes) the application has
		/// allocated times the number of seconds the application has been running.
		/// </summary>
		/// <param name="memory_seconds">the aggregated amount of memory seconds</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetMemorySeconds(long memory_seconds);

		/// <summary>
		/// Get the aggregated amount of memory (in megabytes) the application has
		/// allocated times the number of seconds the application has been running.
		/// </summary>
		/// <returns>the aggregated amount of memory seconds</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetMemorySeconds();

		/// <summary>
		/// Set the aggregated number of vcores that the application has allocated
		/// times the number of seconds the application has been running.
		/// </summary>
		/// <param name="vcore_seconds">the aggregated number of vcore seconds</param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetVcoreSeconds(long vcore_seconds);

		/// <summary>
		/// Get the aggregated number of vcores that the application has allocated
		/// times the number of seconds the application has been running.
		/// </summary>
		/// <returns>the aggregated number of vcore seconds</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetVcoreSeconds();
	}
}
