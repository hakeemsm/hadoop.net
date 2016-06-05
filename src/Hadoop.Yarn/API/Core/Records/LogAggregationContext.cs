using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>LogAggregationContext</c>
	/// represents all of the
	/// information needed by the
	/// <c>NodeManager</c>
	/// to handle
	/// the logs for an application.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// includePattern. It uses Java Regex to filter the log files
	/// which match the defined include pattern and those log files
	/// will be uploaded when the application finishes.
	/// </li>
	/// <li>
	/// excludePattern. It uses Java Regex to filter the log files
	/// which match the defined exclude pattern and those log files
	/// will not be uploaded when application finishes. If the log file
	/// name matches both the include and the exclude pattern, this file
	/// will be excluded eventually.
	/// </li>
	/// <li>
	/// rolledLogsIncludePattern. It uses Java Regex to filter the log files
	/// which match the defined include pattern and those log files
	/// will be aggregated in a rolling fashion.
	/// </li>
	/// <li>
	/// rolledLogsExcludePattern. It uses Java Regex to filter the log files
	/// which match the defined exclude pattern and those log files
	/// will not be aggregated in a rolling fashion. If the log file
	/// name matches both the include and the exclude pattern, this file
	/// will be excluded eventually.
	/// </li>
	/// </ul>
	/// </summary>
	/// <seealso cref="ApplicationSubmissionContext"/>
	public abstract class LogAggregationContext
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static LogAggregationContext NewInstance(string includePattern, string excludePattern
			)
		{
			LogAggregationContext context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<LogAggregationContext
				>();
			context.SetIncludePattern(includePattern);
			context.SetExcludePattern(excludePattern);
			return context;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static LogAggregationContext NewInstance(string includePattern, string excludePattern
			, string rolledLogsIncludePattern, string rolledLogsExcludePattern)
		{
			LogAggregationContext context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<LogAggregationContext
				>();
			context.SetIncludePattern(includePattern);
			context.SetExcludePattern(excludePattern);
			context.SetRolledLogsIncludePattern(rolledLogsIncludePattern);
			context.SetRolledLogsExcludePattern(rolledLogsExcludePattern);
			return context;
		}

		/// <summary>Get include pattern.</summary>
		/// <remarks>
		/// Get include pattern. This includePattern only takes affect
		/// on logs that exist at the time of application finish.
		/// </remarks>
		/// <returns>include pattern</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetIncludePattern();

		/// <summary>Set include pattern.</summary>
		/// <remarks>
		/// Set include pattern. This includePattern only takes affect
		/// on logs that exist at the time of application finish.
		/// </remarks>
		/// <param name="includePattern"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetIncludePattern(string includePattern);

		/// <summary>Get exclude pattern.</summary>
		/// <remarks>
		/// Get exclude pattern. This excludePattern only takes affect
		/// on logs that exist at the time of application finish.
		/// </remarks>
		/// <returns>exclude pattern</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetExcludePattern();

		/// <summary>Set exclude pattern.</summary>
		/// <remarks>
		/// Set exclude pattern. This excludePattern only takes affect
		/// on logs that exist at the time of application finish.
		/// </remarks>
		/// <param name="excludePattern"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetExcludePattern(string excludePattern);

		/// <summary>Get include pattern in a rolling fashion.</summary>
		/// <returns>include pattern</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetRolledLogsIncludePattern();

		/// <summary>Set include pattern in a rolling fashion.</summary>
		/// <param name="rolledLogsIncludePattern"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetRolledLogsIncludePattern(string rolledLogsIncludePattern);

		/// <summary>Get exclude pattern for aggregation in a rolling fashion.</summary>
		/// <returns>exclude pattern</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetRolledLogsExcludePattern();

		/// <summary>Set exclude pattern for in a rolling fashion.</summary>
		/// <param name="rolledLogsExcludePattern"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetRolledLogsExcludePattern(string rolledLogsExcludePattern);
	}
}
