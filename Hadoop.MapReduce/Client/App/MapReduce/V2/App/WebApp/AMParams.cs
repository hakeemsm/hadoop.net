using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Params constants for the AM webapp and the history webapp.</summary>
	public abstract class AMParams
	{
		public const string RmWeb = "rm.web";

		public const string AppId = "app.id";

		public const string JobId = "job.id";

		public const string TaskId = "task.id";

		public const string TaskType = "task.type";

		public const string TaskState = "task.state";

		public const string AttemptState = "attempt.state";

		public const string CounterGroup = "counter.group";

		public const string CounterName = "counter.name";
	}

	public static class AMParamsConstants
	{
	}
}
