using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptContainerLaunchedEvent : TaskAttemptEvent
	{
		private int shufflePort;

		/// <summary>Create a new TaskAttemptEvent.</summary>
		/// <param name="id">the id of the task attempt</param>
		/// <param name="shufflePort">the port that shuffle is listening on.</param>
		public TaskAttemptContainerLaunchedEvent(TaskAttemptId id, int shufflePort)
			: base(id, TaskAttemptEventType.TaContainerLaunched)
		{
			this.shufflePort = shufflePort;
		}

		/// <summary>Get the port that the shuffle handler is listening on.</summary>
		/// <remarks>
		/// Get the port that the shuffle handler is listening on. This is only
		/// valid if the type of the event is TA_CONTAINER_LAUNCHED
		/// </remarks>
		/// <returns>the port the shuffle handler is listening on.</returns>
		public virtual int GetShufflePort()
		{
			return shufflePort;
		}
	}
}
