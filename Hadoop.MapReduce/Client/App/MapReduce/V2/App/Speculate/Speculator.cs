using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	/// <summary>Speculator component.</summary>
	/// <remarks>
	/// Speculator component. Task Attempts' status updates are sent to this
	/// component. Concrete implementation runs the speculative algorithm and
	/// sends the TaskEventType.T_ADD_ATTEMPT.
	/// An implementation also has to arrange for the jobs to be scanned from
	/// time to time, to launch the speculations.
	/// </remarks>
	public abstract class Speculator : EventHandler<SpeculatorEvent>
	{
		public enum EventType
		{
			AttemptStatusUpdate,
			AttemptStart,
			TaskContainerNeedUpdate,
			JobCreate
		}

		// This will be implemented if we go to a model where the events are
		//  processed within the TaskAttempts' state transitions' code.
		public abstract void HandleAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus
			 status);
	}

	public static class SpeculatorConstants
	{
	}
}
