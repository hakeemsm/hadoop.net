using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class NullTaskRuntimesEngine : TaskRuntimeEstimator
	{
		/*
		* This class is provided solely as an exemplae of the values that mean
		*  that nothing needs to be computed.  It's not currently used.
		*/
		public virtual void EnrollAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus 
			status, long timestamp)
		{
		}

		// no code
		public virtual long AttemptEnrolledTime(TaskAttemptId attemptID)
		{
			return long.MaxValue;
		}

		public virtual void UpdateAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus 
			status, long timestamp)
		{
		}

		// no code
		public virtual void Contextualize(Configuration conf, AppContext context)
		{
		}

		// no code
		public virtual long ThresholdRuntime(TaskId id)
		{
			return long.MaxValue;
		}

		public virtual long EstimatedRuntime(TaskAttemptId id)
		{
			return -1L;
		}

		public virtual long EstimatedNewAttemptRuntime(TaskId id)
		{
			return -1L;
		}

		public virtual long RuntimeEstimateVariance(TaskAttemptId id)
		{
			return -1L;
		}
	}
}
