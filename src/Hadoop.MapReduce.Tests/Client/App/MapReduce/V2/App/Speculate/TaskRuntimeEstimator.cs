using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public interface TaskRuntimeEstimator
	{
		void EnrollAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedStatus, 
			long timestamp);

		long AttemptEnrolledTime(TaskAttemptId attemptID);

		void UpdateAttempt(TaskAttemptStatusUpdateEvent.TaskAttemptStatus reportedStatus, 
			long timestamp);

		void Contextualize(Configuration conf, AppContext context);

		/// <summary>Find a maximum reasonable execution wallclock time.</summary>
		/// <remarks>
		/// Find a maximum reasonable execution wallclock time.  Includes the time
		/// already elapsed.
		/// Find a maximum reasonable execution time.  Includes the time
		/// already elapsed.  If the projected total execution time for this task
		/// ever exceeds its reasonable execution time, we may speculate it.
		/// </remarks>
		/// <param name="id">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskId"/>
		/// of the task we are asking about
		/// </param>
		/// <returns>
		/// the task's maximum reasonable runtime, or MAX_VALUE if
		/// we don't have enough information to rule out any runtime,
		/// however long.
		/// </returns>
		long ThresholdRuntime(TaskId id);

		/// <summary>Estimate a task attempt's total runtime.</summary>
		/// <remarks>
		/// Estimate a task attempt's total runtime.  Includes the time already
		/// elapsed.
		/// </remarks>
		/// <param name="id">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskAttemptId"/>
		/// of the attempt we are asking about
		/// </param>
		/// <returns>
		/// our best estimate of the attempt's runtime, or
		/// <c>-1</c>
		/// if
		/// we don't have enough information yet to produce an estimate.
		/// </returns>
		long EstimatedRuntime(TaskAttemptId id);

		/// <summary>
		/// Estimates how long a new attempt on this task will take if we start
		/// one now
		/// </summary>
		/// <param name="id">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskId"/>
		/// of the task we are asking about
		/// </param>
		/// <returns>
		/// our best estimate of a new attempt's runtime, or
		/// <c>-1</c>
		/// if
		/// we don't have enough information yet to produce an estimate.
		/// </returns>
		long EstimatedNewAttemptRuntime(TaskId id);

		/// <summary>
		/// Computes the width of the error band of our estimate of the task
		/// runtime as returned by
		/// <see cref="EstimatedRuntime(Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskAttemptId)
		/// 	"/>
		/// </summary>
		/// <param name="id">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskAttemptId"/>
		/// of the attempt we are asking about
		/// </param>
		/// <returns>
		/// our best estimate of the attempt's runtime, or
		/// <c>-1</c>
		/// if
		/// we don't have enough information yet to produce an estimate.
		/// </returns>
		long RuntimeEstimateVariance(TaskAttemptId id);
	}
}
