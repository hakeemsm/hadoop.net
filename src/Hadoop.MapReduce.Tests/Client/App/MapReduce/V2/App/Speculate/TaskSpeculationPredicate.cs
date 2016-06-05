using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class TaskSpeculationPredicate
	{
		internal virtual bool CanSpeculate(AppContext context, TaskId taskID)
		{
			// This class rejects speculating any task that already has speculations,
			//  or isn't running.
			//  Subclasses should call TaskSpeculationPredicate.canSpeculate(...) , but
			//  can be even more restrictive.
			JobId jobID = taskID.GetJobId();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = context.GetJob(jobID);
			Task task = job.GetTask(taskID);
			return task.GetAttempts().Count == 1;
		}
	}
}
