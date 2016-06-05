using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class App
	{
		internal readonly AppContext context;

		private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;

		private Task task;

		[Com.Google.Inject.Inject]
		internal App(AppContext ctx)
		{
			context = ctx;
		}

		internal virtual void SetJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			this.job = job;
		}

		public virtual Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob()
		{
			return job;
		}

		internal virtual void SetTask(Task task)
		{
			this.task = task;
		}

		public virtual Task GetTask()
		{
			return task;
		}
	}
}
