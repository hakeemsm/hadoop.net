using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Class AppForTest publishes a methods for test</summary>
	public class AppForTest : Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App
	{
		public AppForTest(AppContext ctx)
			: base(ctx)
		{
		}

		internal override void SetJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			base.SetJob(job);
		}

		internal override void SetTask(Task task)
		{
			base.SetTask(task);
		}
	}
}
