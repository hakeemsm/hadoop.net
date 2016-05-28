using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol
{
	public class TestControlledJob
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddingDependingJobToRunningJobFails()
		{
			Configuration conf = new Configuration();
			ControlledJob job1 = new ControlledJob(conf);
			job1.SetJobState(ControlledJob.State.Running);
			NUnit.Framework.Assert.IsFalse(job1.AddDependingJob(new ControlledJob(conf)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddingDependingJobToCompletedJobFails()
		{
			Configuration conf = new Configuration();
			ControlledJob job1 = new ControlledJob(conf);
			job1.SetJobState(ControlledJob.State.Success);
			NUnit.Framework.Assert.IsFalse(job1.AddDependingJob(new ControlledJob(conf)));
		}
	}
}
