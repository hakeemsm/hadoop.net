using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestTaskHeartbeatHandler
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimeout()
		{
			EventHandler mockHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			Clock clock = new SystemClock();
			TaskHeartbeatHandler hb = new TaskHeartbeatHandler(mockHandler, clock, 1);
			Configuration conf = new Configuration();
			conf.SetInt(MRJobConfig.TaskTimeout, 10);
			//10 ms
			conf.SetInt(MRJobConfig.TaskTimeoutCheckIntervalMs, 10);
			//10 ms
			hb.Init(conf);
			hb.Start();
			try
			{
				ApplicationId appId = ApplicationId.NewInstance(0l, 5);
				JobId jobId = MRBuilderUtils.NewJobId(appId, 4);
				TaskId tid = MRBuilderUtils.NewTaskId(jobId, 3, TaskType.Map);
				TaskAttemptId taid = MRBuilderUtils.NewTaskAttemptId(tid, 2);
				hb.Register(taid);
				Sharpen.Thread.Sleep(100);
				//Events only happen when the task is canceled
				Org.Mockito.Mockito.Verify(mockHandler, Org.Mockito.Mockito.Times(2)).Handle(Matchers.Any
					<Org.Apache.Hadoop.Yarn.Event.Event>());
			}
			finally
			{
				hb.Stop();
			}
		}
	}
}
