using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// Test to make sure that command line output for
	/// job monitoring is correct and prints 100% for map and reduce before
	/// successful completion.
	/// </summary>
	public class TestJobMonitorAndPrint : TestCase
	{
		private Job job;

		private Configuration conf;

		private ClientProtocol clientProtocol;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		protected override void SetUp()
		{
			conf = new Configuration();
			clientProtocol = Org.Mockito.Mockito.Mock<ClientProtocol>();
			Cluster cluster = Org.Mockito.Mockito.Mock<Cluster>();
			Org.Mockito.Mockito.When(cluster.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(cluster.GetClient()).ThenReturn(clientProtocol);
			JobStatus jobStatus = new JobStatus(new JobID("job_000", 1), 0f, 0f, 0f, 0f, JobStatus.State
				.Running, JobPriority.High, "tmp-user", "tmp-jobname", "tmp-jobfile", "tmp-url");
			job = Job.GetInstance(cluster, jobStatus, conf);
			job = Org.Mockito.Mockito.Spy(job);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobMonitorAndPrint()
		{
			JobStatus jobStatus_1 = new JobStatus(new JobID("job_000", 1), 1f, 0.1f, 0.1f, 0f
				, JobStatus.State.Running, JobPriority.High, "tmp-user", "tmp-jobname", "tmp-queue"
				, "tmp-jobfile", "tmp-url", true);
			JobStatus jobStatus_2 = new JobStatus(new JobID("job_000", 1), 1f, 1f, 1f, 1f, JobStatus.State
				.Succeeded, JobPriority.High, "tmp-user", "tmp-jobname", "tmp-queue", "tmp-jobfile"
				, "tmp-url", true);
			Org.Mockito.Mockito.DoAnswer(new _Answer_85()).When(job).GetTaskCompletionEvents(
				Matchers.AnyInt(), Matchers.AnyInt());
			Org.Mockito.Mockito.DoReturn(new TaskReport[5]).When(job).GetTaskReports(Matchers.IsA
				<TaskType>());
			Org.Mockito.Mockito.When(clientProtocol.GetJobStatus(Matchers.Any<JobID>())).ThenReturn
				(jobStatus_1, jobStatus_2);
			// setup the logger to capture all logs
			Layout layout = Logger.GetRootLogger().GetAppender("stdout").GetLayout();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			WriterAppender appender = new WriterAppender(layout, os);
			appender.SetThreshold(Level.All);
			Logger qlogger = Logger.GetLogger(typeof(Job));
			qlogger.AddAppender(appender);
			job.MonitorAndPrintJob();
			qlogger.RemoveAppender(appender);
			LineNumberReader r = new LineNumberReader(new StringReader(os.ToString()));
			string line;
			bool foundHundred = false;
			bool foundComplete = false;
			bool foundUber = false;
			string uberModeMatch = "uber mode : true";
			string progressMatch = "map 100% reduce 100%";
			string completionMatch = "completed successfully";
			while ((line = r.ReadLine()) != null)
			{
				if (line.Contains(uberModeMatch))
				{
					foundUber = true;
				}
				foundHundred = line.Contains(progressMatch);
				if (foundHundred)
				{
					break;
				}
			}
			line = r.ReadLine();
			foundComplete = line.Contains(completionMatch);
			NUnit.Framework.Assert.IsTrue(foundUber);
			NUnit.Framework.Assert.IsTrue(foundHundred);
			NUnit.Framework.Assert.IsTrue(foundComplete);
			System.Console.Out.WriteLine("The output of job.toString() is : \n" + job.ToString
				());
			NUnit.Framework.Assert.IsTrue(job.ToString().Contains("Number of maps: 5\n"));
			NUnit.Framework.Assert.IsTrue(job.ToString().Contains("Number of reduces: 5\n"));
		}

		private sealed class _Answer_85 : Answer<TaskCompletionEvent[]>
		{
			public _Answer_85()
			{
			}

			/// <exception cref="System.Exception"/>
			public TaskCompletionEvent[] Answer(InvocationOnMock invocation)
			{
				return new TaskCompletionEvent[0];
			}
		}
	}
}
