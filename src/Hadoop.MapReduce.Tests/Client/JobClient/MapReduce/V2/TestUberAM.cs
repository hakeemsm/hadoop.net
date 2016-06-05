using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestUberAM : TestMRJobs
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestUberAM));

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			TestMRJobs.Setup();
			if (mrCluster != null)
			{
				mrCluster.GetConfig().SetBoolean(MRJobConfig.JobUbertaskEnable, true);
				mrCluster.GetConfig().SetInt(MRJobConfig.JobUbertaskMaxreduces, 3);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public override void TestSleepJob()
		{
			numSleepReducers = 1;
			base.TestSleepJob();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSleepJobWithMultipleReducers()
		{
			numSleepReducers = 3;
			base.TestSleepJob();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void VerifySleepJobCounters(Job job)
		{
			Counters counters = job.GetCounters();
			base.VerifySleepJobCounters(job);
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.NumUberSubmaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(numSleepReducers, counters.FindCounter(JobCounter
				.NumUberSubreduces).GetValue());
			NUnit.Framework.Assert.AreEqual(3 + numSleepReducers, counters.FindCounter(JobCounter
				.TotalLaunchedUbertasks).GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public override void TestRandomWriter()
		{
			base.TestRandomWriter();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void VerifyRandomWriterCounters(Job job)
		{
			base.VerifyRandomWriterCounters(job);
			Counters counters = job.GetCounters();
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.NumUberSubmaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(3, counters.FindCounter(JobCounter.TotalLaunchedUbertasks
				).GetValue());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		[NUnit.Framework.Test]
		public override void TestFailingMapper()
		{
			Log.Info("\n\n\nStarting uberized testFailingMapper().");
			if (!(new FilePath(MiniMRYarnCluster.Appjar)).Exists())
			{
				Log.Info("MRAppJar " + MiniMRYarnCluster.Appjar + " not found. Not running test."
					);
				return;
			}
			Job job = RunFailingMapperJob();
			// should be able to get diags for single task attempt...
			TaskID taskID = new TaskID(job.GetJobID(), TaskType.Map, 0);
			TaskAttemptID aId = new TaskAttemptID(taskID, 0);
			System.Console.Out.WriteLine("Diagnostics for " + aId + " :");
			foreach (string diag in job.GetTaskDiagnostics(aId))
			{
				System.Console.Out.WriteLine(diag);
			}
			// ...but not for second (shouldn't exist:  uber-AM overrode max attempts)
			bool secondTaskAttemptExists = true;
			try
			{
				aId = new TaskAttemptID(taskID, 1);
				System.Console.Out.WriteLine("Diagnostics for " + aId + " :");
				foreach (string diag_1 in job.GetTaskDiagnostics(aId))
				{
					System.Console.Out.WriteLine(diag_1);
				}
			}
			catch (Exception)
			{
				secondTaskAttemptExists = false;
			}
			NUnit.Framework.Assert.AreEqual(false, secondTaskAttemptExists);
			TaskCompletionEvent[] events = job.GetTaskCompletionEvents(0, 2);
			NUnit.Framework.Assert.AreEqual(1, events.Length);
			// TIPFAILED if it comes from the AM, FAILED if it comes from the JHS
			TaskCompletionEvent.Status status = events[0].GetStatus();
			NUnit.Framework.Assert.IsTrue(status == TaskCompletionEvent.Status.Failed || status
				 == TaskCompletionEvent.Status.Tipfailed);
			NUnit.Framework.Assert.AreEqual(JobStatus.State.Failed, job.GetJobState());
		}

		//Disabling till UberAM honors MRJobConfig.MAP_MAX_ATTEMPTS
		//verifyFailingMapperCounters(job);
		// TODO later:  add explicit "isUber()" checks of some sort
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void VerifyFailingMapperCounters(Job job)
		{
			Counters counters = job.GetCounters();
			base.VerifyFailingMapperCounters(job);
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.TotalLaunchedUbertasks
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.NumUberSubmaps
				).GetValue());
			NUnit.Framework.Assert.AreEqual(2, counters.FindCounter(JobCounter.NumFailedUbertasks
				).GetValue());
		}

		//@Test  //FIXME:  if/when the corresponding TestMRJobs test gets enabled, do so here as well (potentially with mods for ubermode)
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public override void TestSleepJobWithSecurityOn()
		{
			base.TestSleepJobWithSecurityOn();
		}
	}
}
