using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestReduceFetch : TestReduceFetchFromPartialMem
	{
		static TestReduceFetch()
		{
			SetSuite(typeof(TestReduceFetch));
		}

		/// <summary>Verify that all segments are read from disk</summary>
		/// <exception cref="System.Exception">might be thrown</exception>
		public virtual void TestReduceFromDisk()
		{
			int MapTasks = 8;
			JobConf job = mrCluster.CreateJobConf();
			job.Set(JobContext.ReduceInputBufferPercent, "0.0");
			job.SetNumMapTasks(MapTasks);
			job.Set(JobConf.MapredReduceTaskJavaOpts, "-Xmx128m");
			job.SetLong(JobContext.ReduceMemoryTotalBytes, 128 << 20);
			job.Set(JobContext.ShuffleInputBufferPercent, "0.05");
			job.SetInt(JobContext.IoSortFactor, 2);
			job.SetInt(JobContext.ReduceMergeInmemThreshold, 4);
			Counters c = RunJob(job);
			long spill = c.FindCounter(TaskCounter.SpilledRecords).GetCounter();
			long @out = c.FindCounter(TaskCounter.MapOutputRecords).GetCounter();
			NUnit.Framework.Assert.IsTrue("Expected all records spilled during reduce (" + spill
				 + ")", spill >= 2 * @out);
			// all records spill at map, reduce
			NUnit.Framework.Assert.IsTrue("Expected intermediate merges (" + spill + ")", spill
				 >= 2 * @out + (@out / MapTasks));
		}

		// some records hit twice
		/// <summary>Verify that no segment hits disk.</summary>
		/// <exception cref="System.Exception">might be thrown</exception>
		public virtual void TestReduceFromMem()
		{
			int MapTasks = 3;
			JobConf job = mrCluster.CreateJobConf();
			job.Set(JobContext.ReduceInputBufferPercent, "1.0");
			job.Set(JobContext.ShuffleInputBufferPercent, "1.0");
			job.SetLong(JobContext.ReduceMemoryTotalBytes, 128 << 20);
			job.SetNumMapTasks(MapTasks);
			Counters c = RunJob(job);
			long spill = c.FindCounter(TaskCounter.SpilledRecords).GetCounter();
			long @out = c.FindCounter(TaskCounter.MapOutputRecords).GetCounter();
			NUnit.Framework.Assert.AreEqual("Spilled records: " + spill, @out, spill);
		}
		// no reduce spill
	}
}
