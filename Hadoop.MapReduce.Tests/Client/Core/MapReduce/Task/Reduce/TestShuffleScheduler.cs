using System;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class TestShuffleScheduler
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTipFailed()
		{
			JobConf job = new JobConf();
			job.SetNumMapTasks(2);
			TaskStatus status = new _TaskStatus_49();
			Progress progress = new Progress();
			TaskAttemptID reduceId = new TaskAttemptID("314159", 0, TaskType.Reduce, 0, 0);
			ShuffleSchedulerImpl scheduler = new ShuffleSchedulerImpl(job, status, reduceId, 
				null, progress, null, null, null);
			JobID jobId = new JobID();
			TaskID taskId1 = new TaskID(jobId, TaskType.Reduce, 1);
			scheduler.TipFailed(taskId1);
			NUnit.Framework.Assert.AreEqual("Progress should be 0.5", 0.5f, progress.GetProgress
				(), 0.0f);
			NUnit.Framework.Assert.IsFalse(scheduler.WaitUntilDone(1));
			TaskID taskId0 = new TaskID(jobId, TaskType.Reduce, 0);
			scheduler.TipFailed(taskId0);
			NUnit.Framework.Assert.AreEqual("Progress should be 1.0", 1.0f, progress.GetProgress
				(), 0.0f);
			NUnit.Framework.Assert.IsTrue(scheduler.WaitUntilDone(1));
		}

		private sealed class _TaskStatus_49 : TaskStatus
		{
			public _TaskStatus_49()
			{
			}

			public override bool GetIsMap()
			{
				return false;
			}

			public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAggregatedTransferRate<K, V>()
		{
			JobConf job = new JobConf();
			job.SetNumMapTasks(10);
			//mock creation
			TaskUmbilicalProtocol mockUmbilical = Org.Mockito.Mockito.Mock<TaskUmbilicalProtocol
				>();
			Reporter mockReporter = Org.Mockito.Mockito.Mock<Reporter>();
			FileSystem mockFileSystem = Org.Mockito.Mockito.Mock<FileSystem>();
			Type combinerClass = job.GetCombinerClass();
			Task.CombineOutputCollector<K, V> mockCombineOutputCollector = (Task.CombineOutputCollector
				<K, V>)Org.Mockito.Mockito.Mock<Task.CombineOutputCollector>();
			// needed for mock with generic
			TaskAttemptID mockTaskAttemptID = Org.Mockito.Mockito.Mock<TaskAttemptID>();
			LocalDirAllocator mockLocalDirAllocator = Org.Mockito.Mockito.Mock<LocalDirAllocator
				>();
			CompressionCodec mockCompressionCodec = Org.Mockito.Mockito.Mock<CompressionCodec
				>();
			Counters.Counter mockCounter = Org.Mockito.Mockito.Mock<Counters.Counter>();
			TaskStatus mockTaskStatus = Org.Mockito.Mockito.Mock<TaskStatus>();
			Progress mockProgress = Org.Mockito.Mockito.Mock<Progress>();
			MapOutputFile mockMapOutputFile = Org.Mockito.Mockito.Mock<MapOutputFile>();
			Org.Apache.Hadoop.Mapred.Task mockTask = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapred.Task
				>();
			MapOutput<K, V> output = Org.Mockito.Mockito.Mock<MapOutput>();
			ShuffleConsumerPlugin.Context<K, V> context = new ShuffleConsumerPlugin.Context<K
				, V>(mockTaskAttemptID, job, mockFileSystem, mockUmbilical, mockLocalDirAllocator
				, mockReporter, mockCompressionCodec, combinerClass, mockCombineOutputCollector, 
				mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockTaskStatus
				, mockProgress, mockProgress, mockTask, mockMapOutputFile, null);
			TaskStatus status = new _TaskStatus_115();
			Progress progress = new Progress();
			ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job, status
				, null, null, progress, context.GetShuffledMapsCounter(), context.GetReduceShuffleBytes
				(), context.GetFailedShuffleCounter());
			TaskAttemptID attemptID0 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 0), 0);
			//adding the 1st interval, 40MB from 60s to 100s
			long bytes = (long)40 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID0, new MapHost(null, null), bytes, 60000, 100000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(1, 1, 1), progress.ToString());
			TaskAttemptID attemptID1 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 1), 1);
			//adding the 2nd interval before the 1st interval, 50MB from 0s to 50s
			bytes = (long)50 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID1, new MapHost(null, null), bytes, 0, 50000, output
				);
			NUnit.Framework.Assert.AreEqual(CopyMessage(2, 1, 1), progress.ToString());
			TaskAttemptID attemptID2 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 2), 2);
			//adding the 3rd interval overlapping with the 1st and the 2nd interval
			//110MB from 25s to 80s
			bytes = (long)110 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID2, new MapHost(null, null), bytes, 25000, 80000, 
				output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(3, 2, 2), progress.ToString());
			TaskAttemptID attemptID3 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 3), 3);
			//adding the 4th interval just after the 2nd interval, 100MB from 100s to 300s
			bytes = (long)100 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID3, new MapHost(null, null), bytes, 100000, 300000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(4, 0.5, 1), progress.ToString());
			TaskAttemptID attemptID4 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 4), 4);
			//adding the 5th interval between after 4th, 50MB from 350s to 400s
			bytes = (long)50 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID4, new MapHost(null, null), bytes, 350000, 400000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(5, 1, 1), progress.ToString());
			TaskAttemptID attemptID5 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 5), 5);
			//adding the 6th interval between after 5th, 50MB from 450s to 500s
			bytes = (long)50 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID5, new MapHost(null, null), bytes, 450000, 500000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(6, 1, 1), progress.ToString());
			TaskAttemptID attemptID6 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 6), 6);
			//adding the 7th interval between after 5th and 6th interval, 20MB from 320s to 340s
			bytes = (long)20 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID6, new MapHost(null, null), bytes, 320000, 340000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(7, 1, 1), progress.ToString());
			TaskAttemptID attemptID7 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 7), 7);
			//adding the 8th interval overlapping with 4th, 5th, and 7th 30MB from 290s to 350s
			bytes = (long)30 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID7, new MapHost(null, null), bytes, 290000, 350000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(8, 0.5, 1), progress.ToString());
			TaskAttemptID attemptID8 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 8), 8);
			//adding the 9th interval overlapping with 5th and 6th, 50MB from 400s to 450s
			bytes = (long)50 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID8, new MapHost(null, null), bytes, 400000, 450000
				, output);
			NUnit.Framework.Assert.AreEqual(CopyMessage(9, 1, 1), progress.ToString());
			TaskAttemptID attemptID9 = new TaskAttemptID(new TaskID(new JobID("test", 0), TaskType
				.Map, 9), 9);
			//adding the 10th interval overlapping with all intervals, 500MB from 0s to 500s
			bytes = (long)500 * 1024 * 1024;
			scheduler.CopySucceeded(attemptID9, new MapHost(null, null), bytes, 0, 500000, output
				);
			NUnit.Framework.Assert.AreEqual(CopyMessage(10, 1, 2), progress.ToString());
		}

		private sealed class _TaskStatus_115 : TaskStatus
		{
			public _TaskStatus_115()
			{
			}

			public override bool GetIsMap()
			{
				return false;
			}

			public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSucceedAndFailedCopyMap<K, V>()
		{
			JobConf job = new JobConf();
			job.SetNumMapTasks(2);
			//mock creation
			TaskUmbilicalProtocol mockUmbilical = Org.Mockito.Mockito.Mock<TaskUmbilicalProtocol
				>();
			Reporter mockReporter = Org.Mockito.Mockito.Mock<Reporter>();
			FileSystem mockFileSystem = Org.Mockito.Mockito.Mock<FileSystem>();
			Type combinerClass = job.GetCombinerClass();
			Task.CombineOutputCollector<K, V> mockCombineOutputCollector = (Task.CombineOutputCollector
				<K, V>)Org.Mockito.Mockito.Mock<Task.CombineOutputCollector>();
			// needed for mock with generic
			TaskAttemptID mockTaskAttemptID = Org.Mockito.Mockito.Mock<TaskAttemptID>();
			LocalDirAllocator mockLocalDirAllocator = Org.Mockito.Mockito.Mock<LocalDirAllocator
				>();
			CompressionCodec mockCompressionCodec = Org.Mockito.Mockito.Mock<CompressionCodec
				>();
			Counters.Counter mockCounter = Org.Mockito.Mockito.Mock<Counters.Counter>();
			TaskStatus mockTaskStatus = Org.Mockito.Mockito.Mock<TaskStatus>();
			Progress mockProgress = Org.Mockito.Mockito.Mock<Progress>();
			MapOutputFile mockMapOutputFile = Org.Mockito.Mockito.Mock<MapOutputFile>();
			Org.Apache.Hadoop.Mapred.Task mockTask = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapred.Task
				>();
			MapOutput<K, V> output = Org.Mockito.Mockito.Mock<MapOutput>();
			ShuffleConsumerPlugin.Context<K, V> context = new ShuffleConsumerPlugin.Context<K
				, V>(mockTaskAttemptID, job, mockFileSystem, mockUmbilical, mockLocalDirAllocator
				, mockReporter, mockCompressionCodec, combinerClass, mockCombineOutputCollector, 
				mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockCounter, mockTaskStatus
				, mockProgress, mockProgress, mockTask, mockMapOutputFile, null);
			TaskStatus status = new _TaskStatus_251();
			Progress progress = new Progress();
			ShuffleSchedulerImpl<K, V> scheduler = new ShuffleSchedulerImpl<K, V>(job, status
				, null, null, progress, context.GetShuffledMapsCounter(), context.GetReduceShuffleBytes
				(), context.GetFailedShuffleCounter());
			MapHost host1 = new MapHost("host1", null);
			TaskAttemptID failedAttemptID = new TaskAttemptID(new TaskID(new JobID("test", 0)
				, TaskType.Map, 0), 0);
			TaskAttemptID succeedAttemptID = new TaskAttemptID(new TaskID(new JobID("test", 0
				), TaskType.Map, 1), 1);
			// handle output fetch failure for failedAttemptID, part I
			scheduler.HostFailed(host1.GetHostName());
			// handle output fetch succeed for succeedAttemptID
			long bytes = (long)500 * 1024 * 1024;
			scheduler.CopySucceeded(succeedAttemptID, host1, bytes, 0, 500000, output);
			// handle output fetch failure for failedAttemptID, part II
			// for MAPREDUCE-6361: verify no NPE exception get thrown out
			scheduler.CopyFailed(failedAttemptID, host1, true, false);
		}

		private sealed class _TaskStatus_251 : TaskStatus
		{
			public _TaskStatus_251()
			{
			}

			public override bool GetIsMap()
			{
				return false;
			}

			public override void AddFetchFailedMap(TaskAttemptID mapTaskId)
			{
			}
		}

		private static string CopyMessage(int attemptNo, double rate1, double rate2)
		{
			int attemptZero = attemptNo - 1;
			return string.Format("copy task(attempt_test_0000_m_%06d_%d succeeded at %1.2f MB/s)"
				 + " Aggregated copy rate(%d of 10 at %1.2f MB/s)", attemptZero, attemptZero, rate1
				, attemptNo, rate2);
		}
	}
}
