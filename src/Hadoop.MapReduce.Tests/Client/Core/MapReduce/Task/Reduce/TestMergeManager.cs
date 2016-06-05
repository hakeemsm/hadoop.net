using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class TestMergeManager
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestMemoryMerge()
		{
			int TotalMemBytes = 10000;
			int OutputSize = 7950;
			JobConf conf = new JobConf();
			conf.SetFloat(MRJobConfig.ShuffleInputBufferPercent, 1.0f);
			conf.SetLong(MRJobConfig.ReduceMemoryTotalBytes, TotalMemBytes);
			conf.SetFloat(MRJobConfig.ShuffleMemoryLimitPercent, 0.8f);
			conf.SetFloat(MRJobConfig.ShuffleMergePercent, 0.9f);
			TestMergeManager.TestExceptionReporter reporter = new TestMergeManager.TestExceptionReporter
				();
			CyclicBarrier mergeStart = new CyclicBarrier(2);
			CyclicBarrier mergeComplete = new CyclicBarrier(2);
			TestMergeManager.StubbedMergeManager mgr = new TestMergeManager.StubbedMergeManager
				(conf, reporter, mergeStart, mergeComplete);
			// reserve enough map output to cause a merge when it is committed
			MapOutput<Text, Text> out1 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.IsTrue("Should be a memory merge", (out1 is InMemoryMapOutput
				));
			InMemoryMapOutput<Text, Text> mout1 = (InMemoryMapOutput<Text, Text>)out1;
			FillOutput(mout1);
			MapOutput<Text, Text> out2 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.IsTrue("Should be a memory merge", (out2 is InMemoryMapOutput
				));
			InMemoryMapOutput<Text, Text> mout2 = (InMemoryMapOutput<Text, Text>)out2;
			FillOutput(mout2);
			// next reservation should be a WAIT
			MapOutput<Text, Text> out3 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.AreEqual("Should be told to wait", null, out3);
			// trigger the first merge and wait for merge thread to start merging
			// and free enough output to reserve more
			mout1.Commit();
			mout2.Commit();
			mergeStart.Await();
			NUnit.Framework.Assert.AreEqual(1, mgr.GetNumMerges());
			// reserve enough map output to cause another merge when committed
			out1 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.IsTrue("Should be a memory merge", (out1 is InMemoryMapOutput
				));
			mout1 = (InMemoryMapOutput<Text, Text>)out1;
			FillOutput(mout1);
			out2 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.IsTrue("Should be a memory merge", (out2 is InMemoryMapOutput
				));
			mout2 = (InMemoryMapOutput<Text, Text>)out2;
			FillOutput(mout2);
			// next reservation should be null
			out3 = mgr.Reserve(null, OutputSize, 0);
			NUnit.Framework.Assert.AreEqual("Should be told to wait", null, out3);
			// commit output *before* merge thread completes
			mout1.Commit();
			mout2.Commit();
			// allow the first merge to complete
			mergeComplete.Await();
			// start the second merge and verify
			mergeStart.Await();
			NUnit.Framework.Assert.AreEqual(2, mgr.GetNumMerges());
			// trigger the end of the second merge
			mergeComplete.Await();
			NUnit.Framework.Assert.AreEqual(2, mgr.GetNumMerges());
			NUnit.Framework.Assert.AreEqual("exception reporter invoked", 0, reporter.GetNumExceptions
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private void FillOutput(InMemoryMapOutput<Text, Text> output)
		{
			BoundedByteArrayOutputStream stream = output.GetArrayStream();
			int count = stream.GetLimit();
			for (int i = 0; i < count; ++i)
			{
				stream.Write(i);
			}
		}

		private class StubbedMergeManager : MergeManagerImpl<Text, Text>
		{
			private TestMergeManager.TestMergeThread mergeThread;

			public StubbedMergeManager(JobConf conf, ExceptionReporter reporter, CyclicBarrier
				 mergeStart, CyclicBarrier mergeComplete)
				: base(null, conf, Org.Mockito.Mockito.Mock<LocalFileSystem>(), null, null, null, 
					null, null, null, null, null, reporter, null, Org.Mockito.Mockito.Mock<MapOutputFile
					>())
			{
				mergeThread.SetSyncBarriers(mergeStart, mergeComplete);
			}

			protected internal override MergeThread<InMemoryMapOutput<Text, Text>, Text, Text
				> CreateInMemoryMerger()
			{
				mergeThread = new TestMergeManager.TestMergeThread(this, GetExceptionReporter());
				return mergeThread;
			}

			public virtual int GetNumMerges()
			{
				return mergeThread.GetNumMerges();
			}
		}

		private class TestMergeThread : MergeThread<InMemoryMapOutput<Text, Text>, Text, 
			Text>
		{
			private AtomicInteger numMerges;

			private CyclicBarrier mergeStart;

			private CyclicBarrier mergeComplete;

			public TestMergeThread(MergeManagerImpl<Text, Text> mergeManager, ExceptionReporter
				 reporter)
				: base(mergeManager, int.MaxValue, reporter)
			{
				numMerges = new AtomicInteger(0);
			}

			public virtual void SetSyncBarriers(CyclicBarrier mergeStart, CyclicBarrier mergeComplete
				)
			{
				lock (this)
				{
					this.mergeStart = mergeStart;
					this.mergeComplete = mergeComplete;
				}
			}

			public virtual int GetNumMerges()
			{
				return numMerges.Get();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Merge(IList<InMemoryMapOutput<Text, Text>> inputs)
			{
				lock (this)
				{
					numMerges.IncrementAndGet();
					foreach (InMemoryMapOutput<Text, Text> input in inputs)
					{
						manager.Unreserve(input.GetSize());
					}
				}
				try
				{
					mergeStart.Await();
					mergeComplete.Await();
				}
				catch (Exception)
				{
				}
				catch (BrokenBarrierException)
				{
				}
			}
		}

		private class TestExceptionReporter : ExceptionReporter
		{
			private IList<Exception> exceptions = new AList<Exception>();

			public virtual void ReportException(Exception t)
			{
				exceptions.AddItem(t);
				Sharpen.Runtime.PrintStackTrace(t);
			}

			public virtual int GetNumExceptions()
			{
				return exceptions.Count;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestOnDiskMerger()
		{
			JobConf jobConf = new JobConf();
			int SortFactor = 5;
			jobConf.SetInt(MRJobConfig.IoSortFactor, SortFactor);
			MapOutputFile mapOutputFile = new MROutputFiles();
			FileSystem fs = FileSystem.GetLocal(jobConf);
			MergeManagerImpl<IntWritable, IntWritable> manager = new MergeManagerImpl<IntWritable
				, IntWritable>(null, jobConf, fs, null, null, null, null, null, null, null, null
				, null, null, mapOutputFile);
			MergeThread<MapOutput<IntWritable, IntWritable>, IntWritable, IntWritable> onDiskMerger
				 = (MergeThread<MapOutput<IntWritable, IntWritable>, IntWritable, IntWritable>)Whitebox
				.GetInternalState(manager, "onDiskMerger");
			int mergeFactor = (int)Whitebox.GetInternalState(onDiskMerger, "mergeFactor");
			// make sure the io.sort.factor is set properly
			NUnit.Framework.Assert.AreEqual(mergeFactor, SortFactor);
			// Stop the onDiskMerger thread so that we can intercept the list of files
			// waiting to be merged.
			onDiskMerger.Suspend();
			//Send the list of fake files waiting to be merged
			Random rand = new Random();
			for (int i = 0; i < 2 * SortFactor; ++i)
			{
				Path path = new Path("somePath");
				MergeManagerImpl.CompressAwarePath cap = new MergeManagerImpl.CompressAwarePath(path
					, 1l, rand.Next());
				manager.CloseOnDiskFile(cap);
			}
			//Check that the files pending to be merged are in sorted order.
			List<IList<MergeManagerImpl.CompressAwarePath>> pendingToBeMerged = (List<IList<MergeManagerImpl.CompressAwarePath
				>>)Whitebox.GetInternalState(onDiskMerger, "pendingToBeMerged");
			NUnit.Framework.Assert.IsTrue("No inputs were added to list pending to merge", pendingToBeMerged
				.Count > 0);
			for (int i_1 = 0; i_1 < pendingToBeMerged.Count; ++i_1)
			{
				IList<MergeManagerImpl.CompressAwarePath> inputs = pendingToBeMerged[i_1];
				for (int j = 1; j < inputs.Count; ++j)
				{
					NUnit.Framework.Assert.IsTrue("Not enough / too many inputs were going to be merged"
						, inputs.Count > 0 && inputs.Count <= SortFactor);
					NUnit.Framework.Assert.IsTrue("Inputs to be merged were not sorted according to size: "
						, inputs[j].GetCompressedSize() >= inputs[j - 1].GetCompressedSize());
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLargeMemoryLimits()
		{
			JobConf conf = new JobConf();
			// Xmx in production
			conf.SetLong(MRJobConfig.ReduceMemoryTotalBytes, 8L * 1024 * 1024 * 1024);
			// M1 = Xmx fraction for map outputs
			conf.SetFloat(MRJobConfig.ShuffleInputBufferPercent, 1.0f);
			// M2 = max M1 fraction for a single maple output
			conf.SetFloat(MRJobConfig.ShuffleMemoryLimitPercent, 0.95f);
			// M3 = M1 fraction at which in memory merge is triggered
			conf.SetFloat(MRJobConfig.ShuffleMergePercent, 1.0f);
			// M4 = M1 fraction of map outputs remaining in memory for a reduce
			conf.SetFloat(MRJobConfig.ReduceInputBufferPercent, 1.0f);
			MergeManagerImpl<Text, Text> mgr = new MergeManagerImpl<Text, Text>(null, conf, Org.Mockito.Mockito.Mock
				<LocalFileSystem>(), null, null, null, null, null, null, null, null, null, null, 
				new MROutputFiles());
			NUnit.Framework.Assert.IsTrue("Large shuffle area unusable: " + mgr.memoryLimit, 
				mgr.memoryLimit > int.MaxValue);
			long maxInMemReduce = mgr.GetMaxInMemReduceLimit();
			NUnit.Framework.Assert.IsTrue("Large in-memory reduce area unusable: " + maxInMemReduce
				, maxInMemReduce > int.MaxValue);
		}
	}
}
