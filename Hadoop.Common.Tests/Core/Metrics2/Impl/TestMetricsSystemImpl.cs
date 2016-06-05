using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Configuration;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Test the MetricsSystemImpl class</summary>
	public class TestMetricsSystemImpl
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMetricsSystemImpl)
			);

		static TestMetricsSystemImpl()
		{
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		[Captor]
		private ArgumentCaptor<MetricsRecord> r1;

		[Captor]
		private ArgumentCaptor<MetricsRecord> r2;

		private static string hostname = MetricsSystemImpl.GetHostname();

		public class TestSink : MetricsSink
		{
			public virtual void PutMetrics(MetricsRecord record)
			{
				Log.Debug(record);
			}

			public virtual void Flush()
			{
			}

			public virtual void Init(SubsetConfiguration conf)
			{
				Log.Debug(MetricsConfig.ToString(conf));
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitFirstVerifyStopInvokedImmediately()
		{
			DefaultMetricsSystem.Shutdown();
			new ConfigBuilder().Add("*.period", 8).Add("test.sink.test.class", typeof(TestMetricsSystemImpl.TestSink
				).FullName).Add("test.*.source.filter.exclude", "s0").Add("test.source.s1.metric.filter.exclude"
				, "X*").Add("test.sink.sink1.metric.filter.exclude", "Y*").Add("test.sink.sink2.metric.filter.exclude"
				, "Y*").Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"));
			//.add("test.sink.plugin.urls", getPluginUrlsAsString())
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			ms.Register("s0", "s0 desc", new TestMetricsSystemImpl.TestSource("s0rec"));
			TestMetricsSystemImpl.TestSource s1 = ms.Register("s1", "s1 desc", new TestMetricsSystemImpl.TestSource
				("s1rec"));
			s1.c1.Incr();
			s1.xxx.Incr();
			s1.g1.Set(2);
			s1.yyy.Incr(2);
			s1.s1.Add(0);
			MetricsSink sink1 = Org.Mockito.Mockito.Mock<MetricsSink>();
			MetricsSink sink2 = Org.Mockito.Mockito.Mock<MetricsSink>();
			ms.RegisterSink("sink1", "sink1 desc", sink1);
			ms.RegisterSink("sink2", "sink2 desc", sink2);
			ms.PublishMetricsNow();
			// publish the metrics
			ms.Stop();
			ms.Shutdown();
			//When we call stop, at most two sources will be consumed by each sink thread.
			Org.Mockito.Mockito.Verify(sink1, Org.Mockito.Mockito.AtMost(2)).PutMetrics(r1.Capture
				());
			IList<MetricsRecord> mr1 = r1.GetAllValues();
			Org.Mockito.Mockito.Verify(sink2, Org.Mockito.Mockito.AtMost(2)).PutMetrics(r2.Capture
				());
			IList<MetricsRecord> mr2 = r2.GetAllValues();
			if (mr1.Count != 0 && mr2.Count != 0)
			{
				CheckMetricsRecords(mr1);
				MoreAsserts.AssertEquals("output", mr1, mr2);
			}
			else
			{
				if (mr1.Count != 0)
				{
					CheckMetricsRecords(mr1);
				}
				else
				{
					if (mr2.Count != 0)
					{
						CheckMetricsRecords(mr2);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitFirstVerifyCallBacks()
		{
			DefaultMetricsSystem.Shutdown();
			new ConfigBuilder().Add("*.period", 8).Add("test.sink.test.class", typeof(TestMetricsSystemImpl.TestSink
				).FullName).Add("test.*.source.filter.exclude", "s0").Add("test.source.s1.metric.filter.exclude"
				, "X*").Add("test.sink.sink1.metric.filter.exclude", "Y*").Add("test.sink.sink2.metric.filter.exclude"
				, "Y*").Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"));
			//.add("test.sink.plugin.urls", getPluginUrlsAsString())
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			ms.Register("s0", "s0 desc", new TestMetricsSystemImpl.TestSource("s0rec"));
			TestMetricsSystemImpl.TestSource s1 = ms.Register("s1", "s1 desc", new TestMetricsSystemImpl.TestSource
				("s1rec"));
			s1.c1.Incr();
			s1.xxx.Incr();
			s1.g1.Set(2);
			s1.yyy.Incr(2);
			s1.s1.Add(0);
			MetricsSink sink1 = Org.Mockito.Mockito.Mock<MetricsSink>();
			MetricsSink sink2 = Org.Mockito.Mockito.Mock<MetricsSink>();
			ms.RegisterSink("sink1", "sink1 desc", sink1);
			ms.RegisterSink("sink2", "sink2 desc", sink2);
			ms.PublishMetricsNow();
			// publish the metrics
			try
			{
				Org.Mockito.Mockito.Verify(sink1, Org.Mockito.Mockito.Timeout(200).Times(2)).PutMetrics
					(r1.Capture());
				Org.Mockito.Mockito.Verify(sink2, Org.Mockito.Mockito.Timeout(200).Times(2)).PutMetrics
					(r2.Capture());
			}
			finally
			{
				ms.Stop();
				ms.Shutdown();
			}
			//When we call stop, at most two sources will be consumed by each sink thread.
			IList<MetricsRecord> mr1 = r1.GetAllValues();
			IList<MetricsRecord> mr2 = r2.GetAllValues();
			CheckMetricsRecords(mr1);
			MoreAsserts.AssertEquals("output", mr1, mr2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMultiThreadedPublish()
		{
			int numThreads = 10;
			new ConfigBuilder().Add("*.period", 80).Add("test.sink.collector." + MetricsConfig
				.QueueCapacityKey, numThreads).Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"
				));
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			TestMetricsSystemImpl.CollectingSink sink = new TestMetricsSystemImpl.CollectingSink
				(numThreads);
			ms.RegisterSink("collector", "Collector of values from all threads.", sink);
			TestMetricsSystemImpl.TestSource[] sources = new TestMetricsSystemImpl.TestSource
				[numThreads];
			Sharpen.Thread[] threads = new Sharpen.Thread[numThreads];
			string[] results = new string[numThreads];
			CyclicBarrier barrier1 = new CyclicBarrier(numThreads);
			CyclicBarrier barrier2 = new CyclicBarrier(numThreads);
			for (int i = 0; i < numThreads; i++)
			{
				sources[i] = ms.Register("threadSource" + i, "A source of my threaded goodness.", 
					new TestMetricsSystemImpl.TestSource("threadSourceRec" + i));
				threads[i] = new Sharpen.Thread(new _Runnable_194(results, sink, barrier1, sources
					, ms, barrier2), string.Empty + i);
			}
			// Wait for all the threads to come here so we can hammer
			// the system at the same time
			// Since some other thread may have snatched my metric,
			// I need to wait for the threads to finish before checking.
			foreach (Sharpen.Thread t in threads)
			{
				t.Start();
			}
			foreach (Sharpen.Thread t_1 in threads)
			{
				t_1.Join();
			}
			Assert.Equal(0L, ms.droppedPubAll.Value());
			Assert.True(StringUtils.Join("\n", Arrays.AsList(results)), Iterables
				.All(Arrays.AsList(results), new _Predicate_240()));
			ms.Stop();
			ms.Shutdown();
		}

		private sealed class _Runnable_194 : Runnable
		{
			public _Runnable_194(string[] results, TestMetricsSystemImpl.CollectingSink sink, 
				CyclicBarrier barrier1, TestMetricsSystemImpl.TestSource[] sources, MetricsSystemImpl
				 ms, CyclicBarrier barrier2)
			{
				this.results = results;
				this.sink = sink;
				this.barrier1 = barrier1;
				this.sources = sources;
				this.ms = ms;
				this.barrier2 = barrier2;
			}

			private bool SafeAwait(int mySource, CyclicBarrier barrier)
			{
				try
				{
					barrier.Await(2, TimeUnit.Seconds);
				}
				catch (Exception)
				{
					results[mySource] = "Interrupted";
					return false;
				}
				catch (BrokenBarrierException)
				{
					results[mySource] = "Broken Barrier";
					return false;
				}
				catch (TimeoutException)
				{
					results[mySource] = "Timed out on barrier";
					return false;
				}
				return true;
			}

			public void Run()
			{
				int mySource = System.Convert.ToInt32(Sharpen.Thread.CurrentThread().GetName());
				if (sink.collected[mySource].Get() != 0L)
				{
					results[mySource] = "Someone else collected my metric!";
					return;
				}
				if (!this.SafeAwait(mySource, barrier1))
				{
					return;
				}
				sources[mySource].g1.Set(230);
				ms.PublishMetricsNow();
				if (!this.SafeAwait(mySource, barrier2))
				{
					return;
				}
				if (sink.collected[mySource].Get() != 230L)
				{
					results[mySource] = "Metric not collected!";
					return;
				}
				results[mySource] = "Passed";
			}

			private readonly string[] results;

			private readonly TestMetricsSystemImpl.CollectingSink sink;

			private readonly CyclicBarrier barrier1;

			private readonly TestMetricsSystemImpl.TestSource[] sources;

			private readonly MetricsSystemImpl ms;

			private readonly CyclicBarrier barrier2;
		}

		private sealed class _Predicate_240 : Predicate<string>
		{
			public _Predicate_240()
			{
			}

			public bool Apply(string input)
			{
				return Sharpen.Runtime.EqualsIgnoreCase(input, "Passed");
			}
		}

		private class CollectingSink : MetricsSink
		{
			private readonly AtomicLong[] collected;

			public CollectingSink(int capacity)
			{
				collected = new AtomicLong[capacity];
				for (int i = 0; i < capacity; i++)
				{
					collected[i] = new AtomicLong();
				}
			}

			public virtual void Init(SubsetConfiguration conf)
			{
			}

			public virtual void PutMetrics(MetricsRecord record)
			{
				string prefix = "threadSourceRec";
				if (record.Name().StartsWith(prefix))
				{
					int recordNumber = System.Convert.ToInt32(Sharpen.Runtime.Substring(record.Name()
						, prefix.Length));
					AList<string> names = new AList<string>();
					foreach (AbstractMetric m in record.Metrics())
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(m.Name(), "g1"))
						{
							collected[recordNumber].Set(m.Value());
							return;
						}
						names.AddItem(m.Name());
					}
				}
			}

			public virtual void Flush()
			{
			}
		}

		[Fact]
		public virtual void TestHangingSink()
		{
			new ConfigBuilder().Add("*.period", 8).Add("test.sink.test.class", typeof(TestMetricsSystemImpl.TestSink
				).FullName).Add("test.sink.hanging.retry.delay", "1").Add("test.sink.hanging.retry.backoff"
				, "1.01").Add("test.sink.hanging.retry.count", "0").Save(TestMetricsConfig.GetTestFilename
				("hadoop-metrics2-test"));
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			TestMetricsSystemImpl.TestSource s = ms.Register("s3", "s3 desc", new TestMetricsSystemImpl.TestSource
				("s3rec"));
			s.c1.Incr();
			TestMetricsSystemImpl.HangingSink hanging = new TestMetricsSystemImpl.HangingSink
				();
			ms.RegisterSink("hanging", "Hang the sink!", hanging);
			ms.PublishMetricsNow();
			Assert.Equal(1L, ms.droppedPubAll.Value());
			NUnit.Framework.Assert.IsFalse(hanging.GetInterrupted());
			ms.Stop();
			ms.Shutdown();
			Assert.True(hanging.GetInterrupted());
			Assert.True("The sink didn't get called after its first hang " 
				+ "for subsequent records.", hanging.GetGotCalledSecondTime());
		}

		private class HangingSink : MetricsSink
		{
			private volatile bool interrupted;

			private bool gotCalledSecondTime;

			private bool firstTime = true;

			public virtual bool GetGotCalledSecondTime()
			{
				return gotCalledSecondTime;
			}

			public virtual bool GetInterrupted()
			{
				return interrupted;
			}

			public virtual void Init(SubsetConfiguration conf)
			{
			}

			public virtual void PutMetrics(MetricsRecord record)
			{
				// No need to hang every time, just the first record.
				if (!firstTime)
				{
					gotCalledSecondTime = true;
					return;
				}
				firstTime = false;
				try
				{
					Sharpen.Thread.Sleep(10 * 1000);
				}
				catch (Exception)
				{
					interrupted = true;
				}
			}

			public virtual void Flush()
			{
			}
		}

		[Fact]
		public virtual void TestRegisterDups()
		{
			MetricsSystem ms = new MetricsSystemImpl();
			TestMetricsSystemImpl.TestSource ts1 = new TestMetricsSystemImpl.TestSource("ts1"
				);
			TestMetricsSystemImpl.TestSource ts2 = new TestMetricsSystemImpl.TestSource("ts2"
				);
			ms.Register("ts1", string.Empty, ts1);
			MetricsSource s1 = ms.GetSource("ts1");
			NUnit.Framework.Assert.IsNotNull(s1);
			// should work when metrics system is not started
			ms.Register("ts1", string.Empty, ts2);
			MetricsSource s2 = ms.GetSource("ts1");
			NUnit.Framework.Assert.IsNotNull(s2);
			NUnit.Framework.Assert.AreNotSame(s1, s2);
			ms.Shutdown();
		}

		public virtual void TestRegisterDupError()
		{
			MetricsSystem ms = new MetricsSystemImpl("test");
			TestMetricsSystemImpl.TestSource ts = new TestMetricsSystemImpl.TestSource("ts");
			ms.Register(ts);
			ms.Register(ts);
		}

		[Fact]
		public virtual void TestStartStopStart()
		{
			DefaultMetricsSystem.Shutdown();
			// Clear pre-existing source names.
			MetricsSystemImpl ms = new MetricsSystemImpl("test");
			TestMetricsSystemImpl.TestSource ts = new TestMetricsSystemImpl.TestSource("ts");
			ms.Start();
			ms.Register("ts", string.Empty, ts);
			MetricsSourceAdapter sa = ms.GetSourceAdapter("ts");
			NUnit.Framework.Assert.IsNotNull(sa);
			NUnit.Framework.Assert.IsNotNull(sa.GetMBeanName());
			ms.Stop();
			ms.Shutdown();
			ms.Start();
			sa = ms.GetSourceAdapter("ts");
			NUnit.Framework.Assert.IsNotNull(sa);
			NUnit.Framework.Assert.IsNotNull(sa.GetMBeanName());
			ms.Stop();
			ms.Shutdown();
		}

		[Fact]
		public virtual void TestUnregisterSource()
		{
			MetricsSystem ms = new MetricsSystemImpl();
			TestMetricsSystemImpl.TestSource ts1 = new TestMetricsSystemImpl.TestSource("ts1"
				);
			TestMetricsSystemImpl.TestSource ts2 = new TestMetricsSystemImpl.TestSource("ts2"
				);
			ms.Register("ts1", string.Empty, ts1);
			ms.Register("ts2", string.Empty, ts2);
			MetricsSource s1 = ms.GetSource("ts1");
			NUnit.Framework.Assert.IsNotNull(s1);
			// should work when metrics system is not started
			ms.UnregisterSource("ts1");
			s1 = ms.GetSource("ts1");
			NUnit.Framework.Assert.IsNull(s1);
			MetricsSource s2 = ms.GetSource("ts2");
			NUnit.Framework.Assert.IsNotNull(s2);
			ms.Shutdown();
		}

		[Fact]
		public virtual void TestRegisterSourceWithoutName()
		{
			MetricsSystem ms = new MetricsSystemImpl();
			TestMetricsSystemImpl.TestSource ts = new TestMetricsSystemImpl.TestSource("ts");
			TestMetricsSystemImpl.TestSource2 ts2 = new TestMetricsSystemImpl.TestSource2("ts2"
				);
			ms.Register(ts);
			ms.Register(ts2);
			ms.Init("TestMetricsSystem");
			// if metrics source is registered without name,
			// the class name will be used as the name
			MetricsSourceAdapter sa = ((MetricsSystemImpl)ms).GetSourceAdapter("TestSource");
			NUnit.Framework.Assert.IsNotNull(sa);
			MetricsSourceAdapter sa2 = ((MetricsSystemImpl)ms).GetSourceAdapter("TestSource2"
				);
			NUnit.Framework.Assert.IsNotNull(sa2);
			ms.Shutdown();
		}

		private void CheckMetricsRecords(IList<MetricsRecord> recs)
		{
			Log.Debug(recs);
			MetricsRecord r = recs[0];
			Assert.Equal("name", "s1rec", r.Name());
			MoreAsserts.AssertEquals("tags", new MetricsTag[] { Interns.Tag(MsInfo.Context, "test"
				), Interns.Tag(MsInfo.Hostname, hostname) }, r.Tags());
			MoreAsserts.AssertEquals("metrics", ((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl
				)((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl)MetricsLists.Builder(string.Empty
				).AddCounter(Interns.Info("C1", "C1 desc"), 1L)).AddGauge(Interns.Info("G1", "G1 desc"
				), 2L)).AddCounter(Interns.Info("S1NumOps", "Number of ops for s1"), 1L)).AddGauge
				(Interns.Info("S1AvgTime", "Average time for s1"), 0.0)).Metrics(), r.Metrics());
			r = recs[1];
			Assert.True("NumActiveSinks should be 3", Iterables.Contains(r.
				Metrics(), new MetricGaugeInt(MsInfo.NumActiveSinks, 3)));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestQSize()
		{
			new ConfigBuilder().Add("*.period", 8).Add("test.sink.test.class", typeof(TestMetricsSystemImpl.TestSink
				).FullName).Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"));
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			CountDownLatch proceedSignal = new CountDownLatch(1);
			CountDownLatch reachedPutMetricSignal = new CountDownLatch(1);
			ms.Start();
			try
			{
				MetricsSink slowSink = Org.Mockito.Mockito.Mock<MetricsSink>();
				MetricsSink dataSink = Org.Mockito.Mockito.Mock<MetricsSink>();
				ms.RegisterSink("slowSink", "The sink that will wait on putMetric", slowSink);
				ms.RegisterSink("dataSink", "The sink I'll use to get info about slowSink", dataSink
					);
				Org.Mockito.Mockito.DoAnswer(new _Answer_457(reachedPutMetricSignal, proceedSignal
					)).When(slowSink).PutMetrics(Any<MetricsRecord>());
				// trigger metric collection first time
				ms.OnTimerEvent();
				Assert.True(reachedPutMetricSignal.Await(1, TimeUnit.Seconds));
				// Now that the slow sink is still processing the first metric,
				// its queue length should be 1 for the second collection.
				ms.OnTimerEvent();
				Org.Mockito.Mockito.Verify(dataSink, Org.Mockito.Mockito.Timeout(500).Times(2)).PutMetrics
					(r1.Capture());
				IList<MetricsRecord> mr = r1.GetAllValues();
				Number qSize = Iterables.Find(mr[1].Metrics(), new _Predicate_475()).Value();
				Assert.Equal(1, qSize);
			}
			finally
			{
				proceedSignal.CountDown();
				ms.Stop();
			}
		}

		private sealed class _Answer_457 : Answer
		{
			public _Answer_457(CountDownLatch reachedPutMetricSignal, CountDownLatch proceedSignal
				)
			{
				this.reachedPutMetricSignal = reachedPutMetricSignal;
				this.proceedSignal = proceedSignal;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				reachedPutMetricSignal.CountDown();
				proceedSignal.Await();
				return null;
			}

			private readonly CountDownLatch reachedPutMetricSignal;

			private readonly CountDownLatch proceedSignal;
		}

		private sealed class _Predicate_475 : Predicate<AbstractMetric>
		{
			public _Predicate_475()
			{
			}

			public bool Apply(AbstractMetric input)
			{
				System.Diagnostics.Debug.Assert(input != null);
				return input.Name().Equals("Sink_slowSinkQsize");
			}
		}

		/// <summary>Class to verify HADOOP-11932.</summary>
		/// <remarks>
		/// Class to verify HADOOP-11932. Instead of reading from HTTP, going in loop
		/// until closed.
		/// </remarks>
		private class TestClosableSink : MetricsSink, IDisposable
		{
			internal bool closed = false;

			internal CountDownLatch collectingLatch;

			public TestClosableSink(CountDownLatch collectingLatch)
			{
				this.collectingLatch = collectingLatch;
			}

			public virtual void Init(SubsetConfiguration conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				closed = true;
			}

			public virtual void PutMetrics(MetricsRecord record)
			{
				while (!closed)
				{
					collectingLatch.CountDown();
				}
			}

			public virtual void Flush()
			{
			}
		}

		/// <summary>HADOOP-11932</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestHangOnSinkRead()
		{
			new ConfigBuilder().Add("*.period", 8).Add("test.sink.test.class", typeof(TestMetricsSystemImpl.TestSink
				).FullName).Save(TestMetricsConfig.GetTestFilename("hadoop-metrics2-test"));
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			try
			{
				CountDownLatch collectingLatch = new CountDownLatch(1);
				MetricsSink sink = new TestMetricsSystemImpl.TestClosableSink(collectingLatch);
				ms.RegisterSink("closeableSink", "The sink will be used to test closeability", sink
					);
				// trigger metric collection first time
				ms.OnTimerEvent();
				// Make sure that sink is collecting metrics
				Assert.True(collectingLatch.Await(1, TimeUnit.Seconds));
			}
			finally
			{
				ms.Stop();
			}
		}

		private class TestSource
		{
			internal MutableCounterLong c1;

			internal MutableCounterLong xxx;

			internal MutableGaugeLong g1;

			internal MutableGaugeLong yyy;

			[Metric]
			internal MutableRate s1;

			internal readonly MetricsRegistry registry;

			internal TestSource(string recName)
			{
				registry = new MetricsRegistry(recName);
			}
		}

		private class TestSource2
		{
			internal MutableCounterLong c1;

			internal MutableCounterLong xxx;

			internal MutableGaugeLong g1;

			internal MutableGaugeLong yyy;

			[Metric]
			internal MutableRate s1;

			internal readonly MetricsRegistry registry;

			internal TestSource2(string recName)
			{
				registry = new MetricsRegistry(recName);
			}
		}

		private static string GetPluginUrlsAsString()
		{
			return "file:metrics2-test-plugin.jar";
		}
	}
}
