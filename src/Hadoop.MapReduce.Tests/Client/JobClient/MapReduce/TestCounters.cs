using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// TestCounters checks the sanity and recoverability of
	/// <c>Counters</c>
	/// </summary>
	public class TestCounters
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestCounters));

		/// <summary>Verify counter value works</summary>
		[NUnit.Framework.Test]
		public virtual void TestCounterValue()
		{
			int NumberTests = 100;
			int NumberInc = 10;
			Random rand = new Random();
			for (int i = 0; i < NumberTests; i++)
			{
				long initValue = rand.Next();
				long expectedValue = initValue;
				Counter counter = new Counters().FindCounter("test", "foo");
				counter.SetValue(initValue);
				NUnit.Framework.Assert.AreEqual("Counter value is not initialized correctly", expectedValue
					, counter.GetValue());
				for (int j = 0; j < NumberInc; j++)
				{
					int incValue = rand.Next();
					counter.Increment(incValue);
					expectedValue += incValue;
					NUnit.Framework.Assert.AreEqual("Counter value is not incremented correctly", expectedValue
						, counter.GetValue());
				}
				expectedValue = rand.Next();
				counter.SetValue(expectedValue);
				NUnit.Framework.Assert.AreEqual("Counter value is not set correctly", expectedValue
					, counter.GetValue());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestLimits()
		{
			for (int i = 0; i < 3; ++i)
			{
				// make sure limits apply to separate containers
				TestMaxCounters(new Counters());
				TestMaxGroups(new Counters());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCountersIncrement()
		{
			Counters fCounters = new Counters();
			Counter fCounter = fCounters.FindCounter(FrameworkCounter);
			fCounter.SetValue(100);
			Counter gCounter = fCounters.FindCounter("test", "foo");
			gCounter.SetValue(200);
			Counters counters = new Counters();
			counters.IncrAllCounters(fCounters);
			Counter counter;
			foreach (CounterGroup cg in fCounters)
			{
				CounterGroup group = counters.GetGroup(cg.GetName());
				if (group.GetName().Equals("test"))
				{
					counter = counters.FindCounter("test", "foo");
					NUnit.Framework.Assert.AreEqual(200, counter.GetValue());
				}
				else
				{
					counter = counters.FindCounter(FrameworkCounter);
					NUnit.Framework.Assert.AreEqual(100, counter.GetValue());
				}
			}
		}

		internal static readonly Enum<object> FrameworkCounter = TaskCounter.CpuMilliseconds;

		internal const long FrameworkCounterValue = 8;

		internal const string FsScheme = "HDFS";

		internal static readonly FileSystemCounter FsCounter = FileSystemCounter.BytesRead;

		internal const long FsCounterValue = 10;

		private void TestMaxCounters(Counters counters)
		{
			Log.Info("counters max=" + Limits.GetCountersMax());
			for (int i = 0; i < Limits.GetCountersMax(); ++i)
			{
				counters.FindCounter("test", "test" + i);
			}
			SetExpected(counters);
			ShouldThrow(typeof(LimitExceededException), new _Runnable_109(counters));
			CheckExpected(counters);
		}

		private sealed class _Runnable_109 : Runnable
		{
			public _Runnable_109(Org.Apache.Hadoop.Mapreduce.Counters counters)
			{
				this.counters = counters;
			}

			public void Run()
			{
				counters.FindCounter("test", "bad");
			}

			private readonly Org.Apache.Hadoop.Mapreduce.Counters counters;
		}

		private void TestMaxGroups(Org.Apache.Hadoop.Mapreduce.Counters counters)
		{
			Log.Info("counter groups max=" + Limits.GetGroupsMax());
			for (int i = 0; i < Limits.GetGroupsMax(); ++i)
			{
				// assuming COUNTERS_MAX > GROUPS_MAX
				counters.FindCounter("test" + i, "test");
			}
			SetExpected(counters);
			ShouldThrow(typeof(LimitExceededException), new _Runnable_124(counters));
			CheckExpected(counters);
		}

		private sealed class _Runnable_124 : Runnable
		{
			public _Runnable_124(Org.Apache.Hadoop.Mapreduce.Counters counters)
			{
				this.counters = counters;
			}

			public void Run()
			{
				counters.FindCounter("bad", "test");
			}

			private readonly Org.Apache.Hadoop.Mapreduce.Counters counters;
		}

		private void SetExpected(Org.Apache.Hadoop.Mapreduce.Counters counters)
		{
			counters.FindCounter(FrameworkCounter).SetValue(FrameworkCounterValue);
			counters.FindCounter(FsScheme, FsCounter).SetValue(FsCounterValue);
		}

		private void CheckExpected(Org.Apache.Hadoop.Mapreduce.Counters counters)
		{
			NUnit.Framework.Assert.AreEqual(FrameworkCounterValue, counters.FindCounter(FrameworkCounter
				).GetValue());
			NUnit.Framework.Assert.AreEqual(FsCounterValue, counters.FindCounter(FsScheme, FsCounter
				).GetValue());
		}

		private void ShouldThrow(Type ecls, Runnable runnable)
		{
			try
			{
				runnable.Run();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.AreSame(ecls, e.GetType());
				Log.Info("got expected: " + e);
				return;
			}
			NUnit.Framework.Assert.IsTrue("Should've thrown " + ecls.Name, false);
		}
	}
}
