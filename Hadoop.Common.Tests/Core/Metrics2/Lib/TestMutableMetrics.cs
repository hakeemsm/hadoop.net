using System.Collections.Generic;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Test;
using Org.Mockito;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Test metrics record builder interface and mutable metrics</summary>
	public class TestMutableMetrics
	{
		private readonly double Epsilon = 1e-42;

		/// <summary>Test the snapshot method</summary>
		[Fact]
		public virtual void TestSnapshot()
		{
			MetricsRecordBuilder mb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry registry = new MetricsRegistry("test");
			registry.NewCounter("c1", "int counter", 1);
			registry.NewCounter("c2", "long counter", 2L);
			registry.NewGauge("g1", "int gauge", 3);
			registry.NewGauge("g2", "long gauge", 4L);
			registry.NewStat("s1", "stat", "Ops", "Time", true).Add(0);
			registry.NewRate("s2", "stat", false).Add(0);
			registry.Snapshot(mb, true);
			MutableStat s2 = (MutableStat)registry.Get("s2");
			s2.Snapshot(mb, true);
			// should get the same back.
			s2.Add(1);
			s2.Snapshot(mb, true);
			// should get new interval values back
			Org.Mockito.Mockito.Verify(mb).AddCounter(Interns.Info("c1", "int counter"), 1);
			Org.Mockito.Mockito.Verify(mb).AddCounter(Interns.Info("c2", "long counter"), 2L);
			Org.Mockito.Mockito.Verify(mb).AddGauge(Interns.Info("g1", "int gauge"), 3);
			Org.Mockito.Mockito.Verify(mb).AddGauge(Interns.Info("g2", "long gauge"), 4L);
			Org.Mockito.Mockito.Verify(mb).AddCounter(Interns.Info("S1NumOps", "Number of ops for stat"
				), 1L);
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1AvgTime", "Average time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1StdevTime", "Standard deviation of time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1IMinTime", "Interval min time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1IMaxTime", "Interval max time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1MinTime", "Min time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S1MaxTime", "Max time for stat"
				)), AdditionalMatchers.Eq(0.0, Epsilon));
			Org.Mockito.Mockito.Verify(mb, Org.Mockito.Mockito.Times(2)).AddCounter(Interns.Info
				("S2NumOps", "Number of ops for stat"), 1L);
			Org.Mockito.Mockito.Verify(mb, Org.Mockito.Mockito.Times(2)).AddGauge(Matchers.Eq
				(Interns.Info("S2AvgTime", "Average time for stat")), AdditionalMatchers.Eq(0.0, 
				Epsilon));
			Org.Mockito.Mockito.Verify(mb).AddCounter(Interns.Info("S2NumOps", "Number of ops for stat"
				), 2L);
			Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info("S2AvgTime", "Average time for stat"
				)), AdditionalMatchers.Eq(1.0, Epsilon));
		}

		internal interface TestProtocol
		{
			void Foo();

			void Bar();
		}

		[Fact]
		public virtual void TestMutableRates()
		{
			MetricsRecordBuilder rb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry registry = new MetricsRegistry("test");
			MutableRates rates = new MutableRates(registry);
			rates.Init(typeof(TestMutableMetrics.TestProtocol));
			registry.Snapshot(rb, false);
			MetricsAsserts.AssertCounter("FooNumOps", 0L, rb);
			MetricsAsserts.AssertGauge("FooAvgTime", 0.0, rb);
			MetricsAsserts.AssertCounter("BarNumOps", 0L, rb);
			MetricsAsserts.AssertGauge("BarAvgTime", 0.0, rb);
		}

		/// <summary>
		/// Ensure that quantile estimates from
		/// <see cref="MutableQuantiles"/>
		/// are within
		/// specified error bounds.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMutableQuantilesError()
		{
			MetricsRecordBuilder mb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry registry = new MetricsRegistry("test");
			// Use a 5s rollover period
			MutableQuantiles quantiles = registry.NewQuantiles("foo", "stat", "Ops", "Latency"
				, 5);
			// Push some values in and wait for it to publish
			long start = Runtime.NanoTime() / 1000000;
			for (long i = 1; i <= 1000; i++)
			{
				quantiles.Add(i);
				quantiles.Add(1001 - i);
			}
			long end = Runtime.NanoTime() / 1000000;
			Thread.Sleep(6000 - (end - start));
			registry.Snapshot(mb, false);
			// Print out the snapshot
			IDictionary<Quantile, long> previousSnapshot = quantiles.previousSnapshot;
			foreach (KeyValuePair<Quantile, long> item in previousSnapshot)
			{
				System.Console.Out.WriteLine(string.Format("Quantile %.2f has value %d", item.Key
					.quantile, item.Value));
			}
			// Verify the results are within our requirements
			Org.Mockito.Mockito.Verify(mb).AddGauge(Interns.Info("FooNumOps", "Number of ops for stat with 5s interval"
				), (long)2000);
			Quantile[] quants = MutableQuantiles.quantiles;
			string name = "Foo%dthPercentileLatency";
			string desc = "%d percentile latency with 5 second interval for stat";
			foreach (Quantile q in quants)
			{
				int percentile = (int)(100 * q.quantile);
				int error = (int)(1000 * q.error);
				string n = string.Format(name, percentile);
				string d = string.Format(desc, percentile);
				long expected = (long)(q.quantile * 1000);
				Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info(n, d)), AdditionalMatchers.Leq
					(expected + error));
				Org.Mockito.Mockito.Verify(mb).AddGauge(Matchers.Eq(Interns.Info(n, d)), AdditionalMatchers.Geq
					(expected - error));
			}
		}

		/// <summary>
		/// Test that
		/// <see cref="MutableQuantiles"/>
		/// rolls the window over at the specified
		/// interval.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMutableQuantilesRollover()
		{
			MetricsRecordBuilder mb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry registry = new MetricsRegistry("test");
			// Use a 5s rollover period
			MutableQuantiles quantiles = registry.NewQuantiles("foo", "stat", "Ops", "Latency"
				, 5);
			Quantile[] quants = MutableQuantiles.quantiles;
			string name = "Foo%dthPercentileLatency";
			string desc = "%d percentile latency with 5 second interval for stat";
			// Push values for three intervals
			long start = Runtime.NanoTime() / 1000000;
			for (int i = 1; i <= 3; i++)
			{
				// Insert the values
				for (long j = 1; j <= 1000; j++)
				{
					quantiles.Add(i);
				}
				// Sleep until 1s after the next 5s interval, to let the metrics
				// roll over
				long sleep = (start + (5000 * i) + 1000) - (Runtime.NanoTime() / 1000000);
				Thread.Sleep(sleep);
				// Verify that the window reset, check it has the values we pushed in
				registry.Snapshot(mb, false);
				foreach (Quantile q in quants)
				{
					int percentile = (int)(100 * q.quantile);
					string n = string.Format(name, percentile);
					string d = string.Format(desc, percentile);
					Org.Mockito.Mockito.Verify(mb).AddGauge(Interns.Info(n, d), (long)i);
				}
			}
			// Verify the metrics were added the right number of times
			Org.Mockito.Mockito.Verify(mb, Org.Mockito.Mockito.Times(3)).AddGauge(Interns.Info
				("FooNumOps", "Number of ops for stat with 5s interval"), (long)1000);
			foreach (Quantile q_1 in quants)
			{
				int percentile = (int)(100 * q_1.quantile);
				string n = string.Format(name, percentile);
				string d = string.Format(desc, percentile);
				Org.Mockito.Mockito.Verify(mb, Org.Mockito.Mockito.Times(3)).AddGauge(Matchers.Eq
					(Interns.Info(n, d)), Matchers.AnyLong());
			}
		}

		/// <summary>
		/// Test that
		/// <see cref="MutableQuantiles"/>
		/// rolls over correctly even if no items
		/// have been added to the window
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestMutableQuantilesEmptyRollover()
		{
			MetricsRecordBuilder mb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry registry = new MetricsRegistry("test");
			// Use a 5s rollover period
			MutableQuantiles quantiles = registry.NewQuantiles("foo", "stat", "Ops", "Latency"
				, 5);
			// Check it initially
			quantiles.Snapshot(mb, true);
			Org.Mockito.Mockito.Verify(mb).AddGauge(Interns.Info("FooNumOps", "Number of ops for stat with 5s interval"
				), (long)0);
			Thread.Sleep(6000);
			quantiles.Snapshot(mb, false);
			Org.Mockito.Mockito.Verify(mb, Org.Mockito.Mockito.Times(2)).AddGauge(Interns.Info
				("FooNumOps", "Number of ops for stat with 5s interval"), (long)0);
		}
	}
}
