using NUnit.Framework;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Test the metric registry class</summary>
	public class TestMetricsRegistry
	{
		/// <summary>Test various factory methods</summary>
		[NUnit.Framework.Test]
		public virtual void TestNewMetrics()
		{
			MetricsRegistry r = new MetricsRegistry("test");
			r.NewCounter("c1", "c1 desc", 1);
			r.NewCounter("c2", "c2 desc", 2L);
			r.NewGauge("g1", "g1 desc", 3);
			r.NewGauge("g2", "g2 desc", 4L);
			r.NewStat("s1", "s1 desc", "ops", "time");
			NUnit.Framework.Assert.AreEqual("num metrics in registry", 5, r.Metrics().Count);
			NUnit.Framework.Assert.IsTrue("c1 found", r.Get("c1") is MutableCounterInt);
			NUnit.Framework.Assert.IsTrue("c2 found", r.Get("c2") is MutableCounterLong);
			NUnit.Framework.Assert.IsTrue("g1 found", r.Get("g1") is MutableGaugeInt);
			NUnit.Framework.Assert.IsTrue("g2 found", r.Get("g2") is MutableGaugeLong);
			NUnit.Framework.Assert.IsTrue("s1 found", r.Get("s1") is MutableStat);
			ExpectMetricsException("Metric name c1 already exists", new _Runnable_54(r));
		}

		private sealed class _Runnable_54 : Runnable
		{
			public _Runnable_54(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("c1", "test dup", 0);
			}

			private readonly MetricsRegistry r;
		}

		/// <summary>Test adding metrics with whitespace in the name</summary>
		[NUnit.Framework.Test]
		public virtual void TestMetricsRegistryIllegalMetricNames()
		{
			MetricsRegistry r = new MetricsRegistry("test");
			// Fill up with some basics
			r.NewCounter("c1", "c1 desc", 1);
			r.NewGauge("g1", "g1 desc", 1);
			r.NewQuantiles("q1", "q1 desc", "q1 name", "q1 val type", 1);
			// Add some illegal names
			ExpectMetricsException("Metric name 'badcount 2' contains " + "illegal whitespace character"
				, new _Runnable_72(r));
			ExpectMetricsException("Metric name 'badcount3  ' contains " + "illegal whitespace character"
				, new _Runnable_77(r));
			ExpectMetricsException("Metric name '  badcount4' contains " + "illegal whitespace character"
				, new _Runnable_82(r));
			ExpectMetricsException("Metric name 'withtab5	' contains " + "illegal whitespace character"
				, new _Runnable_87(r));
			ExpectMetricsException("Metric name 'withnewline6\n' contains " + "illegal whitespace character"
				, new _Runnable_92(r));
			// Final validation
			NUnit.Framework.Assert.AreEqual("num metrics in registry", 3, r.Metrics().Count);
		}

		private sealed class _Runnable_72 : Runnable
		{
			public _Runnable_72(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("badcount 2", "c2 desc", 2);
			}

			private readonly MetricsRegistry r;
		}

		private sealed class _Runnable_77 : Runnable
		{
			public _Runnable_77(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("badcount3  ", "c3 desc", 3);
			}

			private readonly MetricsRegistry r;
		}

		private sealed class _Runnable_82 : Runnable
		{
			public _Runnable_82(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("  badcount4", "c4 desc", 4);
			}

			private readonly MetricsRegistry r;
		}

		private sealed class _Runnable_87 : Runnable
		{
			public _Runnable_87(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("withtab5	", "c5 desc", 5);
			}

			private readonly MetricsRegistry r;
		}

		private sealed class _Runnable_92 : Runnable
		{
			public _Runnable_92(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.NewCounter("withnewline6\n", "c6 desc", 6);
			}

			private readonly MetricsRegistry r;
		}

		/// <summary>Test the add by name method</summary>
		[NUnit.Framework.Test]
		public virtual void TestAddByName()
		{
			MetricsRecordBuilder rb = MetricsAsserts.MockMetricsRecordBuilder();
			MetricsRegistry r = new MetricsRegistry("test");
			r.Add("s1", 42);
			r.Get("s1").Snapshot(rb);
			Org.Mockito.Mockito.Verify(rb).AddCounter(Interns.Info("S1NumOps", "Number of ops for s1"
				), 1L);
			Org.Mockito.Mockito.Verify(rb).AddGauge(Interns.Info("S1AvgTime", "Average time for s1"
				), 42.0);
			r.NewCounter("c1", "test add", 1);
			r.NewGauge("g1", "test add", 1);
			ExpectMetricsException("Unsupported add", new _Runnable_114(r));
			ExpectMetricsException("Unsupported add", new _Runnable_119(r));
		}

		private sealed class _Runnable_114 : Runnable
		{
			public _Runnable_114(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.Add("c1", 42);
			}

			private readonly MetricsRegistry r;
		}

		private sealed class _Runnable_119 : Runnable
		{
			public _Runnable_119(MetricsRegistry r)
			{
				this.r = r;
			}

			public void Run()
			{
				r.Add("g1", 42);
			}

			private readonly MetricsRegistry r;
		}

		[Ignore]
		private void ExpectMetricsException(string prefix, Runnable fun)
		{
			try
			{
				fun.Run();
			}
			catch (MetricsException e)
			{
				NUnit.Framework.Assert.IsTrue("expected exception", e.Message.StartsWith(prefix));
				return;
			}
			NUnit.Framework.Assert.Fail("should've thrown '" + prefix + "...'");
		}
	}
}
