using System.Collections.Generic;
using Javax.Management;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class TestMetricsSourceAdapter
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPurgeOldMetrics()
		{
			// create test source with a single metric counter of value 1
			TestMetricsSourceAdapter.PurgableSource source = new TestMetricsSourceAdapter.PurgableSource
				(this);
			MetricsSourceBuilder sb = MetricsAnnotations.NewSourceBuilder(source);
			MetricsSource s = sb.Build();
			IList<MetricsTag> injectedTags = new AList<MetricsTag>();
			MetricsSourceAdapter sa = new MetricsSourceAdapter("tst", "tst", "testdesc", s, injectedTags
				, null, null, 1, false);
			MBeanInfo info = sa.GetMBeanInfo();
			bool sawIt = false;
			foreach (MBeanAttributeInfo mBeanAttributeInfo in info.GetAttributes())
			{
				sawIt |= mBeanAttributeInfo.GetName().Equals(source.lastKeyName);
			}
			Assert.True("The last generated metric is not exported to jmx", 
				sawIt);
			Sharpen.Thread.Sleep(1000);
			// skip JMX cache TTL
			info = sa.GetMBeanInfo();
			sawIt = false;
			foreach (MBeanAttributeInfo mBeanAttributeInfo_1 in info.GetAttributes())
			{
				sawIt |= mBeanAttributeInfo_1.GetName().Equals(source.lastKeyName);
			}
			Assert.True("The last generated metric is not exported to jmx", 
				sawIt);
		}

		internal class PurgableSource : MetricsSource
		{
			internal int nextKey = 0;

			internal string lastKeyName = null;

			//generate a new key per each call
			public virtual void GetMetrics(MetricsCollector collector, bool all)
			{
				MetricsRecordBuilder rb = collector.AddRecord("purgablesource").SetContext("test"
					);
				this.lastKeyName = "key" + this.nextKey++;
				rb.AddGauge(Interns.Info(this.lastKeyName, "desc"), 1);
			}

			internal PurgableSource(TestMetricsSourceAdapter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMetricsSourceAdapter _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetMetricsAndJmx()
		{
			// create test source with a single metric counter of value 0
			TestMetricsSourceAdapter.TestSource source = new TestMetricsSourceAdapter.TestSource
				("test");
			MetricsSourceBuilder sb = MetricsAnnotations.NewSourceBuilder(source);
			MetricsSource s = sb.Build();
			IList<MetricsTag> injectedTags = new AList<MetricsTag>();
			MetricsSourceAdapter sa = new MetricsSourceAdapter("test", "test", "test desc", s
				, injectedTags, null, null, 1, false);
			// all metrics are initially assumed to have changed
			MetricsCollectorImpl builder = new MetricsCollectorImpl();
			IEnumerable<MetricsRecordImpl> metricsRecords = sa.GetMetrics(builder, true);
			// Validate getMetrics and JMX initial values
			MetricsRecordImpl metricsRecord = metricsRecords.GetEnumerator().Next();
			Assert.Equal(0L, metricsRecord.Metrics().GetEnumerator().Next(
				).Value());
			Sharpen.Thread.Sleep(100);
			// skip JMX cache TTL
			Assert.Equal(0L, (Number)sa.GetAttribute("C1"));
			// change metric value
			source.IncrementCnt();
			// validate getMetrics and JMX
			builder = new MetricsCollectorImpl();
			metricsRecords = sa.GetMetrics(builder, true);
			metricsRecord = metricsRecords.GetEnumerator().Next();
			Assert.True(metricsRecord.Metrics().GetEnumerator().HasNext());
			Sharpen.Thread.Sleep(100);
			// skip JMX cache TTL
			Assert.Equal(1L, (Number)sa.GetAttribute("C1"));
		}

		private class TestSource
		{
			internal MutableCounterLong c1;

			internal readonly MetricsRegistry registry;

			internal TestSource(string recName)
			{
				registry = new MetricsRegistry(recName);
			}

			public virtual void IncrementCnt()
			{
				c1.Incr();
			}
		}
	}
}
