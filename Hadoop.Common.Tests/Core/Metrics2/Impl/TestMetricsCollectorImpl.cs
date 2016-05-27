using Org.Apache.Commons.Configuration;
using Org.Apache.Hadoop.Metrics2.Filter;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class TestMetricsCollectorImpl
	{
		[NUnit.Framework.Test]
		public virtual void RecordBuilderShouldNoOpIfFiltered()
		{
			SubsetConfiguration fc = new ConfigBuilder().Add("p.exclude", "foo").Subset("p");
			MetricsCollectorImpl mb = new MetricsCollectorImpl();
			mb.SetRecordFilter(TestPatternFilter.NewGlobFilter(fc));
			MetricsRecordBuilderImpl rb = mb.AddRecord("foo");
			((MetricsRecordBuilderImpl)rb.Tag(Interns.Info("foo", string.Empty), "value")).AddGauge
				(Interns.Info("g0", string.Empty), 1);
			NUnit.Framework.Assert.AreEqual("no tags", 0, rb.Tags().Count);
			NUnit.Framework.Assert.AreEqual("no metrics", 0, rb.Metrics().Count);
			NUnit.Framework.Assert.IsNull("null record", rb.GetRecord());
			NUnit.Framework.Assert.AreEqual("no records", 0, mb.GetRecords().Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestPerMetricFiltering()
		{
			SubsetConfiguration fc = new ConfigBuilder().Add("p.exclude", "foo").Subset("p");
			MetricsCollectorImpl mb = new MetricsCollectorImpl();
			mb.SetMetricFilter(TestPatternFilter.NewGlobFilter(fc));
			MetricsRecordBuilderImpl rb = mb.AddRecord("foo");
			((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl)rb.Tag(Interns.Info("foo", 
				string.Empty), string.Empty)).AddCounter(Interns.Info("c0", string.Empty), 0)).AddGauge
				(Interns.Info("foo", string.Empty), 1);
			NUnit.Framework.Assert.AreEqual("1 tag", 1, rb.Tags().Count);
			NUnit.Framework.Assert.AreEqual("1 metric", 1, rb.Metrics().Count);
			NUnit.Framework.Assert.AreEqual("expect foo tag", "foo", rb.Tags()[0].Name());
			NUnit.Framework.Assert.AreEqual("expect c0", "c0", rb.Metrics()[0].Name());
		}
	}
}
