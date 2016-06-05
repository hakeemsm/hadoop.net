using Org.Apache.Commons.Configuration;
using Org.Apache.Hadoop.Metrics2.Filter;
using Org.Apache.Hadoop.Metrics2.Lib;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class TestMetricsCollectorImpl
	{
		[Fact]
		public virtual void RecordBuilderShouldNoOpIfFiltered()
		{
			SubsetConfiguration fc = new ConfigBuilder().Add("p.exclude", "foo").Subset("p");
			MetricsCollectorImpl mb = new MetricsCollectorImpl();
			mb.SetRecordFilter(TestPatternFilter.NewGlobFilter(fc));
			MetricsRecordBuilderImpl rb = mb.AddRecord("foo");
			((MetricsRecordBuilderImpl)rb.Tag(Interns.Info("foo", string.Empty), "value")).AddGauge
				(Interns.Info("g0", string.Empty), 1);
			Assert.Equal("no tags", 0, rb.Tags().Count);
			Assert.Equal("no metrics", 0, rb.Metrics().Count);
			NUnit.Framework.Assert.IsNull("null record", rb.GetRecord());
			Assert.Equal("no records", 0, mb.GetRecords().Count);
		}

		[Fact]
		public virtual void TestPerMetricFiltering()
		{
			SubsetConfiguration fc = new ConfigBuilder().Add("p.exclude", "foo").Subset("p");
			MetricsCollectorImpl mb = new MetricsCollectorImpl();
			mb.SetMetricFilter(TestPatternFilter.NewGlobFilter(fc));
			MetricsRecordBuilderImpl rb = mb.AddRecord("foo");
			((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl)rb.Tag(Interns.Info("foo", 
				string.Empty), string.Empty)).AddCounter(Interns.Info("c0", string.Empty), 0)).AddGauge
				(Interns.Info("foo", string.Empty), 1);
			Assert.Equal("1 tag", 1, rb.Tags().Count);
			Assert.Equal("1 metric", 1, rb.Metrics().Count);
			Assert.Equal("expect foo tag", "foo", rb.Tags()[0].Name());
			Assert.Equal("expect c0", "c0", rb.Metrics()[0].Name());
		}
	}
}
