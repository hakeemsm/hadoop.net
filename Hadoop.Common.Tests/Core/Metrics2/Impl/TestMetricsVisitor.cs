using System.Collections.Generic;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Test the metric visitor interface</summary>
	public class TestMetricsVisitor
	{
		[Captor]
		private ArgumentCaptor<MetricsInfo> c1;

		[Captor]
		private ArgumentCaptor<MetricsInfo> c2;

		[Captor]
		private ArgumentCaptor<MetricsInfo> g1;

		[Captor]
		private ArgumentCaptor<MetricsInfo> g2;

		[Captor]
		private ArgumentCaptor<MetricsInfo> g3;

		[Captor]
		private ArgumentCaptor<MetricsInfo> g4;

		/// <summary>Test the common use cases</summary>
		[NUnit.Framework.Test]
		public virtual void TestCommon()
		{
			MetricsVisitor visitor = Org.Mockito.Mockito.Mock<MetricsVisitor>();
			MetricsRegistry registry = new MetricsRegistry("test");
			IList<AbstractMetric> metrics = ((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl
				)((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl)((MetricsRecordBuilderImpl
				)((MetricsRecordBuilderImpl)MetricsLists.Builder("test").AddCounter(Interns.Info
				("c1", "int counter"), 1)).AddCounter(Interns.Info("c2", "long counter"), 2L)).AddGauge
				(Interns.Info("g1", "int gauge"), 5)).AddGauge(Interns.Info("g2", "long gauge"), 
				6L)).AddGauge(Interns.Info("g3", "float gauge"), 7f)).AddGauge(Interns.Info("g4"
				, "double gauge"), 8d)).Metrics();
			foreach (AbstractMetric metric in metrics)
			{
				metric.Visit(visitor);
			}
			Org.Mockito.Mockito.Verify(visitor).Counter(c1.Capture(), Eq(1));
			NUnit.Framework.Assert.AreEqual("c1 name", "c1", c1.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("c1 description", "int counter", c1.GetValue().Description
				());
			Org.Mockito.Mockito.Verify(visitor).Counter(c2.Capture(), Eq(2L));
			NUnit.Framework.Assert.AreEqual("c2 name", "c2", c2.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("c2 description", "long counter", c2.GetValue().Description
				());
			Org.Mockito.Mockito.Verify(visitor).Gauge(g1.Capture(), Eq(5));
			NUnit.Framework.Assert.AreEqual("g1 name", "g1", g1.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("g1 description", "int gauge", g1.GetValue().Description
				());
			Org.Mockito.Mockito.Verify(visitor).Gauge(g2.Capture(), Eq(6L));
			NUnit.Framework.Assert.AreEqual("g2 name", "g2", g2.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("g2 description", "long gauge", g2.GetValue().Description
				());
			Org.Mockito.Mockito.Verify(visitor).Gauge(g3.Capture(), Eq(7f));
			NUnit.Framework.Assert.AreEqual("g3 name", "g3", g3.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("g3 description", "float gauge", g3.GetValue().Description
				());
			Org.Mockito.Mockito.Verify(visitor).Gauge(g4.Capture(), Eq(8d));
			NUnit.Framework.Assert.AreEqual("g4 name", "g4", g4.GetValue().Name());
			NUnit.Framework.Assert.AreEqual("g4 description", "double gauge", g4.GetValue().Description
				());
		}
	}
}
