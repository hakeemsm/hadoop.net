using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Metrics.Spi;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics
{
	public class TestMetricsServlet : TestCase
	{
		internal MetricsContext nc1;

		internal MetricsContext nc2;

		internal IList<MetricsContext> contexts;

		internal OutputRecord outputRecord;

		// List containing nc1 and nc2.
		/// <summary>
		/// Initializes, for testing, two NoEmitMetricsContext's, and adds one value
		/// to the first of them.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			nc1 = new NoEmitMetricsContext();
			nc1.Init("test1", ContextFactory.GetFactory());
			nc2 = new NoEmitMetricsContext();
			nc2.Init("test2", ContextFactory.GetFactory());
			contexts = new AList<MetricsContext>();
			contexts.AddItem(nc1);
			contexts.AddItem(nc2);
			MetricsRecord r = nc1.CreateRecord("testRecord");
			r.SetTag("testTag1", "testTagValue1");
			r.SetTag("testTag2", "testTagValue2");
			r.SetMetric("testMetric1", 1);
			r.SetMetric("testMetric2", 33);
			r.Update();
			IDictionary<string, ICollection<OutputRecord>> m = nc1.GetAllRecords();
			NUnit.Framework.Assert.AreEqual(1, m.Count);
			NUnit.Framework.Assert.AreEqual(1, m.Values.Count);
			ICollection<OutputRecord> outputRecords = m.Values.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(1, outputRecords.Count);
			outputRecord = outputRecords.GetEnumerator().Next();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTagsMetricsPair()
		{
			MetricsServlet.TagsMetricsPair pair = new MetricsServlet.TagsMetricsPair(outputRecord
				.GetTagsCopy(), outputRecord.GetMetricsCopy());
			string s = JSON.ToString(pair);
			NUnit.Framework.Assert.AreEqual("[{\"testTag1\":\"testTagValue1\",\"testTag2\":\"testTagValue2\"},"
				 + "{\"testMetric1\":1,\"testMetric2\":33}]", s);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetMap()
		{
			MetricsServlet servlet = new MetricsServlet();
			IDictionary<string, IDictionary<string, IList<MetricsServlet.TagsMetricsPair>>> m
				 = servlet.MakeMap(contexts);
			NUnit.Framework.Assert.AreEqual("Map missing contexts", 2, m.Count);
			NUnit.Framework.Assert.IsTrue(m.Contains("test1"));
			IDictionary<string, IList<MetricsServlet.TagsMetricsPair>> m2 = m["test1"];
			NUnit.Framework.Assert.AreEqual("Missing records", 1, m2.Count);
			NUnit.Framework.Assert.IsTrue(m2.Contains("testRecord"));
			NUnit.Framework.Assert.AreEqual("Wrong number of tags-values pairs.", 1, m2["testRecord"
				].Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPrintMap()
		{
			StringWriter sw = new StringWriter();
			PrintWriter @out = new PrintWriter(sw);
			MetricsServlet servlet = new MetricsServlet();
			servlet.PrintMap(@out, servlet.MakeMap(contexts));
			string Expected = string.Empty + "test1\n" + "  testRecord\n" + "    {testTag1=testTagValue1,testTag2=testTagValue2}:\n"
				 + "      testMetric1=1\n" + "      testMetric2=33\n" + "test2\n";
			NUnit.Framework.Assert.AreEqual(Expected, sw.ToString());
		}
	}
}
