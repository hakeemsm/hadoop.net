using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestStatisticsCollector : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestMovingWindow()
		{
			StatisticsCollector collector = new StatisticsCollector(1);
			StatisticsCollector.TimeWindow window = new StatisticsCollector.TimeWindow("test"
				, 6, 2);
			StatisticsCollector.TimeWindow sincStart = StatisticsCollector.SinceStart;
			StatisticsCollector.TimeWindow[] windows = new StatisticsCollector.TimeWindow[] { 
				sincStart, window };
			StatisticsCollector.Stat stat = collector.CreateStat("m1", windows);
			stat.Inc(3);
			collector.Update();
			NUnit.Framework.Assert.AreEqual(0, stat.GetValues()[window].GetValue());
			NUnit.Framework.Assert.AreEqual(3, stat.GetValues()[sincStart].GetValue());
			stat.Inc(3);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((3 + 3), stat.GetValues()[window].GetValue());
			NUnit.Framework.Assert.AreEqual(6, stat.GetValues()[sincStart].GetValue());
			stat.Inc(10);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((3 + 3), stat.GetValues()[window].GetValue());
			NUnit.Framework.Assert.AreEqual(16, stat.GetValues()[sincStart].GetValue());
			stat.Inc(10);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((3 + 3 + 10 + 10), stat.GetValues()[window].GetValue
				());
			NUnit.Framework.Assert.AreEqual(26, stat.GetValues()[sincStart].GetValue());
			stat.Inc(10);
			collector.Update();
			stat.Inc(10);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((3 + 3 + 10 + 10 + 10 + 10), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(46, stat.GetValues()[sincStart].GetValue());
			stat.Inc(10);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((3 + 3 + 10 + 10 + 10 + 10), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(56, stat.GetValues()[sincStart].GetValue());
			stat.Inc(12);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((10 + 10 + 10 + 10 + 10 + 12), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(68, stat.GetValues()[sincStart].GetValue());
			stat.Inc(13);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((10 + 10 + 10 + 10 + 10 + 12), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(81, stat.GetValues()[sincStart].GetValue());
			stat.Inc(14);
			collector.Update();
			NUnit.Framework.Assert.AreEqual((10 + 10 + 10 + 12 + 13 + 14), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(95, stat.GetValues()[sincStart].GetValue());
			//  test Stat class 
			IDictionary updaters = collector.GetUpdaters();
			NUnit.Framework.Assert.AreEqual(updaters.Count, 2);
			IDictionary<string, StatisticsCollector.Stat> ststistics = collector.GetStatistics
				();
			NUnit.Framework.Assert.IsNotNull(ststistics["m1"]);
			StatisticsCollector.Stat newStat = collector.CreateStat("m2");
			NUnit.Framework.Assert.AreEqual(newStat.name, "m2");
			StatisticsCollector.Stat st = collector.RemoveStat("m1");
			NUnit.Framework.Assert.AreEqual(st.name, "m1");
			NUnit.Framework.Assert.AreEqual((10 + 10 + 10 + 12 + 13 + 14), stat.GetValues()[window
				].GetValue());
			NUnit.Framework.Assert.AreEqual(95, stat.GetValues()[sincStart].GetValue());
			st = collector.RemoveStat("m1");
			// try to remove stat again
			NUnit.Framework.Assert.IsNull(st);
			collector.Start();
			// waiting 2,5 sec
			Sharpen.Thread.Sleep(2500);
			NUnit.Framework.Assert.AreEqual(69, stat.GetValues()[window].GetValue());
			NUnit.Framework.Assert.AreEqual(95, stat.GetValues()[sincStart].GetValue());
		}
	}
}
