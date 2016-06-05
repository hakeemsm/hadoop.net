/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;


namespace Org.Apache.Hadoop.Metrics2.Util
{
	public class TestMetricsCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMetricsCache));

		[Fact]
		public virtual void TestUpdate()
		{
			MetricsCache cache = new MetricsCache();
			MetricsRecord mr = MakeRecord("r", Arrays.AsList(MakeTag("t", "tv")), Arrays.AsList
				(MakeMetric("m", 0), MakeMetric("m1", 1)));
			MetricsCache.Record cr = cache.Update(mr);
			Org.Mockito.Mockito.Verify(mr).Name();
			Org.Mockito.Mockito.Verify(mr).Tags();
			Org.Mockito.Mockito.Verify(mr).Metrics();
			Assert.Equal("same record size", cr.Metrics().Count, ((ICollection
				<AbstractMetric>)mr.Metrics()).Count);
			Assert.Equal("same metric value", 0, cr.GetMetric("m"));
			MetricsRecord mr2 = MakeRecord("r", Arrays.AsList(MakeTag("t", "tv")), Arrays.AsList
				(MakeMetric("m", 2), MakeMetric("m2", 42)));
			cr = cache.Update(mr2);
			Assert.Equal("contains 3 metric", 3, cr.Metrics().Count);
			CheckMetricValue("updated metric value", cr, "m", 2);
			CheckMetricValue("old metric value", cr, "m1", 1);
			CheckMetricValue("new metric value", cr, "m2", 42);
			MetricsRecord mr3 = MakeRecord("r", Arrays.AsList(MakeTag("t", "tv3")), Arrays.AsList
				(MakeMetric("m3", 3)));
			// different tag value
			cr = cache.Update(mr3);
			// should get a new record
			Assert.Equal("contains 1 metric", 1, cr.Metrics().Count);
			CheckMetricValue("updated metric value", cr, "m3", 3);
			// tags cache should be empty so far
			Assert.Equal("no tags", 0, cr.Tags().Count);
			// until now
			cr = cache.Update(mr3, true);
			Assert.Equal("Got 1 tag", 1, cr.Tags().Count);
			Assert.Equal("Tag value", "tv3", cr.GetTag("t"));
			CheckMetricValue("Metric value", cr, "m3", 3);
		}

		[Fact]
		public virtual void TestGet()
		{
			MetricsCache cache = new MetricsCache();
			NUnit.Framework.Assert.IsNull("empty", cache.Get("r", Arrays.AsList(MakeTag("t", 
				"t"))));
			MetricsRecord mr = MakeRecord("r", Arrays.AsList(MakeTag("t", "t")), Arrays.AsList
				(MakeMetric("m", 1)));
			cache.Update(mr);
			MetricsCache.Record cr = cache.Get("r", mr.Tags());
			Log.Debug("tags=" + mr.Tags() + " cr=" + cr);
			NUnit.Framework.Assert.IsNotNull("Got record", cr);
			Assert.Equal("contains 1 metric", 1, cr.Metrics().Count);
			CheckMetricValue("new metric value", cr, "m", 1);
		}

		/// <summary>Make sure metrics tag has a sane hashCode impl</summary>
		[Fact]
		public virtual void TestNullTag()
		{
			MetricsCache cache = new MetricsCache();
			MetricsRecord mr = MakeRecord("r", Arrays.AsList(MakeTag("t", null)), Arrays.AsList
				(MakeMetric("m", 0), MakeMetric("m1", 1)));
			MetricsCache.Record cr = cache.Update(mr);
			Assert.True("t value should be null", null == cr.GetTag("t"));
		}

		[Fact]
		public virtual void TestOverflow()
		{
			MetricsCache cache = new MetricsCache();
			MetricsCache.Record cr;
			ICollection<MetricsTag> t0 = Arrays.AsList(MakeTag("t0", "0"));
			for (int i = 0; i < MetricsCache.MaxRecsPerNameDefault + 1; ++i)
			{
				cr = cache.Update(MakeRecord("r", Arrays.AsList(MakeTag("t" + i, string.Empty + i
					)), Arrays.AsList(MakeMetric("m", i))));
				CheckMetricValue("new metric value", cr, "m", i);
				if (i < MetricsCache.MaxRecsPerNameDefault)
				{
					NUnit.Framework.Assert.IsNotNull("t0 is still there", cache.Get("r", t0));
				}
			}
			NUnit.Framework.Assert.IsNull("t0 is gone", cache.Get("r", t0));
		}

		private void CheckMetricValue(string description, MetricsCache.Record cr, string 
			key, Number val)
		{
			Assert.Equal(description, val, cr.GetMetric(key));
			NUnit.Framework.Assert.IsNotNull("metric not null", cr.GetMetricInstance(key));
			Assert.Equal(description, val, cr.GetMetricInstance(key).Value
				());
		}

		private MetricsRecord MakeRecord(string name, ICollection<MetricsTag> tags, ICollection
			<AbstractMetric> metrics)
		{
			MetricsRecord mr = Org.Mockito.Mockito.Mock<MetricsRecord>();
			Org.Mockito.Mockito.When(mr.Name()).ThenReturn(name);
			Org.Mockito.Mockito.When(mr.Tags()).ThenReturn(tags);
			Org.Mockito.Mockito.When(mr.Metrics()).ThenReturn(metrics);
			return mr;
		}

		private MetricsTag MakeTag(string name, string value)
		{
			return new MetricsTag(Interns.Info(name, string.Empty), value);
		}

		private AbstractMetric MakeMetric(string name, Number value)
		{
			AbstractMetric metric = Org.Mockito.Mockito.Mock<AbstractMetric>();
			Org.Mockito.Mockito.When(metric.Name()).ThenReturn(name);
			Org.Mockito.Mockito.When(metric.Value()).ThenReturn(value);
			return metric;
		}
	}
}
