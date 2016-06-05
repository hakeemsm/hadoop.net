/*
* OutputRecord.java
*
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


namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>Represents a record of metric data to be sent to a metrics system.</summary>
	public class OutputRecord
	{
		private AbstractMetricsContext.TagMap tagMap;

		private AbstractMetricsContext.MetricMap metricMap;

		/// <summary>Creates a new instance of OutputRecord</summary>
		internal OutputRecord(AbstractMetricsContext.TagMap tagMap, AbstractMetricsContext.MetricMap
			 metricMap)
		{
			this.tagMap = tagMap;
			this.metricMap = metricMap;
		}

		/// <summary>Returns the set of tag names</summary>
		public virtual ICollection<string> GetTagNames()
		{
			return Collections.UnmodifiableSet(tagMap.Keys);
		}

		/// <summary>Returns a tag object which is can be a String, Integer, Short or Byte.</summary>
		/// <returns>the tag value, or null if there is no such tag</returns>
		public virtual object GetTag(string name)
		{
			return tagMap[name];
		}

		/// <summary>Returns the set of metric names.</summary>
		public virtual ICollection<string> GetMetricNames()
		{
			return Collections.UnmodifiableSet(metricMap.Keys);
		}

		/// <summary>Returns the metric object which can be a Float, Integer, Short or Byte.</summary>
		public virtual Number GetMetric(string name)
		{
			return metricMap[name];
		}

		/// <summary>Returns a copy of this record's tags.</summary>
		public virtual AbstractMetricsContext.TagMap GetTagsCopy()
		{
			return new AbstractMetricsContext.TagMap(tagMap);
		}

		/// <summary>Returns a copy of this record's metrics.</summary>
		public virtual AbstractMetricsContext.MetricMap GetMetricsCopy()
		{
			return new AbstractMetricsContext.MetricMap(metricMap);
		}
	}
}
