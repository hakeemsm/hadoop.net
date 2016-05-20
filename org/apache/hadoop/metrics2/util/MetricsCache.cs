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
using Sharpen;

namespace org.apache.hadoop.metrics2.util
{
	/// <summary>A metrics cache for sinks that don't support sparse updates.</summary>
	public class MetricsCache
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.util.MetricsCache
			)));

		internal const int MAX_RECS_PER_NAME_DEFAULT = 1000;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.util.MetricsCache.RecordCache
			> map = com.google.common.collect.Maps.newHashMap();

		private readonly int maxRecsPerName;

		[System.Serializable]
		internal class RecordCache : java.util.LinkedHashMap<System.Collections.Generic.ICollection
			<org.apache.hadoop.metrics2.MetricsTag>, org.apache.hadoop.metrics2.util.MetricsCache.Record
			>
		{
			private const long serialVersionUID = 1L;

			private bool gotOverflow = false;

			protected override bool removeEldestEntry(System.Collections.Generic.KeyValuePair
				<System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag>, 
				org.apache.hadoop.metrics2.util.MetricsCache.Record> eldest)
			{
				bool overflow = this._enclosing._enclosing.Count > this._enclosing.maxRecsPerName;
				if (overflow && !this.gotOverflow)
				{
					org.apache.hadoop.metrics2.util.MetricsCache.LOG.warn("Metrics cache overflow at "
						 + this._enclosing._enclosing.Count + " for " + eldest);
					this.gotOverflow = true;
				}
				return overflow;
			}

			internal RecordCache(MetricsCache _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MetricsCache _enclosing;
		}

		/// <summary>Cached record</summary>
		public class Record
		{
			internal readonly System.Collections.Generic.IDictionary<string, string> tags = com.google.common.collect.Maps
				.newHashMap();

			internal readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics2.AbstractMetric
				> metrics = com.google.common.collect.Maps.newHashMap();

			/// <summary>Lookup a tag value</summary>
			/// <param name="key">name of the tag</param>
			/// <returns>the tag value</returns>
			public virtual string getTag(string key)
			{
				return tags[key];
			}

			/// <summary>Lookup a metric value</summary>
			/// <param name="key">name of the metric</param>
			/// <returns>the metric value</returns>
			public virtual java.lang.Number getMetric(string key)
			{
				org.apache.hadoop.metrics2.AbstractMetric metric = metrics[key];
				return metric != null ? metric.value() : null;
			}

			/// <summary>Lookup a metric instance</summary>
			/// <param name="key">name of the metric</param>
			/// <returns>the metric instance</returns>
			public virtual org.apache.hadoop.metrics2.AbstractMetric getMetricInstance(string
				 key)
			{
				return metrics[key];
			}

			/// <returns>the entry set of the tags of the record</returns>
			public virtual System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair
				<string, string>> tags()
			{
				return tags;
			}

			/// <returns>entry set of metrics</returns>
			[System.ObsoleteAttribute(@"use metricsEntrySet() instead")]
			public virtual System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair
				<string, java.lang.Number>> metrics()
			{
				System.Collections.Generic.IDictionary<string, java.lang.Number> map = new java.util.LinkedHashMap
					<string, java.lang.Number>(metrics.Count);
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics2.AbstractMetric
					> mapEntry in metrics)
				{
					map[mapEntry.Key] = mapEntry.Value.value();
				}
				return map;
			}

			/// <returns>entry set of metrics</returns>
			public virtual System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair
				<string, org.apache.hadoop.metrics2.AbstractMetric>> metricsEntrySet()
			{
				return metrics;
			}

			public override string ToString()
			{
				return com.google.common.@base.Objects.toStringHelper(this).add("tags", tags).add
					("metrics", metrics).ToString();
			}
		}

		public MetricsCache()
			: this(MAX_RECS_PER_NAME_DEFAULT)
		{
		}

		/// <summary>Construct a metrics cache</summary>
		/// <param name="maxRecsPerName">limit of the number records per record name</param>
		public MetricsCache(int maxRecsPerName)
		{
			this.maxRecsPerName = maxRecsPerName;
		}

		/// <summary>Update the cache and return the current cached record</summary>
		/// <param name="mr">the update record</param>
		/// <param name="includingTags">cache tag values (for later lookup by name) if true</param>
		/// <returns>the updated cache record</returns>
		public virtual org.apache.hadoop.metrics2.util.MetricsCache.Record update(org.apache.hadoop.metrics2.MetricsRecord
			 mr, bool includingTags)
		{
			string name = mr.name();
			org.apache.hadoop.metrics2.util.MetricsCache.RecordCache recordCache = map[name];
			if (recordCache == null)
			{
				recordCache = new org.apache.hadoop.metrics2.util.MetricsCache.RecordCache(this);
				map[name] = recordCache;
			}
			System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag> tags
				 = mr.tags();
			org.apache.hadoop.metrics2.util.MetricsCache.Record record = recordCache[tags];
			if (record == null)
			{
				record = new org.apache.hadoop.metrics2.util.MetricsCache.Record();
				recordCache[tags] = record;
			}
			foreach (org.apache.hadoop.metrics2.AbstractMetric m in mr.metrics())
			{
				record.metrics[m.name()] = m;
			}
			if (includingTags)
			{
				// mostly for some sinks that include tags as part of a dense schema
				foreach (org.apache.hadoop.metrics2.MetricsTag t in mr.tags())
				{
					record.tags[t.name()] = t.value();
				}
			}
			return record;
		}

		/// <summary>Update the cache and return the current cache record</summary>
		/// <param name="mr">the update record</param>
		/// <returns>the updated cache record</returns>
		public virtual org.apache.hadoop.metrics2.util.MetricsCache.Record update(org.apache.hadoop.metrics2.MetricsRecord
			 mr)
		{
			return update(mr, false);
		}

		/// <summary>Get the cached record</summary>
		/// <param name="name">of the record</param>
		/// <param name="tags">of the record</param>
		/// <returns>the cached record or null</returns>
		public virtual org.apache.hadoop.metrics2.util.MetricsCache.Record get(string name
			, System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag> 
			tags)
		{
			org.apache.hadoop.metrics2.util.MetricsCache.RecordCache rc = map[name];
			if (rc == null)
			{
				return null;
			}
			return rc[tags];
		}
	}
}
