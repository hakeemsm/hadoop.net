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
using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>A metrics cache for sinks that don't support sparse updates.</summary>
	public class MetricsCache
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Util.MetricsCache
			));

		internal const int MaxRecsPerNameDefault = 1000;

		private readonly IDictionary<string, MetricsCache.RecordCache> map = Maps.NewHashMap
			();

		private readonly int maxRecsPerName;

		[System.Serializable]
		internal class RecordCache : LinkedHashMap<ICollection<MetricsTag>, MetricsCache.Record
			>
		{
			private const long serialVersionUID = 1L;

			private bool gotOverflow = false;

			protected override bool RemoveEldestEntry(KeyValuePair<ICollection<MetricsTag>, MetricsCache.Record
				> eldest)
			{
				bool overflow = this._enclosing._enclosing.Count > this._enclosing.maxRecsPerName;
				if (overflow && !this.gotOverflow)
				{
					MetricsCache.Log.Warn("Metrics cache overflow at " + this._enclosing._enclosing.Count
						 + " for " + eldest);
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
			internal readonly IDictionary<string, string> tags = Maps.NewHashMap();

			internal readonly IDictionary<string, AbstractMetric> metrics = Maps.NewHashMap();

			/// <summary>Lookup a tag value</summary>
			/// <param name="key">name of the tag</param>
			/// <returns>the tag value</returns>
			public virtual string GetTag(string key)
			{
				return tags[key];
			}

			/// <summary>Lookup a metric value</summary>
			/// <param name="key">name of the metric</param>
			/// <returns>the metric value</returns>
			public virtual Number GetMetric(string key)
			{
				AbstractMetric metric = metrics[key];
				return metric != null ? metric.Value() : null;
			}

			/// <summary>Lookup a metric instance</summary>
			/// <param name="key">name of the metric</param>
			/// <returns>the metric instance</returns>
			public virtual AbstractMetric GetMetricInstance(string key)
			{
				return metrics[key];
			}

			/// <returns>the entry set of the tags of the record</returns>
			public virtual ICollection<KeyValuePair<string, string>> Tags()
			{
				return tags;
			}

			/// <returns>entry set of metrics</returns>
			[System.ObsoleteAttribute(@"use metricsEntrySet() instead")]
			public virtual ICollection<KeyValuePair<string, Number>> Metrics()
			{
				IDictionary<string, Number> map = new LinkedHashMap<string, Number>(metrics.Count
					);
				foreach (KeyValuePair<string, AbstractMetric> mapEntry in metrics)
				{
					map[mapEntry.Key] = mapEntry.Value.Value();
				}
				return map;
			}

			/// <returns>entry set of metrics</returns>
			public virtual ICollection<KeyValuePair<string, AbstractMetric>> MetricsEntrySet(
				)
			{
				return metrics;
			}

			public override string ToString()
			{
				return Objects.ToStringHelper(this).Add("tags", tags).Add("metrics", metrics).ToString
					();
			}
		}

		public MetricsCache()
			: this(MaxRecsPerNameDefault)
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
		public virtual MetricsCache.Record Update(MetricsRecord mr, bool includingTags)
		{
			string name = mr.Name();
			MetricsCache.RecordCache recordCache = map[name];
			if (recordCache == null)
			{
				recordCache = new MetricsCache.RecordCache(this);
				map[name] = recordCache;
			}
			ICollection<MetricsTag> tags = mr.Tags();
			MetricsCache.Record record = recordCache[tags];
			if (record == null)
			{
				record = new MetricsCache.Record();
				recordCache[tags] = record;
			}
			foreach (AbstractMetric m in mr.Metrics())
			{
				record.metrics[m.Name()] = m;
			}
			if (includingTags)
			{
				// mostly for some sinks that include tags as part of a dense schema
				foreach (MetricsTag t in mr.Tags())
				{
					record.tags[t.Name()] = t.Value();
				}
			}
			return record;
		}

		/// <summary>Update the cache and return the current cache record</summary>
		/// <param name="mr">the update record</param>
		/// <returns>the updated cache record</returns>
		public virtual MetricsCache.Record Update(MetricsRecord mr)
		{
			return Update(mr, false);
		}

		/// <summary>Get the cached record</summary>
		/// <param name="name">of the record</param>
		/// <param name="tags">of the record</param>
		/// <returns>the cached record or null</returns>
		public virtual MetricsCache.Record Get(string name, ICollection<MetricsTag> tags)
		{
			MetricsCache.RecordCache rc = map[name];
			if (rc == null)
			{
				return null;
			}
			return rc[tags];
		}
	}
}
