/*
* MetricsRecordImpl.java
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
using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>An implementation of MetricsRecord.</summary>
	/// <remarks>
	/// An implementation of MetricsRecord.  Keeps a back-pointer to the context
	/// from which it was created, and delegates back to it on <code>update</code>
	/// and <code>remove()</code>.
	/// </remarks>
	public class MetricsRecordImpl : MetricsRecord
	{
		private AbstractMetricsContext.TagMap tagTable = new AbstractMetricsContext.TagMap
			();

		private IDictionary<string, MetricValue> metricTable = new LinkedHashMap<string, 
			MetricValue>();

		private string recordName;

		private AbstractMetricsContext context;

		/// <summary>Creates a new instance of FileRecord</summary>
		protected internal MetricsRecordImpl(string recordName, AbstractMetricsContext context
			)
		{
			this.recordName = recordName;
			this.context = context;
		}

		/// <summary>Returns the record name.</summary>
		/// <returns>the record name</returns>
		public virtual string GetRecordName()
		{
			return recordName;
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void SetTag(string tagName, string tagValue)
		{
			if (tagValue == null)
			{
				tagValue = string.Empty;
			}
			tagTable[tagName] = tagValue;
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void SetTag(string tagName, int tagValue)
		{
			tagTable[tagName] = Sharpen.Extensions.ValueOf(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void SetTag(string tagName, long tagValue)
		{
			tagTable[tagName] = Sharpen.Extensions.ValueOf(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void SetTag(string tagName, short tagValue)
		{
			tagTable[tagName] = short.ValueOf(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void SetTag(string tagName, byte tagValue)
		{
			tagTable[tagName] = byte.ValueOf(tagValue);
		}

		/// <summary>Removes any tag of the specified name.</summary>
		public virtual void RemoveTag(string tagName)
		{
			Sharpen.Collections.Remove(tagTable, tagName);
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void SetMetric(string metricName, int metricValue)
		{
			SetAbsolute(metricName, Sharpen.Extensions.ValueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void SetMetric(string metricName, long metricValue)
		{
			SetAbsolute(metricName, Sharpen.Extensions.ValueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void SetMetric(string metricName, short metricValue)
		{
			SetAbsolute(metricName, short.ValueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void SetMetric(string metricName, byte metricValue)
		{
			SetAbsolute(metricName, byte.ValueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void SetMetric(string metricName, float metricValue)
		{
			SetAbsolute(metricName, metricValue);
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void IncrMetric(string metricName, int metricValue)
		{
			SetIncrement(metricName, Sharpen.Extensions.ValueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void IncrMetric(string metricName, long metricValue)
		{
			SetIncrement(metricName, Sharpen.Extensions.ValueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void IncrMetric(string metricName, short metricValue)
		{
			SetIncrement(metricName, short.ValueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void IncrMetric(string metricName, byte metricValue)
		{
			SetIncrement(metricName, byte.ValueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void IncrMetric(string metricName, float metricValue)
		{
			SetIncrement(metricName, metricValue);
		}

		private void SetAbsolute(string metricName, Number metricValue)
		{
			metricTable[metricName] = new MetricValue(metricValue, MetricValue.Absolute);
		}

		private void SetIncrement(string metricName, Number metricValue)
		{
			metricTable[metricName] = new MetricValue(metricValue, MetricValue.Increment);
		}

		/// <summary>Updates the table of buffered data which is to be sent periodically.</summary>
		/// <remarks>
		/// Updates the table of buffered data which is to be sent periodically.
		/// If the tag values match an existing row, that row is updated;
		/// otherwise, a new row is added.
		/// </remarks>
		public virtual void Update()
		{
			context.Update(this);
		}

		/// <summary>
		/// Removes the row, if it exists, in the buffered data table having tags
		/// that equal the tags that have been set on this record.
		/// </summary>
		public virtual void Remove()
		{
			context.Remove(this);
		}

		internal virtual AbstractMetricsContext.TagMap GetTagTable()
		{
			return tagTable;
		}

		internal virtual IDictionary<string, MetricValue> GetMetricTable()
		{
			return metricTable;
		}
	}
}
