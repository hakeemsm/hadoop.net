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
using Sharpen;

namespace org.apache.hadoop.metrics.spi
{
	/// <summary>An implementation of MetricsRecord.</summary>
	/// <remarks>
	/// An implementation of MetricsRecord.  Keeps a back-pointer to the context
	/// from which it was created, and delegates back to it on <code>update</code>
	/// and <code>remove()</code>.
	/// </remarks>
	public class MetricsRecordImpl : org.apache.hadoop.metrics.MetricsRecord
	{
		private org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagTable = new 
			org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap();

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.spi.MetricValue
			> metricTable = new java.util.LinkedHashMap<string, org.apache.hadoop.metrics.spi.MetricValue
			>();

		private string recordName;

		private org.apache.hadoop.metrics.spi.AbstractMetricsContext context;

		/// <summary>Creates a new instance of FileRecord</summary>
		protected internal MetricsRecordImpl(string recordName, org.apache.hadoop.metrics.spi.AbstractMetricsContext
			 context)
		{
			this.recordName = recordName;
			this.context = context;
		}

		/// <summary>Returns the record name.</summary>
		/// <returns>the record name</returns>
		public virtual string getRecordName()
		{
			return recordName;
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void setTag(string tagName, string tagValue)
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
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void setTag(string tagName, int tagValue)
		{
			tagTable[tagName] = int.Parse(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void setTag(string tagName, long tagValue)
		{
			tagTable[tagName] = long.valueOf(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void setTag(string tagName, short tagValue)
		{
			tagTable[tagName] = short.valueOf(tagValue);
		}

		/// <summary>Sets the named tag to the specified value.</summary>
		/// <param name="tagName">name of the tag</param>
		/// <param name="tagValue">new value of the tag</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if the tagName conflicts with the configuration
		/// 	</exception>
		public virtual void setTag(string tagName, byte tagValue)
		{
			tagTable[tagName] = byte.valueOf(tagValue);
		}

		/// <summary>Removes any tag of the specified name.</summary>
		public virtual void removeTag(string tagName)
		{
			Sharpen.Collections.Remove(tagTable, tagName);
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void setMetric(string metricName, int metricValue)
		{
			setAbsolute(metricName, int.Parse(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void setMetric(string metricName, long metricValue)
		{
			setAbsolute(metricName, long.valueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void setMetric(string metricName, short metricValue)
		{
			setAbsolute(metricName, short.valueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void setMetric(string metricName, byte metricValue)
		{
			setAbsolute(metricName, byte.valueOf(metricValue));
		}

		/// <summary>Sets the named metric to the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">new value of the metric</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void setMetric(string metricName, float metricValue)
		{
			setAbsolute(metricName, metricValue);
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void incrMetric(string metricName, int metricValue)
		{
			setIncrement(metricName, int.Parse(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void incrMetric(string metricName, long metricValue)
		{
			setIncrement(metricName, long.valueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void incrMetric(string metricName, short metricValue)
		{
			setIncrement(metricName, short.valueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void incrMetric(string metricName, byte metricValue)
		{
			setIncrement(metricName, byte.valueOf(metricValue));
		}

		/// <summary>Increments the named metric by the specified value.</summary>
		/// <param name="metricName">name of the metric</param>
		/// <param name="metricValue">incremental value</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">
		/// if the metricName or the type of the metricValue
		/// conflicts with the configuration
		/// </exception>
		public virtual void incrMetric(string metricName, float metricValue)
		{
			setIncrement(metricName, metricValue);
		}

		private void setAbsolute(string metricName, java.lang.Number metricValue)
		{
			metricTable[metricName] = new org.apache.hadoop.metrics.spi.MetricValue(metricValue
				, org.apache.hadoop.metrics.spi.MetricValue.ABSOLUTE);
		}

		private void setIncrement(string metricName, java.lang.Number metricValue)
		{
			metricTable[metricName] = new org.apache.hadoop.metrics.spi.MetricValue(metricValue
				, org.apache.hadoop.metrics.spi.MetricValue.INCREMENT);
		}

		/// <summary>Updates the table of buffered data which is to be sent periodically.</summary>
		/// <remarks>
		/// Updates the table of buffered data which is to be sent periodically.
		/// If the tag values match an existing row, that row is updated;
		/// otherwise, a new row is added.
		/// </remarks>
		public virtual void update()
		{
			context.update(this);
		}

		/// <summary>
		/// Removes the row, if it exists, in the buffered data table having tags
		/// that equal the tags that have been set on this record.
		/// </summary>
		public virtual void remove()
		{
			context.remove(this);
		}

		internal virtual org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap getTagTable
			()
		{
			return tagTable;
		}

		internal virtual System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.spi.MetricValue
			> getMetricTable()
		{
			return metricTable;
		}
	}
}
