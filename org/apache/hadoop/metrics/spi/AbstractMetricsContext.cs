/*
* AbstractMetricsContext.java
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
	/// <summary>The main class of the Service Provider Interface.</summary>
	/// <remarks>
	/// The main class of the Service Provider Interface.  This class should be
	/// extended in order to integrate the Metrics API with a specific metrics
	/// client library. <p/>
	/// This class implements the internal table of metric data, and the timer
	/// on which data is to be sent to the metrics system.  Subclasses must
	/// override the abstract <code>emitRecord</code> method in order to transmit
	/// the data. <p/>
	/// </remarks>
	public abstract class AbstractMetricsContext : org.apache.hadoop.metrics.MetricsContext
	{
		private int period = org.apache.hadoop.metrics.MetricsContext.DEFAULT_PERIOD;

		private java.util.Timer timer = null;

		private System.Collections.Generic.ICollection<org.apache.hadoop.metrics.Updater>
			 updaters = new java.util.HashSet<org.apache.hadoop.metrics.Updater>(1);

		private volatile bool isMonitoring = false;

		private org.apache.hadoop.metrics.ContextFactory factory = null;

		private string contextName = null;

		[System.Serializable]
		public class TagMap : System.Collections.Generic.SortedDictionary<string, object>
		{
			private const long serialVersionUID = 3546309335061952993L;

			internal TagMap()
				: base()
			{
			}

			internal TagMap(org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap orig)
				: base(orig)
			{
			}

			/// <summary>Returns true if this tagmap contains every tag in other.</summary>
			public virtual bool containsAll(org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
				 other)
			{
				foreach (System.Collections.Generic.KeyValuePair<string, object> entry in other)
				{
					object value = this[entry.Key];
					if (value == null || !value.Equals(entry.Value))
					{
						// either key does not exist here, or the value is different
						return false;
					}
				}
				return true;
			}
		}

		[System.Serializable]
		public class MetricMap : System.Collections.Generic.SortedDictionary<string, java.lang.Number
			>
		{
			private const long serialVersionUID = -7495051861141631609L;

			internal MetricMap()
				: base()
			{
			}

			internal MetricMap(org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap
				 orig)
				: base(orig)
			{
			}
		}

		[System.Serializable]
		internal class RecordMap : System.Collections.Generic.Dictionary<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
			, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap>
		{
			private const long serialVersionUID = 259835619700264611L;
		}

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap
			> bufferedData = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap
			>();

		/// <summary>Creates a new instance of AbstractMetricsContext</summary>
		protected internal AbstractMetricsContext()
		{
		}

		/// <summary>Initializes the context.</summary>
		public override void init(string contextName, org.apache.hadoop.metrics.ContextFactory
			 factory)
		{
			this.contextName = contextName;
			this.factory = factory;
		}

		/// <summary>Convenience method for subclasses to access factory attributes.</summary>
		protected internal virtual string getAttribute(string attributeName)
		{
			string factoryAttribute = contextName + "." + attributeName;
			return (string)factory.getAttribute(factoryAttribute);
		}

		/// <summary>
		/// Returns an attribute-value map derived from the factory attributes
		/// by finding all factory attributes that begin with
		/// <i>contextName</i>.<i>tableName</i>.
		/// </summary>
		/// <remarks>
		/// Returns an attribute-value map derived from the factory attributes
		/// by finding all factory attributes that begin with
		/// <i>contextName</i>.<i>tableName</i>.  The returned map consists of
		/// those attributes with the contextName and tableName stripped off.
		/// </remarks>
		protected internal virtual System.Collections.Generic.IDictionary<string, string>
			 getAttributeTable(string tableName)
		{
			string prefix = contextName + "." + tableName + ".";
			System.Collections.Generic.IDictionary<string, string> result = new System.Collections.Generic.Dictionary
				<string, string>();
			foreach (string attributeName in factory.getAttributeNames())
			{
				if (attributeName.StartsWith(prefix))
				{
					string name = Sharpen.Runtime.substring(attributeName, prefix.Length);
					string value = (string)factory.getAttribute(attributeName);
					result[name] = value;
				}
			}
			return result;
		}

		/// <summary>Returns the context name.</summary>
		public override string getContextName()
		{
			return contextName;
		}

		/// <summary>Returns the factory by which this context was created.</summary>
		public virtual org.apache.hadoop.metrics.ContextFactory getContextFactory()
		{
			return factory;
		}

		/// <summary>Starts or restarts monitoring, the emitting of metrics records.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void startMonitoring()
		{
			lock (this)
			{
				if (!isMonitoring)
				{
					startTimer();
					isMonitoring = true;
				}
			}
		}

		/// <summary>Stops monitoring.</summary>
		/// <remarks>Stops monitoring.  This does not free buffered data.</remarks>
		/// <seealso cref="close()"/>
		public override void stopMonitoring()
		{
			lock (this)
			{
				if (isMonitoring)
				{
					stopTimer();
					isMonitoring = false;
				}
			}
		}

		/// <summary>Returns true if monitoring is currently in progress.</summary>
		public override bool isMonitoring()
		{
			return isMonitoring;
		}

		/// <summary>
		/// Stops monitoring and frees buffered data, returning this
		/// object to its initial state.
		/// </summary>
		public override void close()
		{
			lock (this)
			{
				stopMonitoring();
				clearUpdaters();
			}
		}

		/// <summary>Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
		/// 	</summary>
		/// <remarks>
		/// Creates a new AbstractMetricsRecord instance with the given <code>recordName</code>.
		/// Throws an exception if the metrics implementation is configured with a fixed
		/// set of record names and <code>recordName</code> is not in that set.
		/// </remarks>
		/// <param name="recordName">the name of the record</param>
		/// <exception cref="org.apache.hadoop.metrics.MetricsException">if recordName conflicts with configuration data
		/// 	</exception>
		public sealed override org.apache.hadoop.metrics.MetricsRecord createRecord(string
			 recordName)
		{
			lock (this)
			{
				if (bufferedData[recordName] == null)
				{
					bufferedData[recordName] = new org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap
						();
				}
				return newRecord(recordName);
			}
		}

		/// <summary>Subclasses should override this if they subclass MetricsRecordImpl.</summary>
		/// <param name="recordName">the name of the record</param>
		/// <returns>newly created instance of MetricsRecordImpl or subclass</returns>
		protected internal virtual org.apache.hadoop.metrics.MetricsRecord newRecord(string
			 recordName)
		{
			return new org.apache.hadoop.metrics.spi.MetricsRecordImpl(recordName, this);
		}

		/// <summary>
		/// Registers a callback to be called at time intervals determined by
		/// the configuration.
		/// </summary>
		/// <param name="updater">
		/// object to be run periodically; it should update
		/// some metrics records
		/// </param>
		public override void registerUpdater(org.apache.hadoop.metrics.Updater updater)
		{
			lock (this)
			{
				if (!updaters.contains(updater))
				{
					updaters.add(updater);
				}
			}
		}

		/// <summary>Removes a callback, if it exists.</summary>
		/// <param name="updater">object to be removed from the callback list</param>
		public override void unregisterUpdater(org.apache.hadoop.metrics.Updater updater)
		{
			lock (this)
			{
				updaters.remove(updater);
			}
		}

		private void clearUpdaters()
		{
			lock (this)
			{
				updaters.clear();
			}
		}

		/// <summary>Starts timer if it is not already started</summary>
		private void startTimer()
		{
			lock (this)
			{
				if (timer == null)
				{
					timer = new java.util.Timer("Timer thread for monitoring " + getContextName(), true
						);
					java.util.TimerTask task = new _TimerTask_261(this);
					long millis = period * 1000;
					timer.scheduleAtFixedRate(task, millis, millis);
				}
			}
		}

		private sealed class _TimerTask_261 : java.util.TimerTask
		{
			public _TimerTask_261(AbstractMetricsContext _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void run()
			{
				try
				{
					this._enclosing.timerEvent();
				}
				catch (System.IO.IOException ioe)
				{
					Sharpen.Runtime.printStackTrace(ioe);
				}
			}

			private readonly AbstractMetricsContext _enclosing;
		}

		/// <summary>Stops timer if it is running</summary>
		private void stopTimer()
		{
			lock (this)
			{
				if (timer != null)
				{
					timer.cancel();
					timer = null;
				}
			}
		}

		/// <summary>Timer callback.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void timerEvent()
		{
			if (isMonitoring)
			{
				System.Collections.Generic.ICollection<org.apache.hadoop.metrics.Updater> myUpdaters;
				lock (this)
				{
					myUpdaters = new System.Collections.Generic.List<org.apache.hadoop.metrics.Updater
						>(updaters);
				}
				// Run all the registered updates without holding a lock
				// on this context
				foreach (org.apache.hadoop.metrics.Updater updater in myUpdaters)
				{
					try
					{
						updater.doUpdates(this);
					}
					catch (System.Exception throwable)
					{
						Sharpen.Runtime.printStackTrace(throwable);
					}
				}
				emitRecords();
			}
		}

		/// <summary>Emits the records.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void emitRecords()
		{
			lock (this)
			{
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap
					> recordEntry in bufferedData)
				{
					org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap recordMap = recordEntry
						.Value;
					lock (recordMap)
					{
						System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
							, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap>> entrySet = recordMap;
						foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
							, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap> entry in entrySet)
						{
							org.apache.hadoop.metrics.spi.OutputRecord outRec = new org.apache.hadoop.metrics.spi.OutputRecord
								(entry.Key, entry.Value);
							emitRecord(contextName, recordEntry.Key, outRec);
						}
					}
				}
				flush();
			}
		}

		/// <summary>Retrieves all the records managed by this MetricsContext.</summary>
		/// <remarks>
		/// Retrieves all the records managed by this MetricsContext.
		/// Useful for monitoring systems that are polling-based.
		/// </remarks>
		/// <returns>A non-null collection of all monitoring records.</returns>
		public override System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
			<org.apache.hadoop.metrics.spi.OutputRecord>> getAllRecords()
		{
			lock (this)
			{
				System.Collections.Generic.IDictionary<string, System.Collections.Generic.ICollection
					<org.apache.hadoop.metrics.spi.OutputRecord>> @out = new System.Collections.Generic.SortedDictionary
					<string, System.Collections.Generic.ICollection<org.apache.hadoop.metrics.spi.OutputRecord
					>>();
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap
					> recordEntry in bufferedData)
				{
					org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap recordMap = recordEntry
						.Value;
					lock (recordMap)
					{
						System.Collections.Generic.IList<org.apache.hadoop.metrics.spi.OutputRecord> records
							 = new System.Collections.Generic.List<org.apache.hadoop.metrics.spi.OutputRecord
							>();
						System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
							, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap>> entrySet = recordMap;
						foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
							, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap> entry in entrySet)
						{
							org.apache.hadoop.metrics.spi.OutputRecord outRec = new org.apache.hadoop.metrics.spi.OutputRecord
								(entry.Key, entry.Value);
							records.add(outRec);
						}
						@out[recordEntry.Key] = records;
					}
				}
				return @out;
			}
		}

		/// <summary>Sends a record to the metrics system.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void emitRecord(string contextName, string recordName
			, org.apache.hadoop.metrics.spi.OutputRecord outRec);

		/// <summary>Called each period after all records have been emitted, this method does nothing.
		/// 	</summary>
		/// <remarks>
		/// Called each period after all records have been emitted, this method does nothing.
		/// Subclasses may override it in order to perform some kind of flush.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void flush()
		{
		}

		/// <summary>Called by MetricsRecordImpl.update().</summary>
		/// <remarks>
		/// Called by MetricsRecordImpl.update().  Creates or updates a row in
		/// the internal table of metric data.
		/// </remarks>
		protected internal virtual void update(org.apache.hadoop.metrics.spi.MetricsRecordImpl
			 record)
		{
			string recordName = record.getRecordName();
			org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagTable = record.getTagTable
				();
			System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.spi.MetricValue
				> metricUpdates = record.getMetricTable();
			org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap recordMap = getRecordMap
				(recordName);
			lock (recordMap)
			{
				org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap metricMap = recordMap
					[tagTable];
				if (metricMap == null)
				{
					metricMap = new org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap();
					org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagMap = new org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
						(tagTable);
					// clone tags
					recordMap[tagMap] = metricMap;
				}
				System.Collections.Generic.ICollection<System.Collections.Generic.KeyValuePair<string
					, org.apache.hadoop.metrics.spi.MetricValue>> entrySet = metricUpdates;
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.metrics.spi.MetricValue
					> entry in entrySet)
				{
					string metricName = entry.Key;
					org.apache.hadoop.metrics.spi.MetricValue updateValue = entry.Value;
					java.lang.Number updateNumber = updateValue.getNumber();
					java.lang.Number currentNumber = metricMap[metricName];
					if (currentNumber == null || updateValue.isAbsolute())
					{
						metricMap[metricName] = updateNumber;
					}
					else
					{
						java.lang.Number newNumber = sum(updateNumber, currentNumber);
						metricMap[metricName] = newNumber;
					}
				}
			}
		}

		private org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap getRecordMap
			(string recordName)
		{
			lock (this)
			{
				return bufferedData[recordName];
			}
		}

		/// <summary>Adds two numbers, coercing the second to the type of the first.</summary>
		private java.lang.Number sum(java.lang.Number a, java.lang.Number b)
		{
			if (a is int)
			{
				return int.Parse(a + b);
			}
			else
			{
				if (a is float)
				{
					return a + b;
				}
				else
				{
					if (a is short)
					{
						return short.valueOf((short)(a + b));
					}
					else
					{
						if (a is byte)
						{
							return byte.valueOf(unchecked((byte)(a + b)));
						}
						else
						{
							if (a is long)
							{
								return long.valueOf((a + b));
							}
							else
							{
								// should never happen
								throw new org.apache.hadoop.metrics.MetricsException("Invalid number type");
							}
						}
					}
				}
			}
		}

		/// <summary>Called by MetricsRecordImpl.remove().</summary>
		/// <remarks>
		/// Called by MetricsRecordImpl.remove().  Removes all matching rows in
		/// the internal table of metric data.  A row matches if it has the same
		/// tag names and values as record, but it may also have additional
		/// tags.
		/// </remarks>
		protected internal virtual void remove(org.apache.hadoop.metrics.spi.MetricsRecordImpl
			 record)
		{
			string recordName = record.getRecordName();
			org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagTable = record.getTagTable
				();
			org.apache.hadoop.metrics.spi.AbstractMetricsContext.RecordMap recordMap = getRecordMap
				(recordName);
			lock (recordMap)
			{
				System.Collections.Generic.IEnumerator<org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
					> it = recordMap.Keys.GetEnumerator();
				while (it.MoveNext())
				{
					org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap rowTags = it.Current;
					if (rowTags.containsAll(tagTable))
					{
						it.remove();
					}
				}
			}
		}

		/// <summary>Returns the timer period.</summary>
		public override int getPeriod()
		{
			return period;
		}

		/// <summary>Sets the timer period</summary>
		protected internal virtual void setPeriod(int period)
		{
			this.period = period;
		}

		/// <summary>
		/// If a period is set in the attribute passed in, override
		/// the default with it.
		/// </summary>
		protected internal virtual void parseAndSetPeriod(string attributeName)
		{
			string periodStr = getAttribute(attributeName);
			if (periodStr != null)
			{
				int period = 0;
				try
				{
					period = System.Convert.ToInt32(periodStr);
				}
				catch (java.lang.NumberFormatException)
				{
				}
				if (period <= 0)
				{
					throw new org.apache.hadoop.metrics.MetricsException("Invalid period: " + periodStr
						);
				}
				setPeriod(period);
			}
		}
	}
}
