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
using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Metrics;


namespace Org.Apache.Hadoop.Metrics.Spi
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
	public abstract class AbstractMetricsContext : MetricsContext
	{
		private int period = MetricsContext.DefaultPeriod;

		private Timer timer = null;

		private ICollection<Updater> updaters = new HashSet<Updater>(1);

		private volatile bool isMonitoring = false;

		private ContextFactory factory = null;

		private string contextName = null;

		[System.Serializable]
		public class TagMap : SortedDictionary<string, object>
		{
			private const long serialVersionUID = 3546309335061952993L;

			internal TagMap()
				: base()
			{
			}

			internal TagMap(AbstractMetricsContext.TagMap orig)
				: base(orig)
			{
			}

			/// <summary>Returns true if this tagmap contains every tag in other.</summary>
			public virtual bool ContainsAll(AbstractMetricsContext.TagMap other)
			{
				foreach (KeyValuePair<string, object> entry in other)
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
		public class MetricMap : SortedDictionary<string, Number>
		{
			private const long serialVersionUID = -7495051861141631609L;

			internal MetricMap()
				: base()
			{
			}

			internal MetricMap(AbstractMetricsContext.MetricMap orig)
				: base(orig)
			{
			}
		}

		[System.Serializable]
		internal class RecordMap : Dictionary<AbstractMetricsContext.TagMap, AbstractMetricsContext.MetricMap
			>
		{
			private const long serialVersionUID = 259835619700264611L;
		}

		private IDictionary<string, AbstractMetricsContext.RecordMap> bufferedData = new 
			Dictionary<string, AbstractMetricsContext.RecordMap>();

		/// <summary>Creates a new instance of AbstractMetricsContext</summary>
		protected internal AbstractMetricsContext()
		{
		}

		/// <summary>Initializes the context.</summary>
		public override void Init(string contextName, ContextFactory factory)
		{
			this.contextName = contextName;
			this.factory = factory;
		}

		/// <summary>Convenience method for subclasses to access factory attributes.</summary>
		protected internal virtual string GetAttribute(string attributeName)
		{
			string factoryAttribute = contextName + "." + attributeName;
			return (string)factory.GetAttribute(factoryAttribute);
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
		protected internal virtual IDictionary<string, string> GetAttributeTable(string tableName
			)
		{
			string prefix = contextName + "." + tableName + ".";
			IDictionary<string, string> result = new Dictionary<string, string>();
			foreach (string attributeName in factory.GetAttributeNames())
			{
				if (attributeName.StartsWith(prefix))
				{
					string name = Runtime.Substring(attributeName, prefix.Length);
					string value = (string)factory.GetAttribute(attributeName);
					result[name] = value;
				}
			}
			return result;
		}

		/// <summary>Returns the context name.</summary>
		public override string GetContextName()
		{
			return contextName;
		}

		/// <summary>Returns the factory by which this context was created.</summary>
		public virtual ContextFactory GetContextFactory()
		{
			return factory;
		}

		/// <summary>Starts or restarts monitoring, the emitting of metrics records.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void StartMonitoring()
		{
			lock (this)
			{
				if (!isMonitoring)
				{
					StartTimer();
					isMonitoring = true;
				}
			}
		}

		/// <summary>Stops monitoring.</summary>
		/// <remarks>Stops monitoring.  This does not free buffered data.</remarks>
		/// <seealso cref="Close()"/>
		public override void StopMonitoring()
		{
			lock (this)
			{
				if (isMonitoring)
				{
					StopTimer();
					isMonitoring = false;
				}
			}
		}

		/// <summary>Returns true if monitoring is currently in progress.</summary>
		public override bool IsMonitoring()
		{
			return isMonitoring;
		}

		/// <summary>
		/// Stops monitoring and frees buffered data, returning this
		/// object to its initial state.
		/// </summary>
		public override void Close()
		{
			lock (this)
			{
				StopMonitoring();
				ClearUpdaters();
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
		/// <exception cref="Org.Apache.Hadoop.Metrics.MetricsException">if recordName conflicts with configuration data
		/// 	</exception>
		public sealed override MetricsRecord CreateRecord(string recordName)
		{
			lock (this)
			{
				if (bufferedData[recordName] == null)
				{
					bufferedData[recordName] = new AbstractMetricsContext.RecordMap();
				}
				return NewRecord(recordName);
			}
		}

		/// <summary>Subclasses should override this if they subclass MetricsRecordImpl.</summary>
		/// <param name="recordName">the name of the record</param>
		/// <returns>newly created instance of MetricsRecordImpl or subclass</returns>
		protected internal virtual MetricsRecord NewRecord(string recordName)
		{
			return new MetricsRecordImpl(recordName, this);
		}

		/// <summary>
		/// Registers a callback to be called at time intervals determined by
		/// the configuration.
		/// </summary>
		/// <param name="updater">
		/// object to be run periodically; it should update
		/// some metrics records
		/// </param>
		public override void RegisterUpdater(Updater updater)
		{
			lock (this)
			{
				if (!updaters.Contains(updater))
				{
					updaters.AddItem(updater);
				}
			}
		}

		/// <summary>Removes a callback, if it exists.</summary>
		/// <param name="updater">object to be removed from the callback list</param>
		public override void UnregisterUpdater(Updater updater)
		{
			lock (this)
			{
				updaters.Remove(updater);
			}
		}

		private void ClearUpdaters()
		{
			lock (this)
			{
				updaters.Clear();
			}
		}

		/// <summary>Starts timer if it is not already started</summary>
		private void StartTimer()
		{
			lock (this)
			{
				if (timer == null)
				{
					timer = new Timer("Timer thread for monitoring " + GetContextName(), true);
					TimerTask task = new _TimerTask_261(this);
					long millis = period * 1000;
					timer.ScheduleAtFixedRate(task, millis, millis);
				}
			}
		}

		private sealed class _TimerTask_261 : TimerTask
		{
			public _TimerTask_261(AbstractMetricsContext _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.TimerEvent();
				}
				catch (IOException ioe)
				{
					Runtime.PrintStackTrace(ioe);
				}
			}

			private readonly AbstractMetricsContext _enclosing;
		}

		/// <summary>Stops timer if it is running</summary>
		private void StopTimer()
		{
			lock (this)
			{
				if (timer != null)
				{
					timer.Cancel();
					timer = null;
				}
			}
		}

		/// <summary>Timer callback.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void TimerEvent()
		{
			if (isMonitoring)
			{
				ICollection<Updater> myUpdaters;
				lock (this)
				{
					myUpdaters = new AList<Updater>(updaters);
				}
				// Run all the registered updates without holding a lock
				// on this context
				foreach (Updater updater in myUpdaters)
				{
					try
					{
						updater.DoUpdates(this);
					}
					catch (Exception throwable)
					{
						Runtime.PrintStackTrace(throwable);
					}
				}
				EmitRecords();
			}
		}

		/// <summary>Emits the records.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void EmitRecords()
		{
			lock (this)
			{
				foreach (KeyValuePair<string, AbstractMetricsContext.RecordMap> recordEntry in bufferedData)
				{
					AbstractMetricsContext.RecordMap recordMap = recordEntry.Value;
					lock (recordMap)
					{
						ICollection<KeyValuePair<AbstractMetricsContext.TagMap, AbstractMetricsContext.MetricMap
							>> entrySet = recordMap;
						foreach (KeyValuePair<AbstractMetricsContext.TagMap, AbstractMetricsContext.MetricMap
							> entry in entrySet)
						{
							OutputRecord outRec = new OutputRecord(entry.Key, entry.Value);
							EmitRecord(contextName, recordEntry.Key, outRec);
						}
					}
				}
				Flush();
			}
		}

		/// <summary>Retrieves all the records managed by this MetricsContext.</summary>
		/// <remarks>
		/// Retrieves all the records managed by this MetricsContext.
		/// Useful for monitoring systems that are polling-based.
		/// </remarks>
		/// <returns>A non-null collection of all monitoring records.</returns>
		public override IDictionary<string, ICollection<OutputRecord>> GetAllRecords()
		{
			lock (this)
			{
				IDictionary<string, ICollection<OutputRecord>> @out = new SortedDictionary<string
					, ICollection<OutputRecord>>();
				foreach (KeyValuePair<string, AbstractMetricsContext.RecordMap> recordEntry in bufferedData)
				{
					AbstractMetricsContext.RecordMap recordMap = recordEntry.Value;
					lock (recordMap)
					{
						IList<OutputRecord> records = new AList<OutputRecord>();
						ICollection<KeyValuePair<AbstractMetricsContext.TagMap, AbstractMetricsContext.MetricMap
							>> entrySet = recordMap;
						foreach (KeyValuePair<AbstractMetricsContext.TagMap, AbstractMetricsContext.MetricMap
							> entry in entrySet)
						{
							OutputRecord outRec = new OutputRecord(entry.Key, entry.Value);
							records.AddItem(outRec);
						}
						@out[recordEntry.Key] = records;
					}
				}
				return @out;
			}
		}

		/// <summary>Sends a record to the metrics system.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void EmitRecord(string contextName, string recordName
			, OutputRecord outRec);

		/// <summary>Called each period after all records have been emitted, this method does nothing.
		/// 	</summary>
		/// <remarks>
		/// Called each period after all records have been emitted, this method does nothing.
		/// Subclasses may override it in order to perform some kind of flush.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Flush()
		{
		}

		/// <summary>Called by MetricsRecordImpl.update().</summary>
		/// <remarks>
		/// Called by MetricsRecordImpl.update().  Creates or updates a row in
		/// the internal table of metric data.
		/// </remarks>
		protected internal virtual void Update(MetricsRecordImpl record)
		{
			string recordName = record.GetRecordName();
			AbstractMetricsContext.TagMap tagTable = record.GetTagTable();
			IDictionary<string, MetricValue> metricUpdates = record.GetMetricTable();
			AbstractMetricsContext.RecordMap recordMap = GetRecordMap(recordName);
			lock (recordMap)
			{
				AbstractMetricsContext.MetricMap metricMap = recordMap[tagTable];
				if (metricMap == null)
				{
					metricMap = new AbstractMetricsContext.MetricMap();
					AbstractMetricsContext.TagMap tagMap = new AbstractMetricsContext.TagMap(tagTable
						);
					// clone tags
					recordMap[tagMap] = metricMap;
				}
				ICollection<KeyValuePair<string, MetricValue>> entrySet = metricUpdates;
				foreach (KeyValuePair<string, MetricValue> entry in entrySet)
				{
					string metricName = entry.Key;
					MetricValue updateValue = entry.Value;
					Number updateNumber = updateValue.GetNumber();
					Number currentNumber = metricMap[metricName];
					if (currentNumber == null || updateValue.IsAbsolute())
					{
						metricMap[metricName] = updateNumber;
					}
					else
					{
						Number newNumber = Sum(updateNumber, currentNumber);
						metricMap[metricName] = newNumber;
					}
				}
			}
		}

		private AbstractMetricsContext.RecordMap GetRecordMap(string recordName)
		{
			lock (this)
			{
				return bufferedData[recordName];
			}
		}

		/// <summary>Adds two numbers, coercing the second to the type of the first.</summary>
		private Number Sum(Number a, Number b)
		{
			if (a is int)
			{
				return Extensions.ValueOf(a + b);
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
						return short.ValueOf((short)(a + b));
					}
					else
					{
						if (a is byte)
						{
							return byte.ValueOf(unchecked((byte)(a + b)));
						}
						else
						{
							if (a is long)
							{
								return Extensions.ValueOf((a + b));
							}
							else
							{
								// should never happen
								throw new MetricsException("Invalid number type");
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
		protected internal virtual void Remove(MetricsRecordImpl record)
		{
			string recordName = record.GetRecordName();
			AbstractMetricsContext.TagMap tagTable = record.GetTagTable();
			AbstractMetricsContext.RecordMap recordMap = GetRecordMap(recordName);
			lock (recordMap)
			{
				IEnumerator<AbstractMetricsContext.TagMap> it = recordMap.Keys.GetEnumerator();
				while (it.HasNext())
				{
					AbstractMetricsContext.TagMap rowTags = it.Next();
					if (rowTags.ContainsAll(tagTable))
					{
						it.Remove();
					}
				}
			}
		}

		/// <summary>Returns the timer period.</summary>
		public override int GetPeriod()
		{
			return period;
		}

		/// <summary>Sets the timer period</summary>
		protected internal virtual void SetPeriod(int period)
		{
			this.period = period;
		}

		/// <summary>
		/// If a period is set in the attribute passed in, override
		/// the default with it.
		/// </summary>
		protected internal virtual void ParseAndSetPeriod(string attributeName)
		{
			string periodStr = GetAttribute(attributeName);
			if (periodStr != null)
			{
				int period = 0;
				try
				{
					period = System.Convert.ToInt32(periodStr);
				}
				catch (FormatException)
				{
				}
				if (period <= 0)
				{
					throw new MetricsException("Invalid period: " + periodStr);
				}
				SetPeriod(period);
			}
		}
	}
}
