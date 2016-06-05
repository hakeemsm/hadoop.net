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
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract class to provide common implementation of the
	/// generic counter group in both mapred and mapreduce package.
	/// </summary>
	/// <?/>
	public abstract class AbstractCounterGroup<T> : CounterGroupBase<T>
		where T : Counter
	{
		private readonly string name;

		private string displayName;

		private readonly ConcurrentMap<string, T> counters = new ConcurrentSkipListMap<string
			, T>();

		private readonly Limits limits;

		public AbstractCounterGroup(string name, string displayName, Limits limits)
		{
			this.name = name;
			this.displayName = displayName;
			this.limits = limits;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual string GetDisplayName()
		{
			lock (this)
			{
				return displayName;
			}
		}

		public virtual void SetDisplayName(string displayName)
		{
			lock (this)
			{
				this.displayName = displayName;
			}
		}

		public virtual void AddCounter(T counter)
		{
			lock (this)
			{
				counters[counter.GetName()] = counter;
				limits.IncrCounters();
			}
		}

		public virtual T AddCounter(string counterName, string displayName, long value)
		{
			lock (this)
			{
				string saveName = Limits.FilterCounterName(counterName);
				T counter = FindCounterImpl(saveName, false);
				if (counter == null)
				{
					return AddCounterImpl(saveName, displayName, value);
				}
				counter.SetValue(value);
				return counter;
			}
		}

		private T AddCounterImpl(string name, string displayName, long value)
		{
			T counter = NewCounter(name, displayName, value);
			AddCounter(counter);
			return counter;
		}

		public virtual T FindCounter(string counterName, string displayName)
		{
			lock (this)
			{
				// Take lock to avoid two threads not finding a counter and trying to add
				// the same counter.
				string saveName = Limits.FilterCounterName(counterName);
				T counter = FindCounterImpl(saveName, false);
				if (counter == null)
				{
					return AddCounterImpl(saveName, displayName, 0);
				}
				return counter;
			}
		}

		public virtual T FindCounter(string counterName, bool create)
		{
			return FindCounterImpl(Limits.FilterCounterName(counterName), create);
		}

		// Lock the object. Cannot simply use concurrent constructs on the counters
		// data-structure (like putIfAbsent) because of localization, limits etc.
		private T FindCounterImpl(string counterName, bool create)
		{
			lock (this)
			{
				T counter = counters[counterName];
				if (counter == null && create)
				{
					string localized = ResourceBundles.GetCounterName(GetName(), counterName, counterName
						);
					return AddCounterImpl(counterName, localized, 0);
				}
				return counter;
			}
		}

		public virtual T FindCounter(string counterName)
		{
			return FindCounter(counterName, true);
		}

		/// <summary>Abstract factory method to create a new counter of type T</summary>
		/// <param name="counterName">of the counter</param>
		/// <param name="displayName">of the counter</param>
		/// <param name="value">of the counter</param>
		/// <returns>a new counter</returns>
		protected internal abstract T NewCounter(string counterName, string displayName, 
			long value);

		/// <summary>Abstract factory method to create a new counter of type T</summary>
		/// <returns>a new counter object</returns>
		protected internal abstract T NewCounter();

		public virtual IEnumerator<T> GetEnumerator()
		{
			return counters.Values.GetEnumerator();
		}

		/// <summary>GenericGroup ::= displayName #counter counter</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			lock (this)
			{
				Text.WriteString(@out, displayName);
				WritableUtils.WriteVInt(@out, counters.Count);
				foreach (Counter counter in counters.Values)
				{
					counter.Write(@out);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			lock (this)
			{
				displayName = StringInterner.WeakIntern(Text.ReadString(@in));
				counters.Clear();
				int size = WritableUtils.ReadVInt(@in);
				for (int i = 0; i < size; i++)
				{
					T counter = NewCounter();
					counter.ReadFields(@in);
					counters[counter.GetName()] = counter;
					limits.IncrCounters();
				}
			}
		}

		public virtual int Size()
		{
			lock (this)
			{
				return counters.Count;
			}
		}

		public override bool Equals(object genericRight)
		{
			lock (this)
			{
				if (genericRight is CounterGroupBase<object>)
				{
					CounterGroupBase<T> right = (CounterGroupBase<T>)genericRight;
					return Iterators.ElementsEqual(GetEnumerator(), right.GetEnumerator());
				}
				return false;
			}
		}

		public override int GetHashCode()
		{
			lock (this)
			{
				return counters.GetHashCode();
			}
		}

		public virtual void IncrAllCounters(CounterGroupBase<T> rightGroup)
		{
			try
			{
				foreach (Counter right in rightGroup)
				{
					Counter left = FindCounter(right.GetName(), right.GetDisplayName());
					left.Increment(right.GetValue());
				}
			}
			catch (LimitExceededException e)
			{
				counters.Clear();
				throw;
			}
		}

		public abstract CounterGroupBase<T> GetUnderlyingGroup();
	}
}
