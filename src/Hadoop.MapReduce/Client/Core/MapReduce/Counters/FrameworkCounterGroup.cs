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
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract class to provide common implementation for the framework
	/// counter group in both mapred and mapreduce packages.
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class FrameworkCounterGroup<T, C> : CounterGroupBase<C>
		where T : Enum<T>
		where C : Counter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Counters.FrameworkCounterGroup
			));

		private readonly Type enumClass;

		private readonly object[] counters;

		private string displayName = null;

		/// <summary>A counter facade for framework counters.</summary>
		/// <remarks>
		/// A counter facade for framework counters.
		/// Use old (which extends new) interface to make compatibility easier.
		/// </remarks>
		public class FrameworkCounter<T> : AbstractCounter
			where T : Enum<T>
		{
			internal readonly T key;

			internal readonly string groupName;

			private long value;

			public FrameworkCounter(T @ref, string groupName)
			{
				// for Enum.valueOf
				// local casts are OK and save a class ref
				key = @ref;
				this.groupName = groupName;
			}

			[InterfaceAudience.Private]
			public virtual T GetKey()
			{
				return key;
			}

			[InterfaceAudience.Private]
			public virtual string GetGroupName()
			{
				return groupName;
			}

			public override string GetName()
			{
				return key.Name();
			}

			public override string GetDisplayName()
			{
				return ResourceBundles.GetCounterName(groupName, GetName(), GetName());
			}

			public override long GetValue()
			{
				return value;
			}

			public override void SetValue(long value)
			{
				this.value = value;
			}

			public override void Increment(long incr)
			{
				value += incr;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				System.Diagnostics.Debug.Assert(false, "shouldn't be called");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(DataInput @in)
			{
				System.Diagnostics.Debug.Assert(false, "shouldn't be called");
			}

			public override Counter GetUnderlyingCounter()
			{
				return this;
			}
		}

		public FrameworkCounterGroup(Type enumClass)
		{
			this.enumClass = enumClass;
			T[] enums = enumClass.GetEnumConstants();
			counters = new object[enums.Length];
		}

		public virtual string GetName()
		{
			return enumClass.FullName;
		}

		public virtual string GetDisplayName()
		{
			if (displayName == null)
			{
				displayName = ResourceBundles.GetCounterGroupName(GetName(), GetName());
			}
			return displayName;
		}

		public virtual void SetDisplayName(string displayName)
		{
			this.displayName = displayName;
		}

		private T ValueOf(string name)
		{
			return Enum.ValueOf(enumClass, name);
		}

		public virtual void AddCounter(C counter)
		{
			C ours = FindCounter(counter.GetName());
			if (ours != null)
			{
				ours.SetValue(counter.GetValue());
			}
			else
			{
				Log.Warn(counter.GetName() + "is not a known counter.");
			}
		}

		public virtual C AddCounter(string name, string displayName, long value)
		{
			C counter = FindCounter(name);
			if (counter != null)
			{
				counter.SetValue(value);
			}
			else
			{
				Log.Warn(name + "is not a known counter.");
			}
			return counter;
		}

		public virtual C FindCounter(string counterName, string displayName)
		{
			return FindCounter(counterName);
		}

		public virtual C FindCounter(string counterName, bool create)
		{
			try
			{
				return FindCounter(ValueOf(counterName));
			}
			catch (Exception e)
			{
				if (create)
				{
					throw new ArgumentException(e);
				}
				return null;
			}
		}

		public virtual C FindCounter(string counterName)
		{
			try
			{
				T enumValue = ValueOf(counterName);
				return FindCounter(enumValue);
			}
			catch (ArgumentException)
			{
				Log.Warn(counterName + " is not a recognized counter.");
				return null;
			}
		}

		private C FindCounter(T key)
		{
			int i = key.Ordinal();
			if (counters[i] == null)
			{
				counters[i] = NewCounter(key);
			}
			return (C)counters[i];
		}

		/// <summary>Abstract factory method for new framework counter</summary>
		/// <param name="key">for the enum value of a counter</param>
		/// <returns>a new counter for the key</returns>
		protected internal abstract C NewCounter(T key);

		public virtual int Size()
		{
			int n = 0;
			for (int i = 0; i < counters.Length; ++i)
			{
				if (counters[i] != null)
				{
					++n;
				}
			}
			return n;
		}

		public virtual void IncrAllCounters(CounterGroupBase<C> other)
		{
			if (Preconditions.CheckNotNull(other, "other counter group") is FrameworkCounterGroup
				<object, object>)
			{
				foreach (Counter counter in other)
				{
					C c = FindCounter(((FrameworkCounterGroup.FrameworkCounter)counter).key.Name());
					if (c != null)
					{
						c.Increment(counter.GetValue());
					}
				}
			}
		}

		/// <summary>FrameworkGroup ::= #counter (key value)</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WritableUtils.WriteVInt(@out, Size());
			for (int i = 0; i < counters.Length; ++i)
			{
				Counter counter = (C)counters[i];
				if (counter != null)
				{
					WritableUtils.WriteVInt(@out, i);
					WritableUtils.WriteVLong(@out, counter.GetValue());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			Clear();
			int len = WritableUtils.ReadVInt(@in);
			T[] enums = enumClass.GetEnumConstants();
			for (int i = 0; i < len; ++i)
			{
				int ord = WritableUtils.ReadVInt(@in);
				Counter counter = NewCounter(enums[ord]);
				counter.SetValue(WritableUtils.ReadVLong(@in));
				counters[ord] = counter;
			}
		}

		private void Clear()
		{
			for (int i = 0; i < counters.Length; ++i)
			{
				counters[i] = null;
			}
		}

		public virtual IEnumerator<C> GetEnumerator()
		{
			return new _AbstractIterator_275(this);
		}

		private sealed class _AbstractIterator_275 : AbstractIterator<C>
		{
			public _AbstractIterator_275(FrameworkCounterGroup<T, C> _enclosing)
			{
				this._enclosing = _enclosing;
				this.i = 0;
			}

			internal int i;

			protected override C ComputeNext()
			{
				while (this.i < this._enclosing.counters.Length)
				{
					C counter = (C)this._enclosing.counters[this.i++];
					if (counter != null)
					{
						return counter;
					}
				}
				return this.EndOfData();
			}

			private readonly FrameworkCounterGroup<T, C> _enclosing;
		}

		public override bool Equals(object genericRight)
		{
			if (genericRight is CounterGroupBase<object>)
			{
				CounterGroupBase<C> right = (CounterGroupBase<C>)genericRight;
				return Iterators.ElementsEqual(GetEnumerator(), right.GetEnumerator());
			}
			return false;
		}

		public override int GetHashCode()
		{
			lock (this)
			{
				// need to be deep as counters is an array
				return Arrays.DeepHashCode(new object[] { enumClass, counters, displayName });
			}
		}

		public abstract CounterGroupBase<C> GetUnderlyingGroup();
	}
}
