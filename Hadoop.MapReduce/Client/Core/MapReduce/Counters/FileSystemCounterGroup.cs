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
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract class to provide common implementation of the filesystem
	/// counter group in both mapred and mapreduce packages.
	/// </summary>
	/// <?/>
	public abstract class FileSystemCounterGroup<C> : CounterGroupBase<C>
		where C : Counter
	{
		internal const int MaxNumSchemes = 100;

		internal static readonly ConcurrentMap<string, string> schemes = Maps.NewConcurrentMap
			();

		private static readonly Log Log = LogFactory.GetLog(typeof(FileSystemCounterGroup
			));

		private readonly IDictionary<string, object[]> map = new ConcurrentSkipListMap<string
			, object[]>();

		private string displayName;

		private static readonly Joiner NameJoiner = Joiner.On('_');

		private static readonly Joiner DispJoiner = Joiner.On(": ");

		public class FSCounter : AbstractCounter
		{
			internal readonly string scheme;

			internal readonly FileSystemCounter key;

			private long value;

			public FSCounter(string scheme, FileSystemCounter @ref)
			{
				// intern/sanity check
				// C[] would need Array.newInstance which requires a Class<C> reference.
				// Just a few local casts probably worth not having to carry it around.
				this.scheme = scheme;
				key = @ref;
			}

			[InterfaceAudience.Private]
			public virtual string GetScheme()
			{
				return scheme;
			}

			[InterfaceAudience.Private]
			public virtual FileSystemCounter GetFileSystemCounter()
			{
				return key;
			}

			public override string GetName()
			{
				return NameJoiner.Join(scheme, key.ToString());
			}

			public override string GetDisplayName()
			{
				return DispJoiner.Join(scheme, LocalizeCounterName(key.ToString()));
			}

			protected internal virtual string LocalizeCounterName(string counterName)
			{
				return ResourceBundles.GetCounterName(typeof(FileSystemCounter).FullName, counterName
					, counterName);
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

		public virtual string GetName()
		{
			return typeof(FileSystemCounter).FullName;
		}

		public virtual string GetDisplayName()
		{
			if (displayName == null)
			{
				displayName = ResourceBundles.GetCounterGroupName(GetName(), "File System Counters"
					);
			}
			return displayName;
		}

		public virtual void SetDisplayName(string displayName)
		{
			this.displayName = displayName;
		}

		public virtual void AddCounter(C counter)
		{
			C ours;
			if (counter is FileSystemCounterGroup.FSCounter)
			{
				FileSystemCounterGroup.FSCounter c = (FileSystemCounterGroup.FSCounter)counter;
				ours = FindCounter(c.scheme, c.key);
			}
			else
			{
				ours = FindCounter(counter.GetName());
			}
			if (ours != null)
			{
				ours.SetValue(counter.GetValue());
			}
		}

		public virtual C AddCounter(string name, string displayName, long value)
		{
			C counter = FindCounter(name);
			if (counter != null)
			{
				counter.SetValue(value);
			}
			return counter;
		}

		// Parse generic counter name into [scheme, key]
		private string[] ParseCounterName(string counterName)
		{
			int schemeEnd = counterName.IndexOf('_');
			if (schemeEnd < 0)
			{
				throw new ArgumentException("bad fs counter name");
			}
			return new string[] { Sharpen.Runtime.Substring(counterName, 0, schemeEnd), Sharpen.Runtime.Substring
				(counterName, schemeEnd + 1) };
		}

		public virtual C FindCounter(string counterName, string displayName)
		{
			return FindCounter(counterName);
		}

		public virtual C FindCounter(string counterName, bool create)
		{
			try
			{
				string[] pair = ParseCounterName(counterName);
				return FindCounter(pair[0], FileSystemCounter.ValueOf(pair[1]));
			}
			catch (Exception e)
			{
				if (create)
				{
					throw new ArgumentException(e);
				}
				Log.Warn(counterName + " is not a recognized counter.");
				return null;
			}
		}

		public virtual C FindCounter(string counterName)
		{
			return FindCounter(counterName, false);
		}

		public virtual C FindCounter(string scheme, FileSystemCounter key)
		{
			lock (this)
			{
				string canonicalScheme = CheckScheme(scheme);
				object[] counters = map[canonicalScheme];
				int ord = (int)(key);
				if (counters == null)
				{
					counters = new object[FileSystemCounter.Values().Length];
					map[canonicalScheme] = counters;
					counters[ord] = NewCounter(canonicalScheme, key);
				}
				else
				{
					if (counters[ord] == null)
					{
						counters[ord] = NewCounter(canonicalScheme, key);
					}
				}
				return (C)counters[ord];
			}
		}

		private string CheckScheme(string scheme)
		{
			string @fixed = StringUtils.ToUpperCase(scheme);
			string interned = schemes.PutIfAbsent(@fixed, @fixed);
			if (schemes.Count > MaxNumSchemes)
			{
				// mistakes or abuses
				throw new ArgumentException("too many schemes? " + schemes.Count + " when process scheme: "
					 + scheme);
			}
			return interned == null ? @fixed : interned;
		}

		/// <summary>Abstract factory method to create a file system counter</summary>
		/// <param name="scheme">of the file system</param>
		/// <param name="key">the enum of the file system counter</param>
		/// <returns>a new file system counter</returns>
		protected internal abstract C NewCounter(string scheme, FileSystemCounter key);

		public virtual int Size()
		{
			int n = 0;
			foreach (object[] counters in map.Values)
			{
				n += NumSetCounters(counters);
			}
			return n;
		}

		public virtual void IncrAllCounters(CounterGroupBase<C> other)
		{
			if (Preconditions.CheckNotNull(other.GetUnderlyingGroup(), "other group") is FileSystemCounterGroup
				<object>)
			{
				foreach (Counter counter in other)
				{
					FileSystemCounterGroup.FSCounter c = (FileSystemCounterGroup.FSCounter)((Counter)
						counter).GetUnderlyingCounter();
					FindCounter(c.scheme, c.key).Increment(counter.GetValue());
				}
			}
		}

		/// <summary>FileSystemGroup ::= #scheme (scheme #counter (key value)*)</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WritableUtils.WriteVInt(@out, map.Count);
			// #scheme
			foreach (KeyValuePair<string, object[]> entry in map)
			{
				WritableUtils.WriteString(@out, entry.Key);
				// scheme
				// #counter for the above scheme
				WritableUtils.WriteVInt(@out, NumSetCounters(entry.Value));
				foreach (object counter in entry.Value)
				{
					if (counter == null)
					{
						continue;
					}
					FileSystemCounterGroup.FSCounter c = (FileSystemCounterGroup.FSCounter)((Counter)
						counter).GetUnderlyingCounter();
					WritableUtils.WriteVInt(@out, (int)(c.key));
					// key
					WritableUtils.WriteVLong(@out, c.GetValue());
				}
			}
		}

		// value
		private int NumSetCounters(object[] counters)
		{
			int n = 0;
			foreach (object counter in counters)
			{
				if (counter != null)
				{
					++n;
				}
			}
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			int numSchemes = WritableUtils.ReadVInt(@in);
			// #scheme
			FileSystemCounter[] enums = FileSystemCounter.Values();
			for (int i = 0; i < numSchemes; ++i)
			{
				string scheme = WritableUtils.ReadString(@in);
				// scheme
				int numCounters = WritableUtils.ReadVInt(@in);
				// #counter
				for (int j = 0; j < numCounters; ++j)
				{
					FindCounter(scheme, enums[WritableUtils.ReadVInt(@in)]).SetValue(WritableUtils.ReadVLong
						(@in));
				}
			}
		}

		// key
		// value
		public virtual IEnumerator<C> GetEnumerator()
		{
			return new _AbstractIterator_311(this);
		}

		private sealed class _AbstractIterator_311 : AbstractIterator<C>
		{
			public _AbstractIterator_311()
			{
				this.it = this._enclosing.map.Values.GetEnumerator();
				this.counters = this.it.HasNext() ? this.it.Next() : null;
				this.i = 0;
			}

			internal IEnumerator<object[]> it;

			internal object[] counters;

			internal int i;

			protected override C ComputeNext()
			{
				while (this.counters != null)
				{
					while (this.i < this.counters.Length)
					{
						C counter = (C)this.counters[this.i++];
						if (counter != null)
						{
							return counter;
						}
					}
					this.i = 0;
					this.counters = this.it.HasNext() ? this.it.Next() : null;
				}
				return this.EndOfData();
			}
		}

		public override bool Equals(object genericRight)
		{
			lock (this)
			{
				if (genericRight is CounterGroupBase<object>)
				{
					CounterGroupBase<C> right = (CounterGroupBase<C>)genericRight;
					return Iterators.ElementsEqual(GetEnumerator(), right.GetEnumerator());
				}
				return false;
			}
		}

		public override int GetHashCode()
		{
			lock (this)
			{
				// need to be deep as counters is an array
				int hash = typeof(FileSystemCounter).GetHashCode();
				foreach (object[] counters in map.Values)
				{
					if (counters != null)
					{
						hash ^= Arrays.HashCode(counters);
					}
				}
				return hash;
			}
		}

		public abstract CounterGroupBase<C> GetUnderlyingGroup();
	}
}
