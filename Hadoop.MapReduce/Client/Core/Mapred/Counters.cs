using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Collections;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A set of named counters.</summary>
	/// <remarks>
	/// A set of named counters.
	/// <p><code>Counters</code> represent global counters, defined either by the
	/// Map-Reduce framework or applications. Each <code>Counter</code> can be of
	/// any
	/// <see cref="Sharpen.Enum{E}"/>
	/// type.</p>
	/// <p><code>Counters</code> are bunched into
	/// <see cref="Group"/>
	/// s, each comprising of
	/// counters from a particular <code>Enum</code> class.
	/// </remarks>
	public class Counters : AbstractCounters<Counters.Counter, Counters.Group>
	{
		public static int MaxCounterLimit = Limits.GetCountersMax();

		public static int MaxGroupLimit = Limits.GetGroupsMax();

		private static Dictionary<string, string> depricatedCounterMap = new Dictionary<string
			, string>();

		static Counters()
		{
			InitDepricatedMap();
		}

		public Counters()
			: base(groupFactory)
		{
		}

		public Counters(Org.Apache.Hadoop.Mapreduce.Counters newCounters)
			: base(newCounters, groupFactory)
		{
		}

		private static void InitDepricatedMap()
		{
			depricatedCounterMap[typeof(FileInputFormat.Counter).FullName] = typeof(FileInputFormatCounter
				).FullName;
			depricatedCounterMap[typeof(FileOutputFormat.Counter).FullName] = typeof(FileOutputFormatCounter
				).FullName;
			depricatedCounterMap[typeof(FileInputFormat.Counter).FullName] = typeof(FileInputFormatCounter
				).FullName;
			depricatedCounterMap[typeof(FileOutputFormat.Counter).FullName] = typeof(FileOutputFormatCounter
				).FullName;
		}

		private static string GetNewGroupKey(string oldGroup)
		{
			if (depricatedCounterMap.Contains(oldGroup))
			{
				return depricatedCounterMap[oldGroup];
			}
			return null;
		}

		/// <summary>
		/// Downgrade new
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Counters"/>
		/// to old Counters
		/// </summary>
		/// <param name="newCounters">new Counters</param>
		/// <returns>old Counters instance corresponding to newCounters</returns>
		internal static Org.Apache.Hadoop.Mapred.Counters Downgrade(Org.Apache.Hadoop.Mapreduce.Counters
			 newCounters)
		{
			return new Org.Apache.Hadoop.Mapred.Counters(newCounters);
		}

		public override Counters.Group GetGroup(string groupName)
		{
			lock (this)
			{
				return base.GetGroup(groupName);
			}
		}

		public override IEnumerable<string> GetGroupNames()
		{
			lock (this)
			{
				return IteratorUtils.ToList(base.GetGroupNames().GetEnumerator());
			}
		}

		public virtual string MakeCompactString()
		{
			lock (this)
			{
				StringBuilder builder = new StringBuilder();
				bool first = true;
				foreach (Counters.Group group in this)
				{
					foreach (Counters.Counter counter in group)
					{
						if (first)
						{
							first = false;
						}
						else
						{
							builder.Append(',');
						}
						builder.Append(group.GetDisplayName());
						builder.Append('.');
						builder.Append(counter.GetDisplayName());
						builder.Append(':');
						builder.Append(counter.GetCounter());
					}
				}
				return builder.ToString();
			}
		}

		/// <summary>A counter record, comprising its name and value.</summary>
		public class Counter : Org.Apache.Hadoop.Mapreduce.Counter
		{
			internal Org.Apache.Hadoop.Mapreduce.Counter realCounter;

			internal Counter(Org.Apache.Hadoop.Mapreduce.Counter counter)
			{
				this.realCounter = counter;
			}

			public Counter()
				: this(new GenericCounter())
			{
			}

			public virtual void SetDisplayName(string displayName)
			{
				realCounter.SetDisplayName(displayName);
			}

			public virtual string GetName()
			{
				return realCounter.GetName();
			}

			public virtual string GetDisplayName()
			{
				return realCounter.GetDisplayName();
			}

			public virtual long GetValue()
			{
				return realCounter.GetValue();
			}

			public virtual void SetValue(long value)
			{
				realCounter.SetValue(value);
			}

			public virtual void Increment(long incr)
			{
				realCounter.Increment(incr);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				realCounter.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				realCounter.ReadFields(@in);
			}

			/// <summary>
			/// Returns the compact stringified version of the counter in the format
			/// [(actual-name)(display-name)(value)]
			/// </summary>
			/// <returns>the stringified result</returns>
			public virtual string MakeEscapedCompactString()
			{
				return CountersStrings.ToEscapedCompactString(realCounter);
			}

			/// <summary>Checks for (content) equality of two (basic) counters</summary>
			/// <param name="counter">to compare</param>
			/// <returns>true if content equals</returns>
			[System.ObsoleteAttribute]
			public virtual bool ContentEquals(Counters.Counter counter)
			{
				return realCounter.Equals(counter.GetUnderlyingCounter());
			}

			/// <returns>the value of the counter</returns>
			public virtual long GetCounter()
			{
				return realCounter.GetValue();
			}

			public virtual Org.Apache.Hadoop.Mapreduce.Counter GetUnderlyingCounter()
			{
				return realCounter;
			}

			public override bool Equals(object genericRight)
			{
				lock (this)
				{
					if (genericRight is Counters.Counter)
					{
						lock (genericRight)
						{
							Counters.Counter right = (Counters.Counter)genericRight;
							return GetName().Equals(right.GetName()) && GetDisplayName().Equals(right.GetDisplayName
								()) && GetValue() == right.GetValue();
						}
					}
					return false;
				}
			}

			public override int GetHashCode()
			{
				return realCounter.GetHashCode();
			}
		}

		/// <summary>
		/// <code>Group</code> of counters, comprising of counters from a particular
		/// counter
		/// <see cref="Sharpen.Enum{E}"/>
		/// class.
		/// <p><code>Group</code>handles localization of the class name and the
		/// counter names.</p>
		/// </summary>
		public class Group : CounterGroupBase<Counters.Counter>
		{
			private CounterGroupBase<Counters.Counter> realGroup;

			protected internal Group()
			{
				realGroup = null;
			}

			internal Group(Counters.GenericGroup group)
			{
				this.realGroup = group;
			}

			internal Group(Counters.FSGroupImpl group)
			{
				this.realGroup = group;
			}

			internal Group(Counters.FrameworkGroupImpl group)
			{
				this.realGroup = group;
			}

			/// <param name="counterName">the name of the counter</param>
			/// <returns>
			/// the value of the specified counter, or 0 if the counter does
			/// not exist.
			/// </returns>
			public virtual long GetCounter(string counterName)
			{
				return GetCounterValue(realGroup, counterName);
			}

			/// <returns>
			/// the compact stringified version of the group in the format
			/// {(actual-name)(display-name)(value)[][][]} where [] are compact strings
			/// for the counters within.
			/// </returns>
			public virtual string MakeEscapedCompactString()
			{
				return CountersStrings.ToEscapedCompactString(realGroup);
			}

			/// <summary>Get the counter for the given id and create it if it doesn't exist.</summary>
			/// <param name="id">the numeric id of the counter within the group</param>
			/// <param name="name">the internal counter name</param>
			/// <returns>the counter</returns>
			[System.ObsoleteAttribute(@"use FindCounter(string) instead")]
			public virtual Counters.Counter GetCounter(int id, string name)
			{
				return FindCounter(name);
			}

			/// <summary>Get the counter for the given name and create it if it doesn't exist.</summary>
			/// <param name="name">the internal counter name</param>
			/// <returns>the counter</returns>
			public virtual Counters.Counter GetCounterForName(string name)
			{
				return FindCounter(name);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				realGroup.Write(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				realGroup.ReadFields(@in);
			}

			public virtual IEnumerator<Counters.Counter> GetEnumerator()
			{
				return realGroup.GetEnumerator();
			}

			public virtual string GetName()
			{
				return realGroup.GetName();
			}

			public virtual string GetDisplayName()
			{
				return realGroup.GetDisplayName();
			}

			public virtual void SetDisplayName(string displayName)
			{
				realGroup.SetDisplayName(displayName);
			}

			public virtual void AddCounter(Counters.Counter counter)
			{
				realGroup.AddCounter(counter);
			}

			public virtual Counters.Counter AddCounter(string name, string displayName, long 
				value)
			{
				return realGroup.AddCounter(name, displayName, value);
			}

			public virtual Counters.Counter FindCounter(string counterName, string displayName
				)
			{
				return realGroup.FindCounter(counterName, displayName);
			}

			public virtual Counters.Counter FindCounter(string counterName, bool create)
			{
				return realGroup.FindCounter(counterName, create);
			}

			public virtual Counters.Counter FindCounter(string counterName)
			{
				return realGroup.FindCounter(counterName);
			}

			public virtual int Size()
			{
				return realGroup.Size();
			}

			public virtual void IncrAllCounters(CounterGroupBase<Counters.Counter> rightGroup
				)
			{
				realGroup.IncrAllCounters(rightGroup);
			}

			public virtual CounterGroupBase<Counters.Counter> GetUnderlyingGroup()
			{
				return realGroup;
			}

			public override bool Equals(object genericRight)
			{
				lock (this)
				{
					if (genericRight is CounterGroupBase<object>)
					{
						CounterGroupBase<Counters.Counter> right = ((CounterGroupBase<Counters.Counter>)genericRight
							).GetUnderlyingGroup();
						return Iterators.ElementsEqual(GetEnumerator(), right.GetEnumerator());
					}
					return false;
				}
			}

			public override int GetHashCode()
			{
				return realGroup.GetHashCode();
			}
		}

		// All the group impls need this for legacy group interface
		internal static long GetCounterValue(CounterGroupBase<Counters.Counter> group, string
			 counterName)
		{
			Counters.Counter counter = group.FindCounter(counterName, false);
			if (counter != null)
			{
				return counter.GetValue();
			}
			return 0L;
		}

		private class GenericGroup : AbstractCounterGroup<Counters.Counter>
		{
			internal GenericGroup(string name, string displayName, Limits limits)
				: base(name, displayName, limits)
			{
			}

			// Mix the generic group implementation into the Group interface
			protected internal override Counters.Counter NewCounter(string counterName, string
				 displayName, long value)
			{
				return new Counters.Counter(new GenericCounter(counterName, displayName, value));
			}

			protected internal override Counters.Counter NewCounter()
			{
				return new Counters.Counter();
			}

			public override CounterGroupBase<Counters.Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		private class FrameworkGroupImpl<T> : FrameworkCounterGroup<T, Counters.Counter>
			where T : Enum<T>
		{
			internal FrameworkGroupImpl(Type cls)
				: base(cls)
			{
			}

			// Mix the framework group implementation into the Group interface
			protected internal override Counters.Counter NewCounter(T key)
			{
				return new Counters.Counter(new FrameworkCounterGroup.FrameworkCounter<T>(key, GetName
					()));
			}

			public override CounterGroupBase<Counters.Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		private class FSGroupImpl : FileSystemCounterGroup<Counters.Counter>
		{
			// Mix the file system counter group implementation into the Group interface
			protected internal override Counters.Counter NewCounter(string scheme, FileSystemCounter
				 key)
			{
				return new Counters.Counter(new FileSystemCounterGroup.FSCounter(scheme, key));
			}

			public override CounterGroupBase<Counters.Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		public override Counters.Counter FindCounter(string group, string name)
		{
			lock (this)
			{
				if (name.Equals("MAP_INPUT_BYTES"))
				{
					Log.Warn("Counter name MAP_INPUT_BYTES is deprecated. " + "Use FileInputFormatCounters as group name and "
						 + " BYTES_READ as counter name instead");
					return FindCounter(FileInputFormatCounter.BytesRead);
				}
				string newGroupKey = GetNewGroupKey(group);
				if (newGroupKey != null)
				{
					group = newGroupKey;
				}
				return GetGroup(group).GetCounterForName(name);
			}
		}

		/// <summary>Provide factory methods for counter group factory implementation.</summary>
		/// <remarks>
		/// Provide factory methods for counter group factory implementation.
		/// See also the GroupFactory in
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Counters">mapreduce.Counters</see>
		/// </remarks>
		internal class GroupFactory : CounterGroupFactory<Counters.Counter, Counters.Group
			>
		{
			protected internal override CounterGroupFactory.FrameworkGroupFactory<Counters.Group
				> NewFrameworkGroupFactory<T>()
			{
				System.Type cls = typeof(T);
				return new _FrameworkGroupFactory_492(cls);
			}

			private sealed class _FrameworkGroupFactory_492 : CounterGroupFactory.FrameworkGroupFactory
				<Counters.Group>
			{
				public _FrameworkGroupFactory_492(Type cls)
				{
					this.cls = cls;
				}

				public Counters.Group NewGroup(string name)
				{
					return new Counters.Group(new Counters.FrameworkGroupImpl<T>(cls));
				}

				private readonly Type cls;
			}

			// impl in this package
			protected internal override Counters.Group NewGenericGroup(string name, string displayName
				, Limits limits)
			{
				return new Counters.Group(new Counters.GenericGroup(name, displayName, limits));
			}

			protected internal override Counters.Group NewFileSystemGroup()
			{
				return new Counters.Group(new Counters.FSGroupImpl());
			}
		}

		private static readonly Counters.GroupFactory groupFactory = new Counters.GroupFactory
			();

		/// <summary>Find a counter by using strings</summary>
		/// <param name="group">the name of the group</param>
		/// <param name="id">the id of the counter within the group (0 to N-1)</param>
		/// <param name="name">the internal name of the counter</param>
		/// <returns>the counter for that name</returns>
		[System.ObsoleteAttribute(@"use FindCounter(string, string) instead")]
		public virtual Counters.Counter FindCounter(string group, int id, string name)
		{
			return FindCounter(group, name);
		}

		/// <summary>
		/// Increments the specified counter by the specified amount, creating it if
		/// it didn't already exist.
		/// </summary>
		/// <param name="key">identifies a counter</param>
		/// <param name="amount">amount by which counter is to be incremented</param>
		public virtual void IncrCounter<_T0>(Enum<_T0> key, long amount)
			where _T0 : Enum<E>
		{
			FindCounter(key).Increment(amount);
		}

		/// <summary>
		/// Increments the specified counter by the specified amount, creating it if
		/// it didn't already exist.
		/// </summary>
		/// <param name="group">the name of the group</param>
		/// <param name="counter">the internal name of the counter</param>
		/// <param name="amount">amount by which counter is to be incremented</param>
		public virtual void IncrCounter(string group, string counter, long amount)
		{
			FindCounter(group, counter).Increment(amount);
		}

		/// <summary>
		/// Returns current value of the specified counter, or 0 if the counter
		/// does not exist.
		/// </summary>
		/// <param name="key">the counter enum to lookup</param>
		/// <returns>the counter value or 0 if counter not found</returns>
		public virtual long GetCounter<_T0>(Enum<_T0> key)
			where _T0 : Enum<E>
		{
			lock (this)
			{
				return FindCounter(key).GetValue();
			}
		}

		/// <summary>
		/// Increments multiple counters by their amounts in another Counters
		/// instance.
		/// </summary>
		/// <param name="other">the other Counters instance</param>
		public virtual void IncrAllCounters(Org.Apache.Hadoop.Mapred.Counters other)
		{
			lock (this)
			{
				foreach (Counters.Group otherGroup in other)
				{
					Counters.Group group = GetGroup(otherGroup.GetName());
					group.SetDisplayName(otherGroup.GetDisplayName());
					foreach (Counters.Counter otherCounter in otherGroup)
					{
						Counters.Counter counter = group.GetCounterForName(otherCounter.GetName());
						counter.SetDisplayName(otherCounter.GetDisplayName());
						counter.Increment(otherCounter.GetValue());
					}
				}
			}
		}

		/// <returns>the total number of counters</returns>
		[System.ObsoleteAttribute(@"use Org.Apache.Hadoop.Mapreduce.Counters.AbstractCounters{C, G}.CountCounters() instead"
			)]
		public virtual int Size()
		{
			return CountCounters();
		}

		/// <summary>Convenience method for computing the sum of two sets of counters.</summary>
		/// <param name="a">the first counters</param>
		/// <param name="b">the second counters</param>
		/// <returns>a new summed counters object</returns>
		public static Org.Apache.Hadoop.Mapred.Counters Sum(Org.Apache.Hadoop.Mapred.Counters
			 a, Org.Apache.Hadoop.Mapred.Counters b)
		{
			Org.Apache.Hadoop.Mapred.Counters counters = new Org.Apache.Hadoop.Mapred.Counters
				();
			counters.IncrAllCounters(a);
			counters.IncrAllCounters(b);
			return counters;
		}

		/// <summary>Logs the current counter values.</summary>
		/// <param name="log">The log to use.</param>
		public virtual void Log(Log log)
		{
			log.Info("Counters: " + Size());
			foreach (Counters.Group group in this)
			{
				log.Info("  " + group.GetDisplayName());
				foreach (Counters.Counter counter in group)
				{
					log.Info("    " + counter.GetDisplayName() + "=" + counter.GetCounter());
				}
			}
		}

		/// <summary>
		/// Represent the counter in a textual format that can be converted back to
		/// its object form
		/// </summary>
		/// <returns>
		/// the string in the following format
		/// {(groupName)(group-displayName)[(counterName)(displayName)(value)][]*}
		/// </returns>
		public virtual string MakeEscapedCompactString()
		{
			return CountersStrings.ToEscapedCompactString(this);
		}

		/// <summary>
		/// Convert a stringified (by
		/// <see cref="MakeEscapedCompactString()"/>
		/// counter
		/// representation into a counter object.
		/// </summary>
		/// <param name="compactString">to parse</param>
		/// <returns>a new counters object</returns>
		/// <exception cref="Sharpen.ParseException"/>
		public static Org.Apache.Hadoop.Mapred.Counters FromEscapedCompactString(string compactString
			)
		{
			return CountersStrings.ParseEscapedCompactString(compactString, new Org.Apache.Hadoop.Mapred.Counters
				());
		}

		/// <summary>Counter exception thrown when the number of counters exceed the limit</summary>
		[System.Serializable]
		public class CountersExceededException : RuntimeException
		{
			private const long serialVersionUID = 1L;

			public CountersExceededException(string msg)
				: base(msg)
			{
			}

			public CountersExceededException(Counters.CountersExceededException cause)
				: base(cause)
			{
			}
			// Only allows chaining of related exceptions
		}
	}
}
