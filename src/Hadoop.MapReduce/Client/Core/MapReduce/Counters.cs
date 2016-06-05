using System;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <p><code>Counters</code> holds per job/task counters, defined either by the
	/// Map-Reduce framework or applications.
	/// </summary>
	/// <remarks>
	/// <p><code>Counters</code> holds per job/task counters, defined either by the
	/// Map-Reduce framework or applications. Each <code>Counter</code> can be of
	/// any
	/// <see cref="Sharpen.Enum{E}"/>
	/// type.</p>
	/// <p><code>Counters</code> are bunched into
	/// <see cref="CounterGroup"/>
	/// s, each
	/// comprising of counters from a particular <code>Enum</code> class.
	/// </remarks>
	public class Counters : AbstractCounters<Counter, CounterGroup>
	{
		private class FrameworkGroupImpl<T> : FrameworkCounterGroup<T, Counter>, CounterGroup
			where T : Enum<T>
		{
			internal FrameworkGroupImpl(Type cls)
				: base(cls)
			{
			}

			// Mix framework group implementation into CounterGroup interface
			protected internal override Counter NewCounter(T key)
			{
				return new FrameworkCounterGroup.FrameworkCounter<T>(key, GetName());
			}

			public override CounterGroupBase<Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		private class GenericGroup : AbstractCounterGroup<Counter>, CounterGroup
		{
			internal GenericGroup(string name, string displayName, Limits limits)
				: base(name, displayName, limits)
			{
			}

			// Mix generic group implementation into CounterGroup interface
			// and provide some mandatory group factory methods.
			protected internal override Counter NewCounter(string name, string displayName, long
				 value)
			{
				return new GenericCounter(name, displayName, value);
			}

			protected internal override Counter NewCounter()
			{
				return new GenericCounter();
			}

			public override CounterGroupBase<Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		private class FileSystemGroup : FileSystemCounterGroup<Counter>, CounterGroup
		{
			// Mix file system group implementation into the CounterGroup interface
			protected internal override Counter NewCounter(string scheme, FileSystemCounter key
				)
			{
				return new FileSystemCounterGroup.FSCounter(scheme, key);
			}

			public override CounterGroupBase<Counter> GetUnderlyingGroup()
			{
				return this;
			}
		}

		/// <summary>Provide factory methods for counter group factory implementation.</summary>
		/// <remarks>
		/// Provide factory methods for counter group factory implementation.
		/// See also the GroupFactory in
		/// <see cref="Org.Apache.Hadoop.Mapred.Counters">mapred.Counters</see>
		/// </remarks>
		private class GroupFactory : CounterGroupFactory<Counter, CounterGroup>
		{
			protected internal override CounterGroupFactory.FrameworkGroupFactory<CounterGroup
				> NewFrameworkGroupFactory<T>()
			{
				System.Type cls = typeof(T);
				return new _FrameworkGroupFactory_114(cls);
			}

			private sealed class _FrameworkGroupFactory_114 : CounterGroupFactory.FrameworkGroupFactory
				<CounterGroup>
			{
				public _FrameworkGroupFactory_114(Type cls)
				{
					this.cls = cls;
				}

				public CounterGroup NewGroup(string name)
				{
					return new Counters.FrameworkGroupImpl<T>(cls);
				}

				private readonly Type cls;
			}

			// impl in this package
			protected internal override CounterGroup NewGenericGroup(string name, string displayName
				, Limits limits)
			{
				return new Counters.GenericGroup(name, displayName, limits);
			}

			protected internal override CounterGroup NewFileSystemGroup()
			{
				return new Counters.FileSystemGroup();
			}
		}

		private static readonly Counters.GroupFactory groupFactory = new Counters.GroupFactory
			();

		/// <summary>Default constructor</summary>
		public Counters()
			: base(groupFactory)
		{
		}

		/// <summary>Construct the Counters object from the another counters object</summary>
		/// <?/>
		/// <?/>
		/// <param name="counters">the old counters object</param>
		public Counters(AbstractCounters<C, G> counters)
			: base(counters, groupFactory)
		{
		}
	}
}
