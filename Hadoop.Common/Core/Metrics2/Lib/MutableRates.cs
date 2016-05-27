using System;
using System.Collections.Generic;
using System.Reflection;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Helper class to manage a group of mutable rate metrics</summary>
	public class MutableRates : MutableMetric
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Lib.MutableRates
			));

		private readonly MetricsRegistry registry;

		private readonly ICollection<Type> protocolCache = Sets.NewHashSet();

		internal MutableRates(MetricsRegistry registry)
		{
			this.registry = Preconditions.CheckNotNull(registry, "metrics registry");
		}

		/// <summary>
		/// Initialize the registry with all the methods in a protocol
		/// so they all show up in the first snapshot.
		/// </summary>
		/// <remarks>
		/// Initialize the registry with all the methods in a protocol
		/// so they all show up in the first snapshot.
		/// Convenient for JMX implementations.
		/// </remarks>
		/// <param name="protocol">the protocol class</param>
		public virtual void Init(Type protocol)
		{
			if (protocolCache.Contains(protocol))
			{
				return;
			}
			protocolCache.AddItem(protocol);
			foreach (MethodInfo method in Sharpen.Runtime.GetDeclaredMethods(protocol))
			{
				string name = method.Name;
				Log.Debug(name);
				try
				{
					registry.NewRate(name, name, false, true);
				}
				catch (Exception e)
				{
					Log.Error("Error creating rate metrics for " + method.Name, e);
				}
			}
		}

		/// <summary>Add a rate sample for a rate metric</summary>
		/// <param name="name">of the rate metric</param>
		/// <param name="elapsed">time</param>
		public virtual void Add(string name, long elapsed)
		{
			registry.Add(name, elapsed);
		}

		public override void Snapshot(MetricsRecordBuilder rb, bool all)
		{
			registry.Snapshot(rb, all);
		}
	}
}
