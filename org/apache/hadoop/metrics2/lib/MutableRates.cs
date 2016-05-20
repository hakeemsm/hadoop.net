using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Helper class to manage a group of mutable rate metrics</summary>
	public class MutableRates : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableRates
			)));

		private readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry;

		private readonly System.Collections.Generic.ICollection<java.lang.Class> protocolCache
			 = com.google.common.collect.Sets.newHashSet();

		internal MutableRates(org.apache.hadoop.metrics2.lib.MetricsRegistry registry)
		{
			this.registry = com.google.common.@base.Preconditions.checkNotNull(registry, "metrics registry"
				);
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
		public virtual void init(java.lang.Class protocol)
		{
			if (protocolCache.contains(protocol))
			{
				return;
			}
			protocolCache.add(protocol);
			foreach (java.lang.reflect.Method method in protocol.getDeclaredMethods())
			{
				string name = method.getName();
				LOG.debug(name);
				try
				{
					registry.newRate(name, name, false, true);
				}
				catch (System.Exception e)
				{
					LOG.error("Error creating rate metrics for " + method.getName(), e);
				}
			}
		}

		/// <summary>Add a rate sample for a rate metric</summary>
		/// <param name="name">of the rate metric</param>
		/// <param name="elapsed">time</param>
		public virtual void add(string name, long elapsed)
		{
			registry.add(name, elapsed);
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder rb, 
			bool all)
		{
			registry.snapshot(rb, all);
		}
	}
}
