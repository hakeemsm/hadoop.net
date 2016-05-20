using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>The default metrics system singleton</summary>
	[System.Serializable]
	public sealed class DefaultMetricsSystem
	{
		public static readonly org.apache.hadoop.metrics2.lib.DefaultMetricsSystem INSTANCE
			 = new org.apache.hadoop.metrics2.lib.DefaultMetricsSystem();

		private java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.metrics2.MetricsSystem
			> impl = new java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.metrics2.MetricsSystem
			>(new org.apache.hadoop.metrics2.impl.MetricsSystemImpl());

		internal volatile bool miniClusterMode = false;

		[System.NonSerialized]
		internal readonly org.apache.hadoop.metrics2.lib.UniqueNames mBeanNames = new org.apache.hadoop.metrics2.lib.UniqueNames
			();

		[System.NonSerialized]
		internal readonly org.apache.hadoop.metrics2.lib.UniqueNames sourceNames = new org.apache.hadoop.metrics2.lib.UniqueNames
			();

		// the singleton
		/// <summary>Convenience method to initialize the metrics system</summary>
		/// <param name="prefix">for the metrics system configuration</param>
		/// <returns>the metrics system instance</returns>
		public static org.apache.hadoop.metrics2.MetricsSystem initialize(string prefix)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.init(prefix);
		}

		internal org.apache.hadoop.metrics2.MetricsSystem init(string prefix)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.impl.get().init(prefix
				);
		}

		/// <returns>the metrics system object</returns>
		public static org.apache.hadoop.metrics2.MetricsSystem instance()
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.getImpl();
		}

		/// <summary>Shutdown the metrics system</summary>
		public static void shutdown()
		{
			org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.shutdownInstance();
		}

		internal void shutdownInstance()
		{
			bool last = org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.impl.get().shutdown
				();
			if (last)
			{
				lock (this)
				{
					org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.mBeanNames.map.clear();
					org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.sourceNames.map.clear();
				}
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static org.apache.hadoop.metrics2.MetricsSystem setInstance(org.apache.hadoop.metrics2.MetricsSystem
			 ms)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.setImpl(ms);
		}

		internal org.apache.hadoop.metrics2.MetricsSystem setImpl(org.apache.hadoop.metrics2.MetricsSystem
			 ms)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.impl.getAndSet(ms);
		}

		internal org.apache.hadoop.metrics2.MetricsSystem getImpl()
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.impl.get();
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static void setMiniClusterMode(bool choice)
		{
			org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.miniClusterMode = choice;
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static bool inMiniClusterMode()
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.miniClusterMode;
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static javax.management.ObjectName newMBeanName(string name)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.newObjectName
				(name);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static void removeMBeanName(javax.management.ObjectName name)
		{
			org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.removeObjectName(name
				.ToString());
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static string sourceName(string name, bool dupOK)
		{
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.INSTANCE.newSourceName
				(name, dupOK);
		}

		internal javax.management.ObjectName newObjectName(string name)
		{
			lock (this)
			{
				try
				{
					if (org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.mBeanNames.map.Contains(name
						) && !org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.miniClusterMode)
					{
						throw new org.apache.hadoop.metrics2.MetricsException(name + " already exists!");
					}
					return new javax.management.ObjectName(org.apache.hadoop.metrics2.lib.DefaultMetricsSystem
						.mBeanNames.uniqueName(name));
				}
				catch (System.Exception e)
				{
					throw new org.apache.hadoop.metrics2.MetricsException(e);
				}
			}
		}

		internal void removeObjectName(string name)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.mBeanNames
					.map, name);
			}
		}

		internal string newSourceName(string name, bool dupOK)
		{
			lock (this)
			{
				if (org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.sourceNames.map.Contains(
					name))
				{
					if (dupOK)
					{
						return name;
					}
					else
					{
						if (!org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.miniClusterMode)
						{
							throw new org.apache.hadoop.metrics2.MetricsException("Metrics source " + name + 
								" already exists!");
						}
					}
				}
				return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.sourceNames.uniqueName
					(name);
			}
		}
	}
}
