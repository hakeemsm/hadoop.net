using System;
using Javax.Management;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>The default metrics system singleton</summary>
	[System.Serializable]
	public sealed class DefaultMetricsSystem
	{
		public static readonly DefaultMetricsSystem Instance = new DefaultMetricsSystem();

		private AtomicReference<MetricsSystem> impl = new AtomicReference<MetricsSystem>(
			new MetricsSystemImpl());

		internal volatile bool miniClusterMode = false;

		[System.NonSerialized]
		internal readonly UniqueNames mBeanNames = new UniqueNames();

		[System.NonSerialized]
		internal readonly UniqueNames sourceNames = new UniqueNames();

		// the singleton
		/// <summary>Convenience method to initialize the metrics system</summary>
		/// <param name="prefix">for the metrics system configuration</param>
		/// <returns>the metrics system instance</returns>
		public static MetricsSystem Initialize(string prefix)
		{
			return DefaultMetricsSystem.Instance.Init(prefix);
		}

		internal MetricsSystem Init(string prefix)
		{
			return DefaultMetricsSystem.impl.Get().Init(prefix);
		}

		/// <returns>the metrics system object</returns>
		public static MetricsSystem Instance()
		{
			return DefaultMetricsSystem.Instance.GetImpl();
		}

		/// <summary>Shutdown the metrics system</summary>
		public static void Shutdown()
		{
			DefaultMetricsSystem.Instance.ShutdownInstance();
		}

		internal void ShutdownInstance()
		{
			bool last = DefaultMetricsSystem.impl.Get().Shutdown();
			if (last)
			{
				lock (this)
				{
					DefaultMetricsSystem.mBeanNames.map.Clear();
					DefaultMetricsSystem.sourceNames.map.Clear();
				}
			}
		}

		[InterfaceAudience.Private]
		public static MetricsSystem SetInstance(MetricsSystem ms)
		{
			return DefaultMetricsSystem.Instance.SetImpl(ms);
		}

		internal MetricsSystem SetImpl(MetricsSystem ms)
		{
			return DefaultMetricsSystem.impl.GetAndSet(ms);
		}

		internal MetricsSystem GetImpl()
		{
			return DefaultMetricsSystem.impl.Get();
		}

		[InterfaceAudience.Private]
		public static void SetMiniClusterMode(bool choice)
		{
			DefaultMetricsSystem.Instance.miniClusterMode = choice;
		}

		[InterfaceAudience.Private]
		public static bool InMiniClusterMode()
		{
			return DefaultMetricsSystem.Instance.miniClusterMode;
		}

		[InterfaceAudience.Private]
		public static ObjectName NewMBeanName(string name)
		{
			return DefaultMetricsSystem.Instance.NewObjectName(name);
		}

		[InterfaceAudience.Private]
		public static void RemoveMBeanName(ObjectName name)
		{
			DefaultMetricsSystem.Instance.RemoveObjectName(name.ToString());
		}

		[InterfaceAudience.Private]
		public static string SourceName(string name, bool dupOK)
		{
			return DefaultMetricsSystem.Instance.NewSourceName(name, dupOK);
		}

		internal ObjectName NewObjectName(string name)
		{
			lock (this)
			{
				try
				{
					if (DefaultMetricsSystem.mBeanNames.map.Contains(name) && !DefaultMetricsSystem.miniClusterMode)
					{
						throw new MetricsException(name + " already exists!");
					}
					return new ObjectName(DefaultMetricsSystem.mBeanNames.UniqueName(name));
				}
				catch (Exception e)
				{
					throw new MetricsException(e);
				}
			}
		}

		internal void RemoveObjectName(string name)
		{
			lock (this)
			{
				Collections.Remove(DefaultMetricsSystem.mBeanNames.map, name);
			}
		}

		internal string NewSourceName(string name, bool dupOK)
		{
			lock (this)
			{
				if (DefaultMetricsSystem.sourceNames.map.Contains(name))
				{
					if (dupOK)
					{
						return name;
					}
					else
					{
						if (!DefaultMetricsSystem.miniClusterMode)
						{
							throw new MetricsException("Metrics source " + name + " already exists!");
						}
					}
				}
				return DefaultMetricsSystem.sourceNames.UniqueName(name);
			}
		}
	}
}
