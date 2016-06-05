using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class ClusterMetrics
	{
		private static AtomicBoolean isInitialized = new AtomicBoolean(false);

		internal MutableGaugeInt numActiveNMs;

		internal MutableGaugeInt numDecommissionedNMs;

		internal MutableGaugeInt numLostNMs;

		internal MutableGaugeInt numUnhealthyNMs;

		internal MutableGaugeInt numRebootedNMs;

		internal MutableRate aMLaunchDelay;

		internal MutableRate aMRegisterDelay;

		private static readonly MetricsInfo RecordInfo = Interns.Info("ClusterMetrics", "Metrics for the Yarn Cluster"
			);

		private static volatile ClusterMetrics Instance = null;

		private static MetricsRegistry registry;

		public static ClusterMetrics GetMetrics()
		{
			if (!isInitialized.Get())
			{
				lock (typeof(ClusterMetrics))
				{
					if (Instance == null)
					{
						Instance = new ClusterMetrics();
						RegisterMetrics();
						isInitialized.Set(true);
					}
				}
			}
			return Instance;
		}

		private static void RegisterMetrics()
		{
			registry = new MetricsRegistry(RecordInfo);
			registry.Tag(RecordInfo, "ResourceManager");
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			if (ms != null)
			{
				ms.Register("ClusterMetrics", "Metrics for the Yarn Cluster", Instance);
			}
		}

		[VisibleForTesting]
		internal static void Destroy()
		{
			lock (typeof(ClusterMetrics))
			{
				isInitialized.Set(false);
				Instance = null;
			}
		}

		//Active Nodemanagers
		public virtual int GetNumActiveNMs()
		{
			return numActiveNMs.Value();
		}

		//Decommisioned NMs
		public virtual int GetNumDecommisionedNMs()
		{
			return numDecommissionedNMs.Value();
		}

		public virtual void IncrDecommisionedNMs()
		{
			numDecommissionedNMs.Incr();
		}

		public virtual void SetDecommisionedNMs(int num)
		{
			numDecommissionedNMs.Set(num);
		}

		public virtual void DecrDecommisionedNMs()
		{
			numDecommissionedNMs.Decr();
		}

		//Lost NMs
		public virtual int GetNumLostNMs()
		{
			return numLostNMs.Value();
		}

		public virtual void IncrNumLostNMs()
		{
			numLostNMs.Incr();
		}

		public virtual void DecrNumLostNMs()
		{
			numLostNMs.Decr();
		}

		//Unhealthy NMs
		public virtual int GetUnhealthyNMs()
		{
			return numUnhealthyNMs.Value();
		}

		public virtual void IncrNumUnhealthyNMs()
		{
			numUnhealthyNMs.Incr();
		}

		public virtual void DecrNumUnhealthyNMs()
		{
			numUnhealthyNMs.Decr();
		}

		//Rebooted NMs
		public virtual int GetNumRebootedNMs()
		{
			return numRebootedNMs.Value();
		}

		public virtual void IncrNumRebootedNMs()
		{
			numRebootedNMs.Incr();
		}

		public virtual void DecrNumRebootedNMs()
		{
			numRebootedNMs.Decr();
		}

		public virtual void IncrNumActiveNodes()
		{
			numActiveNMs.Incr();
		}

		public virtual void DecrNumActiveNodes()
		{
			numActiveNMs.Decr();
		}

		public virtual void AddAMLaunchDelay(long delay)
		{
			aMLaunchDelay.Add(delay);
		}

		public virtual void AddAMRegisterDelay(long delay)
		{
			aMRegisterDelay.Add(delay);
		}
	}
}
