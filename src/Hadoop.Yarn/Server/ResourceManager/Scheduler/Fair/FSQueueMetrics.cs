using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FSQueueMetrics : QueueMetrics
	{
		internal MutableGaugeInt fairShareMB;

		internal MutableGaugeInt fairShareVCores;

		internal MutableGaugeInt steadyFairShareMB;

		internal MutableGaugeInt steadyFairShareVCores;

		internal MutableGaugeInt minShareMB;

		internal MutableGaugeInt minShareVCores;

		internal MutableGaugeInt maxShareMB;

		internal MutableGaugeInt maxShareVCores;

		internal FSQueueMetrics(MetricsSystem ms, string queueName, Queue parent, bool enableUserMetrics
			, Configuration conf)
			: base(ms, queueName, parent, enableUserMetrics, conf)
		{
		}

		public virtual void SetFairShare(Resource resource)
		{
			fairShareMB.Set(resource.GetMemory());
			fairShareVCores.Set(resource.GetVirtualCores());
		}

		public virtual int GetFairShareMB()
		{
			return fairShareMB.Value();
		}

		public virtual int GetFairShareVirtualCores()
		{
			return fairShareVCores.Value();
		}

		public virtual void SetSteadyFairShare(Resource resource)
		{
			steadyFairShareMB.Set(resource.GetMemory());
			steadyFairShareVCores.Set(resource.GetVirtualCores());
		}

		public virtual int GetSteadyFairShareMB()
		{
			return steadyFairShareMB.Value();
		}

		public virtual int GetSteadyFairShareVCores()
		{
			return steadyFairShareVCores.Value();
		}

		public virtual void SetMinShare(Resource resource)
		{
			minShareMB.Set(resource.GetMemory());
			minShareVCores.Set(resource.GetVirtualCores());
		}

		public virtual int GetMinShareMB()
		{
			return minShareMB.Value();
		}

		public virtual int GetMinShareVirtualCores()
		{
			return minShareVCores.Value();
		}

		public virtual void SetMaxShare(Resource resource)
		{
			maxShareMB.Set(resource.GetMemory());
			maxShareVCores.Set(resource.GetVirtualCores());
		}

		public virtual int GetMaxShareMB()
		{
			return maxShareMB.Value();
		}

		public virtual int GetMaxShareVirtualCores()
		{
			return maxShareVCores.Value();
		}

		public static QueueMetrics ForQueue(string queueName, Queue parent, bool enableUserMetrics
			, Configuration conf)
		{
			lock (typeof(FSQueueMetrics))
			{
				MetricsSystem ms = DefaultMetricsSystem.Instance();
				QueueMetrics metrics = queueMetrics[queueName];
				if (metrics == null)
				{
					metrics = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSQueueMetrics
						(ms, queueName, parent, enableUserMetrics, conf).Tag(QueueInfo, queueName);
					// Register with the MetricsSystems
					if (ms != null)
					{
						metrics = ms.Register(SourceName(queueName).ToString(), "Metrics for queue: " + queueName
							, metrics);
					}
					queueMetrics[queueName] = metrics;
				}
				return (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSQueueMetrics
					)metrics;
			}
		}
	}
}
