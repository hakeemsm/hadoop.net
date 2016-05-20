using Sharpen;

namespace org.apache.hadoop.ipc.metrics
{
	/// <summary>
	/// This class is for maintaining RPC method related statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RpcDetailedMetrics
	{
		[org.apache.hadoop.metrics2.annotation.Metric]
		internal org.apache.hadoop.metrics2.lib.MutableRates rates;

		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.metrics.RpcDetailedMetrics
			)));

		internal readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry;

		internal readonly string name;

		internal RpcDetailedMetrics(int port)
		{
			name = "RpcDetailedActivityForPort" + port;
			registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry("rpcdetailed").tag(
				"port", "RPC port", Sharpen.Runtime.getStringValueOf(port));
			LOG.debug(registry.info());
		}

		public virtual string name()
		{
			return name;
		}

		public static org.apache.hadoop.ipc.metrics.RpcDetailedMetrics create(int port)
		{
			org.apache.hadoop.ipc.metrics.RpcDetailedMetrics m = new org.apache.hadoop.ipc.metrics.RpcDetailedMetrics
				(port);
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.instance().register(m.
				name, null, m);
		}

		/// <summary>Initialize the metrics for JMX with protocol methods</summary>
		/// <param name="protocol">the protocol class</param>
		public virtual void init(java.lang.Class protocol)
		{
			rates.init(protocol);
		}

		/// <summary>Add an RPC processing time sample</summary>
		/// <param name="name">of the RPC call</param>
		/// <param name="processingTime">the processing time</param>
		public virtual void addProcessingTime(string name, int processingTime)
		{
			//@Override // some instrumentation interface
			rates.add(name, processingTime);
		}

		/// <summary>Shutdown the instrumentation for the process</summary>
		public virtual void shutdown()
		{
		}
		//@Override // some instrumentation interface
	}
}
