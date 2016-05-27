using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc.Metrics
{
	/// <summary>
	/// This class is for maintaining RPC method related statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RpcDetailedMetrics
	{
		[Metric]
		internal MutableRates rates;

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.Metrics.RpcDetailedMetrics
			));

		internal readonly MetricsRegistry registry;

		internal readonly string name;

		internal RpcDetailedMetrics(int port)
		{
			name = "RpcDetailedActivityForPort" + port;
			registry = new MetricsRegistry("rpcdetailed").Tag("port", "RPC port", port.ToString
				());
			Log.Debug(registry.Info());
		}

		public virtual string Name()
		{
			return name;
		}

		public static Org.Apache.Hadoop.Ipc.Metrics.RpcDetailedMetrics Create(int port)
		{
			Org.Apache.Hadoop.Ipc.Metrics.RpcDetailedMetrics m = new Org.Apache.Hadoop.Ipc.Metrics.RpcDetailedMetrics
				(port);
			return DefaultMetricsSystem.Instance().Register(m.name, null, m);
		}

		/// <summary>Initialize the metrics for JMX with protocol methods</summary>
		/// <param name="protocol">the protocol class</param>
		public virtual void Init(Type protocol)
		{
			rates.Init(protocol);
		}

		/// <summary>Add an RPC processing time sample</summary>
		/// <param name="name">of the RPC call</param>
		/// <param name="processingTime">the processing time</param>
		public virtual void AddProcessingTime(string name, int processingTime)
		{
			//@Override // some instrumentation interface
			rates.Add(name, processingTime);
		}

		/// <summary>Shutdown the instrumentation for the process</summary>
		public virtual void Shutdown()
		{
		}
		//@Override // some instrumentation interface
	}
}
