using Sharpen;

namespace org.apache.hadoop.ipc.metrics
{
	/// <summary>
	/// This class is for maintaining  the various RPC statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RpcMetrics
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.metrics.RpcMetrics
			)));

		internal readonly org.apache.hadoop.ipc.Server server;

		internal readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry;

		internal readonly string name;

		internal readonly bool rpcQuantileEnable;

		internal RpcMetrics(org.apache.hadoop.ipc.Server server, org.apache.hadoop.conf.Configuration
			 conf)
		{
			string port = Sharpen.Runtime.getStringValueOf(server.getListenerAddress().getPort
				());
			name = "RpcActivityForPort" + port;
			this.server = server;
			registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry("rpc").tag("port", 
				"RPC port", port);
			int[] intervals = conf.getInts(org.apache.hadoop.fs.CommonConfigurationKeys.RPC_METRICS_PERCENTILES_INTERVALS_KEY
				);
			rpcQuantileEnable = (intervals.Length > 0) && conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys
				.RPC_METRICS_QUANTILE_ENABLE, org.apache.hadoop.fs.CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE_DEFAULT
				);
			if (rpcQuantileEnable)
			{
				rpcQueueTimeMillisQuantiles = new org.apache.hadoop.metrics2.lib.MutableQuantiles
					[intervals.Length];
				rpcProcessingTimeMillisQuantiles = new org.apache.hadoop.metrics2.lib.MutableQuantiles
					[intervals.Length];
				for (int i = 0; i < intervals.Length; i++)
				{
					int interval = intervals[i];
					rpcQueueTimeMillisQuantiles[i] = registry.newQuantiles("rpcQueueTime" + interval 
						+ "s", "rpc queue time in milli second", "ops", "latency", interval);
					rpcProcessingTimeMillisQuantiles[i] = registry.newQuantiles("rpcProcessingTime" +
						 interval + "s", "rpc processing time in milli second", "ops", "latency", interval
						);
				}
			}
			LOG.debug("Initialized " + registry);
		}

		public virtual string name()
		{
			return name;
		}

		public static org.apache.hadoop.ipc.metrics.RpcMetrics create(org.apache.hadoop.ipc.Server
			 server, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.ipc.metrics.RpcMetrics m = new org.apache.hadoop.ipc.metrics.RpcMetrics
				(server, conf);
			return org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.instance().register(m.
				name, null, m);
		}

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong receivedBytes;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong sentBytes;

		internal org.apache.hadoop.metrics2.lib.MutableRate rpcQueueTime;

		internal org.apache.hadoop.metrics2.lib.MutableQuantiles[] rpcQueueTimeMillisQuantiles;

		internal org.apache.hadoop.metrics2.lib.MutableRate rpcProcessingTime;

		internal org.apache.hadoop.metrics2.lib.MutableQuantiles[] rpcProcessingTimeMillisQuantiles;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong rpcAuthenticationFailures;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong rpcAuthenticationSuccesses;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong rpcAuthorizationFailures;

		internal org.apache.hadoop.metrics2.lib.MutableCounterLong rpcAuthorizationSuccesses;

		public virtual int numOpenConnections()
		{
			return server.getNumOpenConnections();
		}

		public virtual int callQueueLength()
		{
			return server.getCallQueueLen();
		}

		// Public instrumentation methods that could be extracted to an
		// abstract class if we decide to do custom instrumentation classes a la
		// JobTrackerInstrumenation. The methods with //@Override comment are
		// candidates for abstract methods in a abstract instrumentation class.
		/// <summary>One authentication failure event</summary>
		public virtual void incrAuthenticationFailures()
		{
			//@Override
			rpcAuthenticationFailures.incr();
		}

		/// <summary>One authentication success event</summary>
		public virtual void incrAuthenticationSuccesses()
		{
			//@Override
			rpcAuthenticationSuccesses.incr();
		}

		/// <summary>One authorization success event</summary>
		public virtual void incrAuthorizationSuccesses()
		{
			//@Override
			rpcAuthorizationSuccesses.incr();
		}

		/// <summary>One authorization failure event</summary>
		public virtual void incrAuthorizationFailures()
		{
			//@Override
			rpcAuthorizationFailures.incr();
		}

		/// <summary>Shutdown the instrumentation for the process</summary>
		public virtual void shutdown()
		{
		}

		//@Override
		/// <summary>Increment sent bytes by count</summary>
		/// <param name="count">to increment</param>
		public virtual void incrSentBytes(int count)
		{
			//@Override
			sentBytes.incr(count);
		}

		/// <summary>Increment received bytes by count</summary>
		/// <param name="count">to increment</param>
		public virtual void incrReceivedBytes(int count)
		{
			//@Override
			receivedBytes.incr(count);
		}

		/// <summary>Add an RPC queue time sample</summary>
		/// <param name="qTime">the queue time</param>
		public virtual void addRpcQueueTime(int qTime)
		{
			//@Override
			rpcQueueTime.add(qTime);
			if (rpcQuantileEnable)
			{
				foreach (org.apache.hadoop.metrics2.lib.MutableQuantiles q in rpcQueueTimeMillisQuantiles)
				{
					q.add(qTime);
				}
			}
		}

		/// <summary>Add an RPC processing time sample</summary>
		/// <param name="processingTime">the processing time</param>
		public virtual void addRpcProcessingTime(int processingTime)
		{
			//@Override
			rpcProcessingTime.add(processingTime);
			if (rpcQuantileEnable)
			{
				foreach (org.apache.hadoop.metrics2.lib.MutableQuantiles q in rpcProcessingTimeMillisQuantiles)
				{
					q.add(processingTime);
				}
			}
		}
	}
}
