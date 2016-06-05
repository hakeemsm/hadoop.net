using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;


namespace Org.Apache.Hadoop.Ipc.Metrics
{
	/// <summary>
	/// This class is for maintaining  the various RPC statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class RpcMetrics
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.Metrics.RpcMetrics
			));

		internal readonly Server server;

		internal readonly MetricsRegistry registry;

		internal readonly string name;

		internal readonly bool rpcQuantileEnable;

		internal RpcMetrics(Server server, Configuration conf)
		{
			string port = server.GetListenerAddress().Port.ToString();
			name = "RpcActivityForPort" + port;
			this.server = server;
			registry = new MetricsRegistry("rpc").Tag("port", "RPC port", port);
			int[] intervals = conf.GetInts(CommonConfigurationKeys.RpcMetricsPercentilesIntervalsKey
				);
			rpcQuantileEnable = (intervals.Length > 0) && conf.GetBoolean(CommonConfigurationKeys
				.RpcMetricsQuantileEnable, CommonConfigurationKeys.RpcMetricsQuantileEnableDefault
				);
			if (rpcQuantileEnable)
			{
				rpcQueueTimeMillisQuantiles = new MutableQuantiles[intervals.Length];
				rpcProcessingTimeMillisQuantiles = new MutableQuantiles[intervals.Length];
				for (int i = 0; i < intervals.Length; i++)
				{
					int interval = intervals[i];
					rpcQueueTimeMillisQuantiles[i] = registry.NewQuantiles("rpcQueueTime" + interval 
						+ "s", "rpc queue time in milli second", "ops", "latency", interval);
					rpcProcessingTimeMillisQuantiles[i] = registry.NewQuantiles("rpcProcessingTime" +
						 interval + "s", "rpc processing time in milli second", "ops", "latency", interval
						);
				}
			}
			Log.Debug("Initialized " + registry);
		}

		public virtual string Name()
		{
			return name;
		}

		public static Org.Apache.Hadoop.Ipc.Metrics.RpcMetrics Create(Server server, Configuration
			 conf)
		{
			Org.Apache.Hadoop.Ipc.Metrics.RpcMetrics m = new Org.Apache.Hadoop.Ipc.Metrics.RpcMetrics
				(server, conf);
			return DefaultMetricsSystem.Instance().Register(m.name, null, m);
		}

		internal MutableCounterLong receivedBytes;

		internal MutableCounterLong sentBytes;

		internal MutableRate rpcQueueTime;

		internal MutableQuantiles[] rpcQueueTimeMillisQuantiles;

		internal MutableRate rpcProcessingTime;

		internal MutableQuantiles[] rpcProcessingTimeMillisQuantiles;

		internal MutableCounterLong rpcAuthenticationFailures;

		internal MutableCounterLong rpcAuthenticationSuccesses;

		internal MutableCounterLong rpcAuthorizationFailures;

		internal MutableCounterLong rpcAuthorizationSuccesses;

		public virtual int NumOpenConnections()
		{
			return server.GetNumOpenConnections();
		}

		public virtual int CallQueueLength()
		{
			return server.GetCallQueueLen();
		}

		// Public instrumentation methods that could be extracted to an
		// abstract class if we decide to do custom instrumentation classes a la
		// JobTrackerInstrumenation. The methods with //@Override comment are
		// candidates for abstract methods in a abstract instrumentation class.
		/// <summary>One authentication failure event</summary>
		public virtual void IncrAuthenticationFailures()
		{
			//@Override
			rpcAuthenticationFailures.Incr();
		}

		/// <summary>One authentication success event</summary>
		public virtual void IncrAuthenticationSuccesses()
		{
			//@Override
			rpcAuthenticationSuccesses.Incr();
		}

		/// <summary>One authorization success event</summary>
		public virtual void IncrAuthorizationSuccesses()
		{
			//@Override
			rpcAuthorizationSuccesses.Incr();
		}

		/// <summary>One authorization failure event</summary>
		public virtual void IncrAuthorizationFailures()
		{
			//@Override
			rpcAuthorizationFailures.Incr();
		}

		/// <summary>Shutdown the instrumentation for the process</summary>
		public virtual void Shutdown()
		{
		}

		//@Override
		/// <summary>Increment sent bytes by count</summary>
		/// <param name="count">to increment</param>
		public virtual void IncrSentBytes(int count)
		{
			//@Override
			sentBytes.Incr(count);
		}

		/// <summary>Increment received bytes by count</summary>
		/// <param name="count">to increment</param>
		public virtual void IncrReceivedBytes(int count)
		{
			//@Override
			receivedBytes.Incr(count);
		}

		/// <summary>Add an RPC queue time sample</summary>
		/// <param name="qTime">the queue time</param>
		public virtual void AddRpcQueueTime(int qTime)
		{
			//@Override
			rpcQueueTime.Add(qTime);
			if (rpcQuantileEnable)
			{
				foreach (MutableQuantiles q in rpcQueueTimeMillisQuantiles)
				{
					q.Add(qTime);
				}
			}
		}

		/// <summary>Add an RPC processing time sample</summary>
		/// <param name="processingTime">the processing time</param>
		public virtual void AddRpcProcessingTime(int processingTime)
		{
			//@Override
			rpcProcessingTime.Add(processingTime);
			if (rpcQuantileEnable)
			{
				foreach (MutableQuantiles q in rpcProcessingTimeMillisQuantiles)
				{
					q.Add(processingTime);
				}
			}
		}
	}
}
