using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>The metrics for a journal from the writer's perspective.</summary>
	internal class IPCLoggerChannelMetrics
	{
		internal readonly MetricsRegistry registry = new MetricsRegistry("NameNode");

		private volatile IPCLoggerChannel ch;

		private readonly MutableQuantiles[] writeEndToEndLatencyQuantiles;

		private readonly MutableQuantiles[] writeRpcLatencyQuantiles;

		/// <summary>
		/// In the case of the NN transitioning between states, edit logs are closed
		/// and reopened.
		/// </summary>
		/// <remarks>
		/// In the case of the NN transitioning between states, edit logs are closed
		/// and reopened. Thus, the IPCLoggerChannel instance that writes to a
		/// given JournalNode may change over the lifetime of the process.
		/// However, metrics2 doesn't have a function to unregister a set of metrics
		/// and fails if a new metrics class is registered with the same name
		/// as the existing one. Hence, we have to maintain our own registry
		/// ("multiton") here, so that we have exactly one metrics instance
		/// per JournalNode, and switch out the pointer to the underlying
		/// IPCLoggerChannel instance.
		/// </remarks>
		private static readonly IDictionary<string, Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannelMetrics
			> Registry = Maps.NewHashMap();

		private IPCLoggerChannelMetrics(IPCLoggerChannel ch)
		{
			this.ch = ch;
			Configuration conf = new HdfsConfiguration();
			int[] intervals = conf.GetInts(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey);
			if (intervals != null)
			{
				writeEndToEndLatencyQuantiles = new MutableQuantiles[intervals.Length];
				writeRpcLatencyQuantiles = new MutableQuantiles[intervals.Length];
				for (int i = 0; i < writeEndToEndLatencyQuantiles.Length; i++)
				{
					int interval = intervals[i];
					writeEndToEndLatencyQuantiles[i] = registry.NewQuantiles("writesE2E" + interval +
						 "s", "End-to-end time for write operations", "ops", "LatencyMicros", interval);
					writeRpcLatencyQuantiles[i] = registry.NewQuantiles("writesRpc" + interval + "s", 
						"RPC RTT for write operations", "ops", "LatencyMicros", interval);
				}
			}
			else
			{
				writeEndToEndLatencyQuantiles = null;
				writeRpcLatencyQuantiles = null;
			}
		}

		private void SetChannel(IPCLoggerChannel ch)
		{
			System.Diagnostics.Debug.Assert(ch.GetRemoteAddress().Equals(this.ch.GetRemoteAddress
				()));
			this.ch = ch;
		}

		internal static Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannelMetrics Create
			(IPCLoggerChannel ch)
		{
			string name = GetName(ch);
			lock (Registry)
			{
				Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannelMetrics m = Registry[name];
				if (m != null)
				{
					m.SetChannel(ch);
				}
				else
				{
					m = new Org.Apache.Hadoop.Hdfs.Qjournal.Client.IPCLoggerChannelMetrics(ch);
					DefaultMetricsSystem.Instance().Register(name, null, m);
					Registry[name] = m;
				}
				return m;
			}
		}

		private static string GetName(IPCLoggerChannel ch)
		{
			IPEndPoint addr = ch.GetRemoteAddress();
			string addrStr = addr.Address.GetHostAddress();
			// IPv6 addresses have colons, which aren't allowed as part of
			// MBean names. Replace with '.'
			addrStr = addrStr.Replace(':', '.');
			return "IPCLoggerChannel-" + addrStr + "-" + addr.Port;
		}

		public virtual string IsOutOfSync()
		{
			return bool.ToString(ch.IsOutOfSync());
		}

		public virtual long GetCurrentLagTxns()
		{
			return ch.GetLagTxns();
		}

		public virtual long GetLagTimeMillis()
		{
			return ch.GetLagTimeMillis();
		}

		public virtual int GetQueuedEditsSize()
		{
			return ch.GetQueuedEditsSize();
		}

		public virtual void AddWriteEndToEndLatency(long micros)
		{
			if (writeEndToEndLatencyQuantiles != null)
			{
				foreach (MutableQuantiles q in writeEndToEndLatencyQuantiles)
				{
					q.Add(micros);
				}
			}
		}

		public virtual void AddWriteRpcLatency(long micros)
		{
			if (writeRpcLatencyQuantiles != null)
			{
				foreach (MutableQuantiles q in writeRpcLatencyQuantiles)
				{
					q.Add(micros);
				}
			}
		}
	}
}
