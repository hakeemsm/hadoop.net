using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics
{
	/// <summary>
	/// This class is for maintaining  the various DataNode statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	/// <remarks>
	/// This class is for maintaining  the various DataNode statistics
	/// and publishing them through the metrics interfaces.
	/// This also registers the JMX MBean for RPC.
	/// <p>
	/// This class has a number of metrics variables that are publicly accessible;
	/// these variables (objects) have methods to update their values;
	/// for example:
	/// <p>
	/// <see cref="blocksRead"/>
	/// .inc()
	/// </remarks>
	public class DataNodeMetrics
	{
		[Metric]
		internal MutableCounterLong bytesWritten;

		internal MutableCounterLong totalWriteTime;

		[Metric]
		internal MutableCounterLong bytesRead;

		internal MutableCounterLong totalReadTime;

		[Metric]
		internal MutableCounterLong blocksWritten;

		[Metric]
		internal MutableCounterLong blocksRead;

		[Metric]
		internal MutableCounterLong blocksReplicated;

		[Metric]
		internal MutableCounterLong blocksRemoved;

		[Metric]
		internal MutableCounterLong blocksVerified;

		[Metric]
		internal MutableCounterLong blockVerificationFailures;

		[Metric]
		internal MutableCounterLong blocksCached;

		[Metric]
		internal MutableCounterLong blocksUncached;

		[Metric]
		internal MutableCounterLong readsFromLocalClient;

		[Metric]
		internal MutableCounterLong readsFromRemoteClient;

		[Metric]
		internal MutableCounterLong writesFromLocalClient;

		[Metric]
		internal MutableCounterLong writesFromRemoteClient;

		[Metric]
		internal MutableCounterLong blocksGetLocalPathInfo;

		internal MutableCounterLong remoteBytesRead;

		internal MutableCounterLong remoteBytesWritten;

		[Metric]
		internal MutableCounterLong ramDiskBlocksWrite;

		[Metric]
		internal MutableCounterLong ramDiskBlocksWriteFallback;

		[Metric]
		internal MutableCounterLong ramDiskBytesWrite;

		[Metric]
		internal MutableCounterLong ramDiskBlocksReadHits;

		[Metric]
		internal MutableCounterLong ramDiskBlocksEvicted;

		[Metric]
		internal MutableCounterLong ramDiskBlocksEvictedWithoutRead;

		[Metric]
		internal MutableRate ramDiskBlocksEvictionWindowMs;

		internal readonly MutableQuantiles[] ramDiskBlocksEvictionWindowMsQuantiles;

		[Metric]
		internal MutableCounterLong ramDiskBlocksLazyPersisted;

		[Metric]
		internal MutableCounterLong ramDiskBlocksDeletedBeforeLazyPersisted;

		[Metric]
		internal MutableCounterLong ramDiskBytesLazyPersisted;

		[Metric]
		internal MutableRate ramDiskBlocksLazyPersistWindowMs;

		internal readonly MutableQuantiles[] ramDiskBlocksLazyPersistWindowMsQuantiles;

		[Metric]
		internal MutableCounterLong fsyncCount;

		[Metric]
		internal MutableCounterLong volumeFailures;

		internal MutableCounterLong datanodeNetworkErrors;

		[Metric]
		internal MutableRate readBlockOp;

		[Metric]
		internal MutableRate writeBlockOp;

		[Metric]
		internal MutableRate blockChecksumOp;

		[Metric]
		internal MutableRate copyBlockOp;

		[Metric]
		internal MutableRate replaceBlockOp;

		[Metric]
		internal MutableRate heartbeats;

		[Metric]
		internal MutableRate blockReports;

		[Metric]
		internal MutableRate incrementalBlockReports;

		[Metric]
		internal MutableRate cacheReports;

		[Metric]
		internal MutableRate packetAckRoundTripTimeNanos;

		internal readonly MutableQuantiles[] packetAckRoundTripTimeNanosQuantiles;

		[Metric]
		internal MutableRate flushNanos;

		internal readonly MutableQuantiles[] flushNanosQuantiles;

		[Metric]
		internal MutableRate fsyncNanos;

		internal readonly MutableQuantiles[] fsyncNanosQuantiles;

		[Metric]
		internal MutableRate sendDataPacketBlockedOnNetworkNanos;

		internal readonly MutableQuantiles[] sendDataPacketBlockedOnNetworkNanosQuantiles;

		[Metric]
		internal MutableRate sendDataPacketTransferNanos;

		internal readonly MutableQuantiles[] sendDataPacketTransferNanosQuantiles;

		internal readonly MetricsRegistry registry = new MetricsRegistry("datanode");

		internal readonly string name;

		internal JvmMetrics jvmMetrics = null;

		public DataNodeMetrics(string name, string sessionId, int[] intervals, JvmMetrics
			 jvmMetrics)
		{
			// RamDisk metrics on read/write
			// RamDisk metrics on eviction
			// RamDisk metrics on lazy persist
			this.name = name;
			this.jvmMetrics = jvmMetrics;
			registry.Tag(MsInfo.SessionId, sessionId);
			int len = intervals.Length;
			packetAckRoundTripTimeNanosQuantiles = new MutableQuantiles[len];
			flushNanosQuantiles = new MutableQuantiles[len];
			fsyncNanosQuantiles = new MutableQuantiles[len];
			sendDataPacketBlockedOnNetworkNanosQuantiles = new MutableQuantiles[len];
			sendDataPacketTransferNanosQuantiles = new MutableQuantiles[len];
			ramDiskBlocksEvictionWindowMsQuantiles = new MutableQuantiles[len];
			ramDiskBlocksLazyPersistWindowMsQuantiles = new MutableQuantiles[len];
			for (int i = 0; i < len; i++)
			{
				int interval = intervals[i];
				packetAckRoundTripTimeNanosQuantiles[i] = registry.NewQuantiles("packetAckRoundTripTimeNanos"
					 + interval + "s", "Packet Ack RTT in ns", "ops", "latency", interval);
				flushNanosQuantiles[i] = registry.NewQuantiles("flushNanos" + interval + "s", "Disk flush latency in ns"
					, "ops", "latency", interval);
				fsyncNanosQuantiles[i] = registry.NewQuantiles("fsyncNanos" + interval + "s", "Disk fsync latency in ns"
					, "ops", "latency", interval);
				sendDataPacketBlockedOnNetworkNanosQuantiles[i] = registry.NewQuantiles("sendDataPacketBlockedOnNetworkNanos"
					 + interval + "s", "Time blocked on network while sending a packet in ns", "ops"
					, "latency", interval);
				sendDataPacketTransferNanosQuantiles[i] = registry.NewQuantiles("sendDataPacketTransferNanos"
					 + interval + "s", "Time reading from disk and writing to network while sending "
					 + "a packet in ns", "ops", "latency", interval);
				ramDiskBlocksEvictionWindowMsQuantiles[i] = registry.NewQuantiles("ramDiskBlocksEvictionWindows"
					 + interval + "s", "Time between the RamDisk block write and eviction in ms", "ops"
					, "latency", interval);
				ramDiskBlocksLazyPersistWindowMsQuantiles[i] = registry.NewQuantiles("ramDiskBlocksLazyPersistWindows"
					 + interval + "s", "Time between the RamDisk block write and disk persist in ms"
					, "ops", "latency", interval);
			}
		}

		public static Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics.DataNodeMetrics Create
			(Configuration conf, string dnName)
		{
			string sessionId = conf.Get(DFSConfigKeys.DfsMetricsSessionIdKey);
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			JvmMetrics jm = JvmMetrics.Create("DataNode", sessionId, ms);
			string name = "DataNodeActivity-" + (dnName.IsEmpty() ? "UndefinedDataNodeName" +
				 DFSUtil.GetRandom().Next() : dnName.Replace(':', '-'));
			// Percentile measurement is off by default, by watching no intervals
			int[] intervals = conf.GetInts(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey);
			return ms.Register(name, null, new Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics.DataNodeMetrics
				(name, sessionId, intervals, jm));
		}

		public virtual string Name()
		{
			return name;
		}

		public virtual JvmMetrics GetJvmMetrics()
		{
			return jvmMetrics;
		}

		public virtual void AddHeartbeat(long latency)
		{
			heartbeats.Add(latency);
		}

		public virtual void AddBlockReport(long latency)
		{
			blockReports.Add(latency);
		}

		public virtual void AddIncrementalBlockReport(long latency)
		{
			incrementalBlockReports.Add(latency);
		}

		public virtual void AddCacheReport(long latency)
		{
			cacheReports.Add(latency);
		}

		public virtual void IncrBlocksReplicated(int delta)
		{
			blocksReplicated.Incr(delta);
		}

		public virtual void IncrBlocksWritten()
		{
			blocksWritten.Incr();
		}

		public virtual void IncrBlocksRemoved(int delta)
		{
			blocksRemoved.Incr(delta);
		}

		public virtual void IncrBytesWritten(int delta)
		{
			bytesWritten.Incr(delta);
		}

		public virtual void IncrBlockVerificationFailures()
		{
			blockVerificationFailures.Incr();
		}

		public virtual void IncrBlocksVerified()
		{
			blocksVerified.Incr();
		}

		public virtual void IncrBlocksCached(int delta)
		{
			blocksCached.Incr(delta);
		}

		public virtual void IncrBlocksUncached(int delta)
		{
			blocksUncached.Incr(delta);
		}

		public virtual void AddReadBlockOp(long latency)
		{
			readBlockOp.Add(latency);
		}

		public virtual void AddWriteBlockOp(long latency)
		{
			writeBlockOp.Add(latency);
		}

		public virtual void AddReplaceBlockOp(long latency)
		{
			replaceBlockOp.Add(latency);
		}

		public virtual void AddCopyBlockOp(long latency)
		{
			copyBlockOp.Add(latency);
		}

		public virtual void AddBlockChecksumOp(long latency)
		{
			blockChecksumOp.Add(latency);
		}

		public virtual void IncrBytesRead(int delta)
		{
			bytesRead.Incr(delta);
		}

		public virtual void IncrBlocksRead()
		{
			blocksRead.Incr();
		}

		public virtual void IncrFsyncCount()
		{
			fsyncCount.Incr();
		}

		public virtual void IncrTotalWriteTime(long timeTaken)
		{
			totalWriteTime.Incr(timeTaken);
		}

		public virtual void IncrTotalReadTime(long timeTaken)
		{
			totalReadTime.Incr(timeTaken);
		}

		public virtual void AddPacketAckRoundTripTimeNanos(long latencyNanos)
		{
			packetAckRoundTripTimeNanos.Add(latencyNanos);
			foreach (MutableQuantiles q in packetAckRoundTripTimeNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void AddFlushNanos(long latencyNanos)
		{
			flushNanos.Add(latencyNanos);
			foreach (MutableQuantiles q in flushNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void AddFsyncNanos(long latencyNanos)
		{
			fsyncNanos.Add(latencyNanos);
			foreach (MutableQuantiles q in fsyncNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void Shutdown()
		{
			DefaultMetricsSystem.Shutdown();
		}

		public virtual void IncrWritesFromClient(bool local, long size)
		{
			if (local)
			{
				writesFromLocalClient.Incr();
			}
			else
			{
				writesFromRemoteClient.Incr();
				remoteBytesWritten.Incr(size);
			}
		}

		public virtual void IncrReadsFromClient(bool local, long size)
		{
			if (local)
			{
				readsFromLocalClient.Incr();
			}
			else
			{
				readsFromRemoteClient.Incr();
				remoteBytesRead.Incr(size);
			}
		}

		public virtual void IncrVolumeFailures()
		{
			volumeFailures.Incr();
		}

		public virtual void IncrDatanodeNetworkErrors()
		{
			datanodeNetworkErrors.Incr();
		}

		/// <summary>Increment for getBlockLocalPathInfo calls</summary>
		public virtual void IncrBlocksGetLocalPathInfo()
		{
			blocksGetLocalPathInfo.Incr();
		}

		public virtual void AddSendDataPacketBlockedOnNetworkNanos(long latencyNanos)
		{
			sendDataPacketBlockedOnNetworkNanos.Add(latencyNanos);
			foreach (MutableQuantiles q in sendDataPacketBlockedOnNetworkNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void AddSendDataPacketTransferNanos(long latencyNanos)
		{
			sendDataPacketTransferNanos.Add(latencyNanos);
			foreach (MutableQuantiles q in sendDataPacketTransferNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void IncrRamDiskBlocksWrite()
		{
			ramDiskBlocksWrite.Incr();
		}

		public virtual void IncrRamDiskBlocksWriteFallback()
		{
			ramDiskBlocksWriteFallback.Incr();
		}

		public virtual void AddRamDiskBytesWrite(long bytes)
		{
			ramDiskBytesWrite.Incr(bytes);
		}

		public virtual void IncrRamDiskBlocksReadHits()
		{
			ramDiskBlocksReadHits.Incr();
		}

		public virtual void IncrRamDiskBlocksEvicted()
		{
			ramDiskBlocksEvicted.Incr();
		}

		public virtual void IncrRamDiskBlocksEvictedWithoutRead()
		{
			ramDiskBlocksEvictedWithoutRead.Incr();
		}

		public virtual void AddRamDiskBlocksEvictionWindowMs(long latencyMs)
		{
			ramDiskBlocksEvictionWindowMs.Add(latencyMs);
			foreach (MutableQuantiles q in ramDiskBlocksEvictionWindowMsQuantiles)
			{
				q.Add(latencyMs);
			}
		}

		public virtual void IncrRamDiskBlocksLazyPersisted()
		{
			ramDiskBlocksLazyPersisted.Incr();
		}

		public virtual void IncrRamDiskBlocksDeletedBeforeLazyPersisted()
		{
			ramDiskBlocksDeletedBeforeLazyPersisted.Incr();
		}

		public virtual void IncrRamDiskBytesLazyPersisted(long bytes)
		{
			ramDiskBytesLazyPersisted.Incr(bytes);
		}

		public virtual void AddRamDiskBlocksLazyPersistWindowMs(long latencyMs)
		{
			ramDiskBlocksLazyPersistWindowMs.Add(latencyMs);
			foreach (MutableQuantiles q in ramDiskBlocksLazyPersistWindowMsQuantiles)
			{
				q.Add(latencyMs);
			}
		}
	}
}
