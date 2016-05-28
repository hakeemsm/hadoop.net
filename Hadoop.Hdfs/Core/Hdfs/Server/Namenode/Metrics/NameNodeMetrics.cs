using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics
{
	/// <summary>
	/// This class is for maintaining  the various NameNode activity statistics
	/// and publishing them through the metrics interfaces.
	/// </summary>
	public class NameNodeMetrics
	{
		internal readonly MetricsRegistry registry = new MetricsRegistry("namenode");

		[Metric]
		internal MutableCounterLong createFileOps;

		[Metric]
		internal MutableCounterLong filesCreated;

		[Metric]
		internal MutableCounterLong filesAppended;

		[Metric]
		internal MutableCounterLong getBlockLocations;

		[Metric]
		internal MutableCounterLong filesRenamed;

		[Metric]
		internal MutableCounterLong filesTruncated;

		[Metric]
		internal MutableCounterLong getListingOps;

		[Metric]
		internal MutableCounterLong deleteFileOps;

		internal MutableCounterLong filesDeleted;

		[Metric]
		internal MutableCounterLong fileInfoOps;

		[Metric]
		internal MutableCounterLong addBlockOps;

		[Metric]
		internal MutableCounterLong getAdditionalDatanodeOps;

		[Metric]
		internal MutableCounterLong createSymlinkOps;

		[Metric]
		internal MutableCounterLong getLinkTargetOps;

		[Metric]
		internal MutableCounterLong filesInGetListingOps;

		internal MutableCounterLong allowSnapshotOps;

		internal MutableCounterLong disallowSnapshotOps;

		internal MutableCounterLong createSnapshotOps;

		internal MutableCounterLong deleteSnapshotOps;

		internal MutableCounterLong renameSnapshotOps;

		internal MutableCounterLong listSnapshottableDirOps;

		internal MutableCounterLong snapshotDiffReportOps;

		internal MutableCounterLong blockReceivedAndDeletedOps;

		internal MutableCounterLong storageBlockReportOps;

		public virtual long TotalFileOps()
		{
			return getBlockLocations.Value() + createFileOps.Value() + filesAppended.Value() 
				+ addBlockOps.Value() + getAdditionalDatanodeOps.Value() + filesRenamed.Value() 
				+ filesTruncated.Value() + deleteFileOps.Value() + getListingOps.Value() + fileInfoOps
				.Value() + getLinkTargetOps.Value() + createSnapshotOps.Value() + deleteSnapshotOps
				.Value() + allowSnapshotOps.Value() + disallowSnapshotOps.Value() + renameSnapshotOps
				.Value() + listSnapshottableDirOps.Value() + createSymlinkOps.Value() + snapshotDiffReportOps
				.Value();
		}

		internal MutableRate transactions;

		internal MutableRate syncs;

		internal readonly MutableQuantiles[] syncsQuantiles;

		internal MutableCounterLong transactionsBatchedInSync;

		internal MutableRate blockReport;

		internal readonly MutableQuantiles[] blockReportQuantiles;

		internal MutableRate cacheReport;

		internal readonly MutableQuantiles[] cacheReportQuantiles;

		internal MutableGaugeInt safeModeTime;

		internal MutableGaugeInt fsImageLoadTime;

		internal MutableRate getEdit;

		internal MutableRate getImage;

		internal MutableRate putImage;

		internal JvmMetrics jvmMetrics = null;

		internal NameNodeMetrics(string processName, string sessionId, int[] intervals, JvmMetrics
			 jvmMetrics)
		{
			this.jvmMetrics = jvmMetrics;
			registry.Tag(MsInfo.ProcessName, processName).Tag(MsInfo.SessionId, sessionId);
			int len = intervals.Length;
			syncsQuantiles = new MutableQuantiles[len];
			blockReportQuantiles = new MutableQuantiles[len];
			cacheReportQuantiles = new MutableQuantiles[len];
			for (int i = 0; i < len; i++)
			{
				int interval = intervals[i];
				syncsQuantiles[i] = registry.NewQuantiles("syncs" + interval + "s", "Journal syncs"
					, "ops", "latency", interval);
				blockReportQuantiles[i] = registry.NewQuantiles("blockReport" + interval + "s", "Block report"
					, "ops", "latency", interval);
				cacheReportQuantiles[i] = registry.NewQuantiles("cacheReport" + interval + "s", "Cache report"
					, "ops", "latency", interval);
			}
		}

		public static Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics.NameNodeMetrics Create
			(Configuration conf, HdfsServerConstants.NamenodeRole r)
		{
			string sessionId = conf.Get(DFSConfigKeys.DfsMetricsSessionIdKey);
			string processName = r.ToString();
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			JvmMetrics jm = JvmMetrics.Create(processName, sessionId, ms);
			// Percentile measurement is off by default, by watching no intervals
			int[] intervals = conf.GetInts(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey);
			return ms.Register(new Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics.NameNodeMetrics
				(processName, sessionId, intervals, jm));
		}

		public virtual JvmMetrics GetJvmMetrics()
		{
			return jvmMetrics;
		}

		public virtual void Shutdown()
		{
			DefaultMetricsSystem.Shutdown();
		}

		public virtual void IncrGetBlockLocations()
		{
			getBlockLocations.Incr();
		}

		public virtual void IncrFilesCreated()
		{
			filesCreated.Incr();
		}

		public virtual void IncrCreateFileOps()
		{
			createFileOps.Incr();
		}

		public virtual void IncrFilesAppended()
		{
			filesAppended.Incr();
		}

		public virtual void IncrAddBlockOps()
		{
			addBlockOps.Incr();
		}

		public virtual void IncrGetAdditionalDatanodeOps()
		{
			getAdditionalDatanodeOps.Incr();
		}

		public virtual void IncrFilesRenamed()
		{
			filesRenamed.Incr();
		}

		public virtual void IncrFilesTruncated()
		{
			filesTruncated.Incr();
		}

		public virtual void IncrFilesDeleted(long delta)
		{
			filesDeleted.Incr(delta);
		}

		public virtual void IncrDeleteFileOps()
		{
			deleteFileOps.Incr();
		}

		public virtual void IncrGetListingOps()
		{
			getListingOps.Incr();
		}

		public virtual void IncrFilesInGetListingOps(int delta)
		{
			filesInGetListingOps.Incr(delta);
		}

		public virtual void IncrFileInfoOps()
		{
			fileInfoOps.Incr();
		}

		public virtual void IncrCreateSymlinkOps()
		{
			createSymlinkOps.Incr();
		}

		public virtual void IncrGetLinkTargetOps()
		{
			getLinkTargetOps.Incr();
		}

		public virtual void IncrAllowSnapshotOps()
		{
			allowSnapshotOps.Incr();
		}

		public virtual void IncrDisAllowSnapshotOps()
		{
			disallowSnapshotOps.Incr();
		}

		public virtual void IncrCreateSnapshotOps()
		{
			createSnapshotOps.Incr();
		}

		public virtual void IncrDeleteSnapshotOps()
		{
			deleteSnapshotOps.Incr();
		}

		public virtual void IncrRenameSnapshotOps()
		{
			renameSnapshotOps.Incr();
		}

		public virtual void IncrListSnapshottableDirOps()
		{
			listSnapshottableDirOps.Incr();
		}

		public virtual void IncrSnapshotDiffReportOps()
		{
			snapshotDiffReportOps.Incr();
		}

		public virtual void IncrBlockReceivedAndDeletedOps()
		{
			blockReceivedAndDeletedOps.Incr();
		}

		public virtual void IncrStorageBlockReportOps()
		{
			storageBlockReportOps.Incr();
		}

		public virtual void AddTransaction(long latency)
		{
			transactions.Add(latency);
		}

		public virtual void IncrTransactionsBatchedInSync()
		{
			transactionsBatchedInSync.Incr();
		}

		public virtual void AddSync(long elapsed)
		{
			syncs.Add(elapsed);
			foreach (MutableQuantiles q in syncsQuantiles)
			{
				q.Add(elapsed);
			}
		}

		public virtual void SetFsImageLoadTime(long elapsed)
		{
			fsImageLoadTime.Set((int)elapsed);
		}

		public virtual void AddBlockReport(long latency)
		{
			blockReport.Add(latency);
			foreach (MutableQuantiles q in blockReportQuantiles)
			{
				q.Add(latency);
			}
		}

		public virtual void AddCacheBlockReport(long latency)
		{
			cacheReport.Add(latency);
			foreach (MutableQuantiles q in cacheReportQuantiles)
			{
				q.Add(latency);
			}
		}

		public virtual void SetSafeModeTime(long elapsed)
		{
			safeModeTime.Set((int)elapsed);
		}

		public virtual void AddGetEdit(long latency)
		{
			getEdit.Add(latency);
		}

		public virtual void AddGetImage(long latency)
		{
			getImage.Add(latency);
		}

		public virtual void AddPutImage(long latency)
		{
			putImage.Add(latency);
		}
	}
}
