using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>
	/// This class is for maintaining the various NFS gateway activity statistics and
	/// publishing them through the metrics interfaces.
	/// </summary>
	public class Nfs3Metrics
	{
		[Metric]
		internal MutableRate getattr;

		[Metric]
		internal MutableRate setattr;

		[Metric]
		internal MutableRate lookup;

		[Metric]
		internal MutableRate access;

		[Metric]
		internal MutableRate readlink;

		[Metric]
		internal MutableRate read;

		internal readonly MutableQuantiles[] readNanosQuantiles;

		[Metric]
		internal MutableRate write;

		internal readonly MutableQuantiles[] writeNanosQuantiles;

		[Metric]
		internal MutableRate create;

		[Metric]
		internal MutableRate mkdir;

		[Metric]
		internal MutableRate symlink;

		[Metric]
		internal MutableRate mknod;

		[Metric]
		internal MutableRate remove;

		[Metric]
		internal MutableRate rmdir;

		[Metric]
		internal MutableRate rename;

		[Metric]
		internal MutableRate link;

		[Metric]
		internal MutableRate readdir;

		[Metric]
		internal MutableRate readdirplus;

		[Metric]
		internal MutableRate fsstat;

		[Metric]
		internal MutableRate fsinfo;

		[Metric]
		internal MutableRate pathconf;

		[Metric]
		internal MutableRate commit;

		internal readonly MutableQuantiles[] commitNanosQuantiles;

		[Metric]
		internal MutableCounterLong bytesWritten;

		[Metric]
		internal MutableCounterLong bytesRead;

		internal readonly MetricsRegistry registry = new MetricsRegistry("nfs3");

		internal readonly string name;

		internal JvmMetrics jvmMetrics = null;

		public Nfs3Metrics(string name, string sessionId, int[] intervals, JvmMetrics jvmMetrics
			)
		{
			// All mutable rates are in nanoseconds
			// No metric for nullProcedure;
			this.name = name;
			this.jvmMetrics = jvmMetrics;
			registry.Tag(MsInfo.SessionId, sessionId);
			int len = intervals.Length;
			readNanosQuantiles = new MutableQuantiles[len];
			writeNanosQuantiles = new MutableQuantiles[len];
			commitNanosQuantiles = new MutableQuantiles[len];
			for (int i = 0; i < len; i++)
			{
				int interval = intervals[i];
				readNanosQuantiles[i] = registry.NewQuantiles("readProcessNanos" + interval + "s"
					, "Read process in ns", "ops", "latency", interval);
				writeNanosQuantiles[i] = registry.NewQuantiles("writeProcessNanos" + interval + "s"
					, "Write process in ns", "ops", "latency", interval);
				commitNanosQuantiles[i] = registry.NewQuantiles("commitProcessNanos" + interval +
					 "s", "Commit process in ns", "ops", "latency", interval);
			}
		}

		public static Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3Metrics Create(Configuration conf
			, string gatewayName)
		{
			string sessionId = conf.Get(DFSConfigKeys.DfsMetricsSessionIdKey);
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			JvmMetrics jm = JvmMetrics.Create(gatewayName, sessionId, ms);
			// Percentile measurement is [50th,75th,90th,95th,99th] currently 
			int[] intervals = conf.GetInts(NfsConfigKeys.NfsMetricsPercentilesIntervalsKey);
			return ms.Register(new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3Metrics(gatewayName, sessionId
				, intervals, jm));
		}

		public virtual string Name()
		{
			return name;
		}

		public virtual JvmMetrics GetJvmMetrics()
		{
			return jvmMetrics;
		}

		public virtual void IncrBytesWritten(long bytes)
		{
			bytesWritten.Incr(bytes);
		}

		public virtual void IncrBytesRead(long bytes)
		{
			bytesRead.Incr(bytes);
		}

		public virtual void AddGetattr(long latencyNanos)
		{
			getattr.Add(latencyNanos);
		}

		public virtual void AddSetattr(long latencyNanos)
		{
			setattr.Add(latencyNanos);
		}

		public virtual void AddLookup(long latencyNanos)
		{
			lookup.Add(latencyNanos);
		}

		public virtual void AddAccess(long latencyNanos)
		{
			access.Add(latencyNanos);
		}

		public virtual void AddReadlink(long latencyNanos)
		{
			readlink.Add(latencyNanos);
		}

		public virtual void AddRead(long latencyNanos)
		{
			read.Add(latencyNanos);
			foreach (MutableQuantiles q in readNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void AddWrite(long latencyNanos)
		{
			write.Add(latencyNanos);
			foreach (MutableQuantiles q in writeNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}

		public virtual void AddCreate(long latencyNanos)
		{
			create.Add(latencyNanos);
		}

		public virtual void AddMkdir(long latencyNanos)
		{
			mkdir.Add(latencyNanos);
		}

		public virtual void AddSymlink(long latencyNanos)
		{
			symlink.Add(latencyNanos);
		}

		public virtual void AddMknod(long latencyNanos)
		{
			mknod.Add(latencyNanos);
		}

		public virtual void AddRemove(long latencyNanos)
		{
			remove.Add(latencyNanos);
		}

		public virtual void AddRmdir(long latencyNanos)
		{
			rmdir.Add(latencyNanos);
		}

		public virtual void AddRename(long latencyNanos)
		{
			rename.Add(latencyNanos);
		}

		public virtual void AddLink(long latencyNanos)
		{
			link.Add(latencyNanos);
		}

		public virtual void AddReaddir(long latencyNanos)
		{
			readdir.Add(latencyNanos);
		}

		public virtual void AddReaddirplus(long latencyNanos)
		{
			readdirplus.Add(latencyNanos);
		}

		public virtual void AddFsstat(long latencyNanos)
		{
			fsstat.Add(latencyNanos);
		}

		public virtual void AddFsinfo(long latencyNanos)
		{
			fsinfo.Add(latencyNanos);
		}

		public virtual void AddPathconf(long latencyNanos)
		{
			pathconf.Add(latencyNanos);
		}

		public virtual void AddCommit(long latencyNanos)
		{
			commit.Add(latencyNanos);
			foreach (MutableQuantiles q in commitNanosQuantiles)
			{
				q.Add(latencyNanos);
			}
		}
	}
}
