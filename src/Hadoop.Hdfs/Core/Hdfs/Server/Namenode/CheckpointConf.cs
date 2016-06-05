using System;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class CheckpointConf
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.CheckpointConf
			));

		/// <summary>How often to checkpoint regardless of number of txns</summary>
		private readonly long checkpointPeriod;

		/// <summary>How often to poll the NN to check checkpointTxnCount</summary>
		private readonly long checkpointCheckPeriod;

		/// <summary>checkpoint once every this many transactions, regardless of time</summary>
		private readonly long checkpointTxnCount;

		/// <summary>maxium number of retries when merge errors occur</summary>
		private readonly int maxRetriesOnMergeError;

		/// <summary>The output dir for legacy OIV image</summary>
		private readonly string legacyOivImageDir;

		public CheckpointConf(Configuration conf)
		{
			// in seconds
			// in seconds
			checkpointCheckPeriod = conf.GetLong(DfsNamenodeCheckpointCheckPeriodKey, DfsNamenodeCheckpointCheckPeriodDefault
				);
			checkpointPeriod = conf.GetLong(DfsNamenodeCheckpointPeriodKey, DfsNamenodeCheckpointPeriodDefault
				);
			checkpointTxnCount = conf.GetLong(DfsNamenodeCheckpointTxnsKey, DfsNamenodeCheckpointTxnsDefault
				);
			maxRetriesOnMergeError = conf.GetInt(DfsNamenodeCheckpointMaxRetriesKey, DfsNamenodeCheckpointMaxRetriesDefault
				);
			legacyOivImageDir = conf.Get(DfsNamenodeLegacyOivImageDirKey);
			WarnForDeprecatedConfigs(conf);
		}

		private static void WarnForDeprecatedConfigs(Configuration conf)
		{
			foreach (string key in ImmutableList.Of("fs.checkpoint.size", "dfs.namenode.checkpoint.size"
				))
			{
				if (conf.Get(key) != null)
				{
					Log.Warn("Configuration key " + key + " is deprecated! Ignoring..." + " Instead please specify a value for "
						 + DfsNamenodeCheckpointTxnsKey);
				}
			}
		}

		public virtual long GetPeriod()
		{
			return checkpointPeriod;
		}

		public virtual long GetCheckPeriod()
		{
			return Math.Min(checkpointCheckPeriod, checkpointPeriod);
		}

		public virtual long GetTxnCount()
		{
			return checkpointTxnCount;
		}

		public virtual int GetMaxRetriesOnMergeError()
		{
			return maxRetriesOnMergeError;
		}

		public virtual string GetLegacyOivImageDir()
		{
			return legacyOivImageDir;
		}
	}
}
