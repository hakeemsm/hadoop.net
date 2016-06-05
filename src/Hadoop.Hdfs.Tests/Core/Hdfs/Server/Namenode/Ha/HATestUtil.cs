using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Static utility functions useful for testing HA.</summary>
	public abstract class HATestUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HATestUtil));

		private const string LogicalHostname = "ha-nn-uri-%d";

		/// <summary>
		/// Trigger an edits log roll on the active and then wait for the standby to
		/// catch up to all the edits done by the active.
		/// </summary>
		/// <remarks>
		/// Trigger an edits log roll on the active and then wait for the standby to
		/// catch up to all the edits done by the active. This method will check
		/// repeatedly for up to NN_LAG_TIMEOUT milliseconds, and then fail throwing
		/// <see cref="CouldNotCatchUpException"/>
		/// </remarks>
		/// <param name="active">active NN</param>
		/// <param name="standby">standby NN which should catch up to active</param>
		/// <exception cref="System.IO.IOException">if an error occurs rolling the edit log</exception>
		/// <exception cref="CouldNotCatchUpException">
		/// if the standby doesn't catch up to the
		/// active in NN_LAG_TIMEOUT milliseconds
		/// </exception>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.HATestUtil.CouldNotCatchUpException
		/// 	"/>
		public static void WaitForStandbyToCatchUp(NameNode active, NameNode standby)
		{
			long activeTxId = active.GetNamesystem().GetFSImage().GetEditLog().GetLastWrittenTxId
				();
			active.GetRpcServer().RollEditLog();
			long start = Time.Now();
			while (Time.Now() - start < TestEditLogTailer.NnLagTimeout)
			{
				long nn2HighestTxId = standby.GetNamesystem().GetFSImage().GetLastAppliedTxId();
				if (nn2HighestTxId >= activeTxId)
				{
					return;
				}
				Sharpen.Thread.Sleep(TestEditLogTailer.SleepTime);
			}
			throw new HATestUtil.CouldNotCatchUpException("Standby did not catch up to txid "
				 + activeTxId + " (currently at " + standby.GetNamesystem().GetFSImage().GetLastAppliedTxId
				() + ")");
		}

		/// <summary>
		/// Wait for the datanodes in the cluster to process any block
		/// deletions that have already been asynchronously queued.
		/// </summary>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		public static void WaitForDNDeletions(MiniDFSCluster cluster)
		{
			GenericTestUtils.WaitFor(new _Supplier_96(cluster), 1000, 10000);
		}

		private sealed class _Supplier_96 : Supplier<bool>
		{
			public _Supplier_96(MiniDFSCluster cluster)
			{
				this.cluster = cluster;
			}

			public bool Get()
			{
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					if (DataNodeTestUtils.GetPendingAsyncDeletions(dn) > 0)
					{
						return false;
					}
				}
				return true;
			}

			private readonly MiniDFSCluster cluster;
		}

		/// <summary>
		/// Wait for the NameNode to issue any deletions that are already
		/// pending (i.e.
		/// </summary>
		/// <remarks>
		/// Wait for the NameNode to issue any deletions that are already
		/// pending (i.e. for the pendingDeletionBlocksCount to go to 0)
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void WaitForNNToIssueDeletions(NameNode nn)
		{
			GenericTestUtils.WaitFor(new _Supplier_116(nn), 250, 10000);
		}

		private sealed class _Supplier_116 : Supplier<bool>
		{
			public _Supplier_116(NameNode nn)
			{
				this.nn = nn;
			}

			public bool Get()
			{
				HATestUtil.Log.Info("Waiting for NN to issue block deletions to DNs");
				return nn.GetNamesystem().GetBlockManager().GetPendingDeletionBlocksCount() == 0;
			}

			private readonly NameNode nn;
		}

		[System.Serializable]
		public class CouldNotCatchUpException : IOException
		{
			private const long serialVersionUID = 1L;

			public CouldNotCatchUpException(string message)
				: base(message)
			{
			}
		}

		/// <summary>Gets the filesystem instance by setting the failover configurations</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static FileSystem ConfigureFailoverFs(MiniDFSCluster cluster, Configuration
			 conf)
		{
			return ConfigureFailoverFs(cluster, conf, 0);
		}

		/// <summary>Gets the filesystem instance by setting the failover configurations</summary>
		/// <param name="cluster">the single process DFS cluster</param>
		/// <param name="conf">cluster configuration</param>
		/// <param name="nsIndex">namespace index starting with zero</param>
		/// <exception cref="System.IO.IOException">if an error occurs rolling the edit log</exception>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static FileSystem ConfigureFailoverFs(MiniDFSCluster cluster, Configuration
			 conf, int nsIndex)
		{
			conf = new Configuration(conf);
			string logicalName = GetLogicalHostname(cluster);
			SetFailoverConfigurations(cluster, conf, logicalName, nsIndex);
			FileSystem fs = FileSystem.Get(new URI("hdfs://" + logicalName), conf);
			return fs;
		}

		public static void SetFailoverConfigurations(MiniDFSCluster cluster, Configuration
			 conf)
		{
			SetFailoverConfigurations(cluster, conf, GetLogicalHostname(cluster));
		}

		/// <summary>Sets the required configurations for performing failover of default namespace.
		/// 	</summary>
		public static void SetFailoverConfigurations(MiniDFSCluster cluster, Configuration
			 conf, string logicalName)
		{
			SetFailoverConfigurations(cluster, conf, logicalName, 0);
		}

		/// <summary>Sets the required configurations for performing failover.</summary>
		public static void SetFailoverConfigurations(MiniDFSCluster cluster, Configuration
			 conf, string logicalName, int nsIndex)
		{
			IPEndPoint nnAddr1 = cluster.GetNameNode(2 * nsIndex).GetNameNodeAddress();
			IPEndPoint nnAddr2 = cluster.GetNameNode(2 * nsIndex + 1).GetNameNodeAddress();
			SetFailoverConfigurations(conf, logicalName, nnAddr1, nnAddr2);
		}

		/// <summary>Sets the required configurations for performing failover</summary>
		public static void SetFailoverConfigurations(Configuration conf, string logicalName
			, IPEndPoint nnAddr1, IPEndPoint nnAddr2)
		{
			string nameNodeId1 = "nn1";
			string nameNodeId2 = "nn2";
			string address1 = "hdfs://" + nnAddr1.GetHostName() + ":" + nnAddr1.Port;
			string address2 = "hdfs://" + nnAddr2.GetHostName() + ":" + nnAddr2.Port;
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, logicalName
				, nameNodeId1), address1);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, logicalName
				, nameNodeId2), address2);
			conf.Set(DFSConfigKeys.DfsNameservices, logicalName);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, logicalName
				), nameNodeId1 + "," + nameNodeId2);
			conf.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + logicalName
				, typeof(ConfiguredFailoverProxyProvider).FullName);
			conf.Set("fs.defaultFS", "hdfs://" + logicalName);
		}

		public static string GetLogicalHostname(MiniDFSCluster cluster)
		{
			return string.Format(LogicalHostname, cluster.GetInstanceId());
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		public static URI GetLogicalUri(MiniDFSCluster cluster)
		{
			return new URI(HdfsConstants.HdfsUriScheme + "://" + GetLogicalHostname(cluster));
		}

		/// <exception cref="System.Exception"/>
		public static void WaitForCheckpoint(MiniDFSCluster cluster, int nnIdx, IList<int
			> txids)
		{
			long start = Time.Now();
			while (true)
			{
				try
				{
					FSImageTestUtil.AssertNNHasCheckpoints(cluster, nnIdx, txids);
					return;
				}
				catch (Exception err)
				{
					if (Time.Now() - start > 10000)
					{
						throw;
					}
					else
					{
						Sharpen.Thread.Sleep(300);
					}
				}
			}
		}
	}
}
