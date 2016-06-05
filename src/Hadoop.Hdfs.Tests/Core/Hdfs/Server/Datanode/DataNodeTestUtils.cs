using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Utility class for accessing package-private DataNode information during tests.
	/// 	</summary>
	public class DataNodeTestUtils
	{
		private const string DirFailureSuffix = ".origin";

		/// <exception cref="System.IO.IOException"/>
		public static DatanodeRegistration GetDNRegistrationForBP(DataNode dn, string bpid
			)
		{
			return dn.GetDNRegistrationForBP(bpid);
		}

		public static void SetHeartbeatsDisabledForTests(DataNode dn, bool heartbeatsDisabledForTests
			)
		{
			dn.SetHeartbeatsDisabledForTests(heartbeatsDisabledForTests);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TriggerDeletionReport(DataNode dn)
		{
			foreach (BPOfferService bpos in dn.GetAllBpOs())
			{
				bpos.TriggerDeletionReportForTests();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TriggerHeartbeat(DataNode dn)
		{
			foreach (BPOfferService bpos in dn.GetAllBpOs())
			{
				bpos.TriggerHeartbeatForTests();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void TriggerBlockReport(DataNode dn)
		{
			foreach (BPOfferService bpos in dn.GetAllBpOs())
			{
				bpos.TriggerBlockReportForTests();
			}
		}

		/// <summary>
		/// Insert a Mockito spy object between the given DataNode and
		/// the given NameNode.
		/// </summary>
		/// <remarks>
		/// Insert a Mockito spy object between the given DataNode and
		/// the given NameNode. This can be used to delay or wait for
		/// RPC calls on the datanode-&gt;NN path.
		/// </remarks>
		public static DatanodeProtocolClientSideTranslatorPB SpyOnBposToNN(DataNode dn, NameNode
			 nn)
		{
			string bpid = nn.GetNamesystem().GetBlockPoolId();
			BPOfferService bpos = null;
			foreach (BPOfferService thisBpos in dn.GetAllBpOs())
			{
				if (thisBpos.GetBlockPoolId().Equals(bpid))
				{
					bpos = thisBpos;
					break;
				}
			}
			Preconditions.CheckArgument(bpos != null, "No such bpid: %s", bpid);
			BPServiceActor bpsa = null;
			foreach (BPServiceActor thisBpsa in bpos.GetBPServiceActors())
			{
				if (thisBpsa.GetNNSocketAddress().Equals(nn.GetServiceRpcAddress()))
				{
					bpsa = thisBpsa;
					break;
				}
			}
			Preconditions.CheckArgument(bpsa != null, "No service actor to NN at %s", nn.GetServiceRpcAddress
				());
			DatanodeProtocolClientSideTranslatorPB origNN = bpsa.GetNameNodeProxy();
			DatanodeProtocolClientSideTranslatorPB spy = Org.Mockito.Mockito.Spy(origNN);
			bpsa.SetNameNode(spy);
			return spy;
		}

		/// <exception cref="System.IO.IOException"/>
		public static InterDatanodeProtocol CreateInterDatanodeProtocolProxy(DataNode dn, 
			DatanodeID datanodeid, Configuration conf, bool connectToDnViaHostname)
		{
			if (connectToDnViaHostname != dn.GetDnConf().connectToDnViaHostname)
			{
				throw new Exception("Unexpected DN hostname configuration");
			}
			return DataNode.CreateInterDataNodeProtocolProxy(datanodeid, conf, dn.GetDnConf()
				.socketTimeout, dn.GetDnConf().connectToDnViaHostname);
		}

		/// <summary>This method is used for testing.</summary>
		/// <remarks>
		/// This method is used for testing.
		/// Examples are adding and deleting blocks directly.
		/// The most common usage will be when the data node's storage is simulated.
		/// </remarks>
		/// <returns>the fsdataset that stores the blocks</returns>
		public static FsDatasetSpi<object> GetFSDataset(DataNode dn)
		{
			return dn.GetFSDataset();
		}

		public static FilePath GetFile(DataNode dn, string bpid, long bid)
		{
			return FsDatasetTestUtil.GetFile(dn.GetFSDataset(), bpid, bid);
		}

		/// <exception cref="System.IO.IOException"/>
		public static FilePath GetBlockFile(DataNode dn, string bpid, Block b)
		{
			return FsDatasetTestUtil.GetBlockFile(dn.GetFSDataset(), bpid, b);
		}

		/// <exception cref="System.IO.IOException"/>
		public static FilePath GetMetaFile(DataNode dn, string bpid, Block b)
		{
			return FsDatasetTestUtil.GetMetaFile(dn.GetFSDataset(), bpid, b);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool UnlinkBlock(DataNode dn, ExtendedBlock bk, int numLinks)
		{
			return FsDatasetTestUtil.UnlinkBlock(dn.GetFSDataset(), bk, numLinks);
		}

		public static long GetPendingAsyncDeletions(DataNode dn)
		{
			return FsDatasetTestUtil.GetPendingAsyncDeletions(dn.GetFSDataset());
		}

		/// <summary>Fetch a copy of ReplicaInfo from a datanode by block id</summary>
		/// <param name="dn">datanode to retrieve a replicainfo object from</param>
		/// <param name="bpid">Block pool Id</param>
		/// <param name="blkId">id of the replica's block</param>
		/// <returns>copy of ReplicaInfo object @link{FSDataset#fetchReplicaInfo}</returns>
		public static ReplicaInfo FetchReplicaInfo(DataNode dn, string bpid, long blkId)
		{
			return FsDatasetTestUtil.FetchReplicaInfo(dn.GetFSDataset(), bpid, blkId);
		}

		/// <summary>
		/// It injects disk failures to data dirs by replacing these data dirs with
		/// regular files.
		/// </summary>
		/// <param name="dirs">data directories.</param>
		/// <exception cref="System.IO.IOException">on I/O error.</exception>
		public static void InjectDataDirFailure(params FilePath[] dirs)
		{
			foreach (FilePath dir in dirs)
			{
				FilePath renamedTo = new FilePath(dir.GetPath() + DirFailureSuffix);
				if (renamedTo.Exists())
				{
					throw new IOException(string.Format("Can not inject failure to dir: %s because %s exists."
						, dir, renamedTo));
				}
				if (!dir.RenameTo(renamedTo))
				{
					throw new IOException(string.Format("Failed to rename %s to %s.", dir, renamedTo)
						);
				}
				if (!dir.CreateNewFile())
				{
					throw new IOException(string.Format("Failed to create file %s to inject disk failure."
						, dir));
				}
			}
		}

		/// <summary>Restore the injected data dir failures.</summary>
		/// <seealso>
		/// 
		/// <see cref="#injectDataDirFailures"/>
		/// .
		/// </seealso>
		/// <param name="dirs">data directories.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void RestoreDataDirFromFailure(params FilePath[] dirs)
		{
			foreach (FilePath dir in dirs)
			{
				FilePath renamedDir = new FilePath(dir.GetPath() + DirFailureSuffix);
				if (renamedDir.Exists())
				{
					if (dir.Exists())
					{
						if (!dir.IsFile())
						{
							throw new IOException("Injected failure data dir is supposed to be file: " + dir);
						}
						if (!dir.Delete())
						{
							throw new IOException("Failed to delete injected failure data dir: " + dir);
						}
					}
					if (!renamedDir.RenameTo(dir))
					{
						throw new IOException(string.Format("Failed to recover injected failure data dir %s to %s."
							, renamedDir, dir));
					}
				}
			}
		}
	}
}
