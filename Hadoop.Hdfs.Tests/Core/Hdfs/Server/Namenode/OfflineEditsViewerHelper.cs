using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// OfflineEditsViewerHelper is a helper class for TestOfflineEditsViewer,
	/// it performs NN operations that generate all op codes
	/// </summary>
	public class OfflineEditsViewerHelper
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(OfflineEditsViewerHelper
			));

		internal readonly long blockSize = 512;

		internal MiniDFSCluster cluster = null;

		internal readonly Configuration config = new Configuration();

		/// <summary>Generates edits with all op codes and returns the edits filename</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual string GenerateEdits()
		{
			CheckpointSignature signature = RunOperations();
			return GetEditsFilename(signature);
		}

		/// <summary>Get edits filename</summary>
		/// <returns>edits file name for cluster</returns>
		/// <exception cref="System.IO.IOException"/>
		private string GetEditsFilename(CheckpointSignature sig)
		{
			FSImage image = cluster.GetNameNode().GetFSImage();
			// it was set up to only have ONE StorageDirectory
			IEnumerator<Storage.StorageDirectory> it = image.GetStorage().DirIterator(NNStorage.NameNodeDirType
				.Edits);
			Storage.StorageDirectory sd = it.Next();
			FilePath ret = NNStorage.GetFinalizedEditsFile(sd, 1, sig.curSegmentTxId - 1);
			System.Diagnostics.Debug.Assert(ret.Exists(), "expected " + ret + " exists");
			return ret.GetAbsolutePath();
		}

		/// <summary>
		/// Sets up a MiniDFSCluster, configures it to create one edits file,
		/// starts DelegationTokenSecretManager (to get security op codes)
		/// </summary>
		/// <param name="dfsDir">DFS directory (where to setup MiniDFS cluster)</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartCluster(string dfsDir)
		{
			// same as manageDfsDirs but only one edits file instead of two
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Util.FileAsURI(new FilePath(dfsDir
				, "name")).ToString());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Util.FileAsURI(new FilePath
				(dfsDir, "namesecondary1")).ToString());
			// blocksize for concat (file size must be multiple of blocksize)
			config.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			// for security to work (fake JobTracker user)
			config.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthToLocal, "RULE:[2:$1@$0](JobTracker@.*FOO.COM)s/@.*//"
				 + "DEFAULT");
			config.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			config.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(config).ManageNameDfsDirs(false).Build();
			cluster.WaitClusterUp();
		}

		/// <summary>Shutdown the cluster</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Run file operations to create edits for all op codes
		/// to be tested.
		/// </summary>
		/// <remarks>
		/// Run file operations to create edits for all op codes
		/// to be tested.
		/// the following op codes are deprecated and therefore not tested:
		/// OP_DATANODE_ADD    ( 5)
		/// OP_DATANODE_REMOVE ( 6)
		/// OP_SET_NS_QUOTA    (11)
		/// OP_CLEAR_NS_QUOTA  (12)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private CheckpointSignature RunOperations()
		{
			Log.Info("Creating edits by performing fs operations");
			// no check, if it's not it throws an exception which is what we want
			DistributedFileSystem dfs = cluster.GetFileSystem();
			DFSTestUtil.RunOperations(cluster, dfs, cluster.GetConfiguration(0), dfs.GetDefaultBlockSize
				(), 0);
			// OP_ROLLING_UPGRADE_START
			cluster.GetNamesystem().GetEditLog().LogStartRollingUpgrade(Time.Now());
			// OP_ROLLING_UPGRADE_FINALIZE
			cluster.GetNamesystem().GetEditLog().LogFinalizeRollingUpgrade(Time.Now());
			// Force a roll so we get an OP_END_LOG_SEGMENT txn
			return cluster.GetNameNodeRpc().RollEditLog();
		}
	}
}
