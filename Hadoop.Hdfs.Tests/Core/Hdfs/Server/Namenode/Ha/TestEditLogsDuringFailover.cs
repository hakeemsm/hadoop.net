using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Test cases for the handling of edit logs during failover
	/// and startup of the standby node.
	/// </summary>
	public class TestEditLogsDuringFailover
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestEditLogsDuringFailover
			));

		private const int NumDirsInLog = 5;

		static TestEditLogsDuringFailover()
		{
			// No need to fsync for the purposes of tests. This makes
			// the tests run much faster.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStartup()
		{
			Configuration conf = new Configuration();
			HAUtil.SetAllowStandbyReads(conf, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			try
			{
				// During HA startup, both nodes should be in
				// standby and we shouldn't have any edits files
				// in any edits directory!
				IList<URI> allDirs = Lists.NewArrayList();
				Sharpen.Collections.AddAll(allDirs, cluster.GetNameDirs(0));
				Sharpen.Collections.AddAll(allDirs, cluster.GetNameDirs(1));
				allDirs.AddItem(cluster.GetSharedEditsDir(0, 1));
				AssertNoEditFiles(allDirs);
				// Set the first NN to active, make sure it creates edits
				// in its own dirs and the shared dir. The standby
				// should still have no edits!
				cluster.TransitionToActive(0);
				AssertEditFiles(cluster.GetNameDirs(0), NNStorage.GetInProgressEditsFileName(1));
				AssertEditFiles(Sharpen.Collections.SingletonList(cluster.GetSharedEditsDir(0, 1)
					), NNStorage.GetInProgressEditsFileName(1));
				AssertNoEditFiles(cluster.GetNameDirs(1));
				cluster.GetNameNode(0).GetRpcServer().Mkdirs("/test", FsPermission.CreateImmutable
					((short)0x1ed), true);
				// Restarting the standby should not finalize any edits files
				// in the shared directory when it starts up!
				cluster.RestartNameNode(1);
				AssertEditFiles(cluster.GetNameDirs(0), NNStorage.GetInProgressEditsFileName(1));
				AssertEditFiles(Sharpen.Collections.SingletonList(cluster.GetSharedEditsDir(0, 1)
					), NNStorage.GetInProgressEditsFileName(1));
				AssertNoEditFiles(cluster.GetNameDirs(1));
				// Additionally it should not have applied any in-progress logs
				// at start-up -- otherwise, it would have read half-way into
				// the current log segment, and on the next roll, it would have to
				// either replay starting in the middle of the segment (not allowed)
				// or double-replay the edits (incorrect).
				NUnit.Framework.Assert.IsNull(NameNodeAdapter.GetFileInfo(cluster.GetNameNode(1), 
					"/test", true));
				cluster.GetNameNode(0).GetRpcServer().Mkdirs("/test2", FsPermission.CreateImmutable
					((short)0x1ed), true);
				// If we restart NN0, it'll come back as standby, and we can
				// transition NN1 to active and make sure it reads edits correctly at this point.
				cluster.RestartNameNode(0);
				cluster.TransitionToActive(1);
				// NN1 should have both the edits that came before its restart, and the edits that
				// came after its restart.
				NUnit.Framework.Assert.IsNotNull(NameNodeAdapter.GetFileInfo(cluster.GetNameNode(
					1), "/test", true));
				NUnit.Framework.Assert.IsNotNull(NameNodeAdapter.GetFileInfo(cluster.GetNameNode(
					1), "/test2", true));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestFailoverFinalizesAndReadsInProgress(bool partialTxAtEnd)
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			try
			{
				// Create a fake in-progress edit-log in the shared directory
				URI sharedUri = cluster.GetSharedEditsDir(0, 1);
				FilePath sharedDir = new FilePath(sharedUri.GetPath(), "current");
				FSNamesystem fsn = cluster.GetNamesystem(0);
				FSImageTestUtil.CreateAbortedLogWithMkdirs(sharedDir, NumDirsInLog, 1, fsn.GetFSDirectory
					().GetLastInodeId() + 1);
				AssertEditFiles(Sharpen.Collections.SingletonList(sharedUri), NNStorage.GetInProgressEditsFileName
					(1));
				if (partialTxAtEnd)
				{
					FileOutputStream outs = null;
					try
					{
						FilePath editLogFile = new FilePath(sharedDir, NNStorage.GetInProgressEditsFileName
							(1));
						outs = new FileOutputStream(editLogFile, true);
						outs.Write(new byte[] { unchecked((int)(0x18)), unchecked((int)(0x00)), unchecked(
							(int)(0x00)), unchecked((int)(0x00)) });
						Log.Error("editLogFile = " + editLogFile);
					}
					finally
					{
						IOUtils.Cleanup(Log, outs);
					}
				}
				// Transition one of the NNs to active
				cluster.TransitionToActive(0);
				// In the transition to active, it should have read the log -- and
				// hence see one of the dirs we made in the fake log.
				string testPath = "/dir" + NumDirsInLog;
				NUnit.Framework.Assert.IsNotNull(cluster.GetNameNode(0).GetRpcServer().GetFileInfo
					(testPath));
				// It also should have finalized that log in the shared directory and started
				// writing to a new one at the next txid.
				AssertEditFiles(Sharpen.Collections.SingletonList(sharedUri), NNStorage.GetFinalizedEditsFileName
					(1, NumDirsInLog + 1), NNStorage.GetInProgressEditsFileName(NumDirsInLog + 2));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverFinalizesAndReadsInProgressSimple()
		{
			TestFailoverFinalizesAndReadsInProgress(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverFinalizesAndReadsInProgressWithPartialTxAtEnd()
		{
			TestFailoverFinalizesAndReadsInProgress(true);
		}

		/// <summary>Check that no edits files are present in the given storage dirs.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void AssertNoEditFiles(IEnumerable<URI> dirs)
		{
			AssertEditFiles(dirs, new string[] {  });
		}

		/// <summary>
		/// Check that the given list of edits files are present in the given storage
		/// dirs.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void AssertEditFiles(IEnumerable<URI> dirs, params string[] files)
		{
			foreach (URI u in dirs)
			{
				FilePath editDirRoot = new FilePath(u.GetPath());
				FilePath editDir = new FilePath(editDirRoot, "current");
				GenericTestUtils.AssertExists(editDir);
				if (files.Length == 0)
				{
					Log.Info("Checking no edit files exist in " + editDir);
				}
				else
				{
					Log.Info("Checking for following edit files in " + editDir + ": " + Joiner.On(","
						).Join(files));
				}
				GenericTestUtils.AssertGlobEquals(editDir, "edits_.*", files);
			}
		}
	}
}
