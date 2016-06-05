using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public class TestNNWithQJM
	{
		internal readonly Configuration conf = new HdfsConfiguration();

		private MiniJournalCluster mjc = null;

		private readonly Path TestPath = new Path("/test-dir");

		private readonly Path TestPath2 = new Path("/test-dir-2");

		[SetUp]
		public virtual void ResetSystemExit()
		{
			ExitUtil.ResetFirstExitException();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void StartJNs()
		{
			mjc = new MiniJournalCluster.Builder(conf).Build();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void StopJNs()
		{
			if (mjc != null)
			{
				mjc.Shutdown();
				mjc = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLogAndRestart()
		{
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, MiniDFSCluster.GetBaseDirectory() +
				 "/TestNNWithQJM/image");
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI("myjournal"
				).ToString());
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs
				(false).Build();
			try
			{
				cluster.GetFileSystem().Mkdirs(TestPath);
				// Restart the NN and make sure the edit was persisted
				// and loaded again
				cluster.RestartNameNode();
				NUnit.Framework.Assert.IsTrue(cluster.GetFileSystem().Exists(TestPath));
				cluster.GetFileSystem().Mkdirs(TestPath2);
				// Restart the NN again and make sure both edits are persisted.
				cluster.RestartNameNode();
				NUnit.Framework.Assert.IsTrue(cluster.GetFileSystem().Exists(TestPath));
				NUnit.Framework.Assert.IsTrue(cluster.GetFileSystem().Exists(TestPath2));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNewNamenodeTakesOverWriter()
		{
			FilePath nn1Dir = new FilePath(MiniDFSCluster.GetBaseDirectory() + "/TestNNWithQJM/image-nn1"
				);
			FilePath nn2Dir = new FilePath(MiniDFSCluster.GetBaseDirectory() + "/TestNNWithQJM/image-nn2"
				);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nn1Dir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI("myjournal"
				).ToString());
			// Start the cluster once to generate the dfs dirs
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs
				(false).CheckExitOnShutdown(false).Build();
			// Shutdown the cluster before making a copy of the namenode dir
			// to release all file locks, otherwise, the copy will fail on
			// some platforms.
			cluster.Shutdown();
			try
			{
				// Start a second NN pointed to the same quorum.
				// We need to copy the image dir from the first NN -- or else
				// the new NN will just be rejected because of Namespace mismatch.
				FileUtil.FullyDelete(nn2Dir);
				FileUtil.Copy(nn1Dir, FileSystem.GetLocal(conf).GetRaw(), new Path(nn2Dir.GetAbsolutePath
					()), false, conf);
				// Start the cluster again
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).ManageNameDfsDirs
					(false).CheckExitOnShutdown(false).Build();
				cluster.GetFileSystem().Mkdirs(TestPath);
				Configuration conf2 = new Configuration();
				conf2.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nn2Dir.GetAbsolutePath());
				conf2.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI("myjournal"
					).ToString());
				MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf2).NumDataNodes(0).Format
					(false).ManageNameDfsDirs(false).Build();
				// Check that the new cluster sees the edits made on the old cluster
				try
				{
					NUnit.Framework.Assert.IsTrue(cluster2.GetFileSystem().Exists(TestPath));
				}
				finally
				{
					cluster2.Shutdown();
				}
				// Check that, if we try to write to the old NN
				// that it aborts.
				try
				{
					cluster.GetFileSystem().Mkdirs(new Path("/x"));
					NUnit.Framework.Assert.Fail("Did not abort trying to write to a fenced NN");
				}
				catch (RemoteException re)
				{
					GenericTestUtils.AssertExceptionContains("Could not sync enough journals to persistent storage"
						, re);
				}
			}
			finally
			{
			}
		}

		//cluster.shutdown();
		/// <exception cref="System.Exception"/>
		public virtual void TestMismatchedNNIsRejected()
		{
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, MiniDFSCluster.GetBaseDirectory() +
				 "/TestNNWithQJM/image");
			string defaultEditsDir = conf.Get(DFSConfigKeys.DfsNamenodeEditsDirKey);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI("myjournal"
				).ToString());
			// Start a NN, so the storage is formatted -- both on-disk
			// and QJM.
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs
				(false).Build();
			cluster.Shutdown();
			// Reformat just the on-disk portion
			Configuration onDiskOnly = new Configuration(conf);
			onDiskOnly.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, defaultEditsDir);
			NameNode.Format(onDiskOnly);
			// Start the NN - should fail because the JNs are still formatted
			// with the old namespace ID.
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs(false
					).Format(false).Build();
				NUnit.Framework.Assert.Fail("New NN with different namespace should have been rejected"
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Unable to start log segment 1: too few journals"
					, ioe);
			}
		}
	}
}
