using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the creation and validation of a checkpoint.</summary>
	public class TestCheckPointForSecurityTokens
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 4096;

		internal const int fileSize = 8192;

		internal const int numDatanodes = 3;

		internal short replication = 3;

		internal MiniDFSCluster cluster = null;

		/// <exception cref="System.IO.IOException"/>
		private void CancelToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			cluster.GetNamesystem().CancelDelegationToken(token);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RenewToken(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> token)
		{
			cluster.GetNamesystem().RenewDelegationToken(token);
		}

		/// <summary>Tests save namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveNamespace()
		{
			DistributedFileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				string renewer = UserGroupInformation.GetLoginUser().GetUserName();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = namesystem
					.GetDelegationToken(new Text(renewer));
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = namesystem
					.GetDelegationToken(new Text(renewer));
				// Saving image without safe mode should fail
				DFSAdmin admin = new DFSAdmin(conf);
				string[] args = new string[] { "-saveNamespace" };
				// verify that the edits file is NOT empty
				NameNode nn = cluster.GetNameNode();
				foreach (Storage.StorageDirectory sd in nn.GetFSImage().GetStorage().DirIterable(
					null))
				{
					FileJournalManager.EditLogFile log = FSImageTestUtil.FindLatestEditsLog(sd);
					NUnit.Framework.Assert.IsTrue(log.IsInProgress());
					log.ValidateLog();
					long numTransactions = (log.GetLastTxId() - log.GetFirstTxId()) + 1;
					NUnit.Framework.Assert.AreEqual("In-progress log " + log + " should have 5 transactions"
						, 5, numTransactions);
				}
				// Saving image in safe mode should succeed
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				try
				{
					admin.Run(args);
				}
				catch (Exception e)
				{
					throw new IOException(e.Message);
				}
				// verify that the edits file is empty except for the START txn
				foreach (Storage.StorageDirectory sd_1 in nn.GetFSImage().GetStorage().DirIterable
					(null))
				{
					FileJournalManager.EditLogFile log = FSImageTestUtil.FindLatestEditsLog(sd_1);
					NUnit.Framework.Assert.IsTrue(log.IsInProgress());
					log.ValidateLog();
					long numTransactions = (log.GetLastTxId() - log.GetFirstTxId()) + 1;
					NUnit.Framework.Assert.AreEqual("In-progress log " + log + " should only have START txn"
						, 1, numTransactions);
				}
				// restart cluster
				cluster.Shutdown();
				cluster = null;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				//Should be able to renew & cancel the delegation token after cluster restart
				try
				{
					RenewToken(token1);
					RenewToken(token2);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Could not renew or cancel the token");
				}
				namesystem = cluster.GetNamesystem();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token3 = namesystem
					.GetDelegationToken(new Text(renewer));
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token4 = namesystem
					.GetDelegationToken(new Text(renewer));
				// restart cluster again
				cluster.Shutdown();
				cluster = null;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				namesystem = cluster.GetNamesystem();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token5 = namesystem
					.GetDelegationToken(new Text(renewer));
				try
				{
					RenewToken(token1);
					RenewToken(token2);
					RenewToken(token3);
					RenewToken(token4);
					RenewToken(token5);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Could not renew or cancel the token");
				}
				// restart cluster again
				cluster.Shutdown();
				cluster = null;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
					).Build();
				cluster.WaitActive();
				namesystem = cluster.GetNamesystem();
				try
				{
					RenewToken(token1);
					CancelToken(token1);
					RenewToken(token2);
					CancelToken(token2);
					RenewToken(token3);
					CancelToken(token3);
					RenewToken(token4);
					CancelToken(token4);
					RenewToken(token5);
					CancelToken(token5);
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Could not renew or cancel the token");
				}
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
