using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the creation and validation of a checkpoint.</summary>
	public class TestSecurityTokenEditLog
	{
		internal const int NumDataNodes = 1;

		internal const int NumTransactions = 100;

		internal const int NumThreads = 100;

		internal const int opsPerTrans = 3;

		static TestSecurityTokenEditLog()
		{
			// This test creates NUM_THREADS threads and each thread does
			// 2 * NUM_TRANSACTIONS Transactions concurrently.
			// No need to fsync for the purposes of tests. This makes
			// the tests run much faster.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		internal class Transactions : Runnable
		{
			internal readonly FSNamesystem namesystem;

			internal readonly int numTransactions;

			internal short replication = 3;

			internal long blockSize = 64;

			internal Transactions(FSNamesystem ns, int num)
			{
				//
				// an object that does a bunch of transactions
				//
				namesystem = ns;
				numTransactions = num;
			}

			// add a bunch of transactions.
			public virtual void Run()
			{
				FSEditLog editLog = namesystem.GetEditLog();
				for (int i = 0; i < numTransactions; i++)
				{
					try
					{
						string renewer = UserGroupInformation.GetLoginUser().GetUserName();
						Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = namesystem
							.GetDelegationToken(new Text(renewer));
						namesystem.RenewDelegationToken(token);
						namesystem.CancelDelegationToken(token);
						editLog.LogSync();
					}
					catch (IOException e)
					{
						System.Console.Out.WriteLine("Transaction " + i + " encountered exception " + e);
					}
				}
			}
		}

		/// <summary>Tests transaction logging in dfs.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLog()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			try
			{
				conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				for (IEnumerator<URI> it = cluster.GetNameDirs(0).GetEnumerator(); it.HasNext(); )
				{
					FilePath dir = new FilePath(it.Next().GetPath());
					System.Console.Out.WriteLine(dir);
				}
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				// set small size of flush buffer
				editLog.SetOutputBufferCapacity(2048);
				// Create threads and make them run transactions concurrently.
				Sharpen.Thread[] threadId = new Sharpen.Thread[NumThreads];
				for (int i = 0; i < NumThreads; i++)
				{
					TestSecurityTokenEditLog.Transactions trans = new TestSecurityTokenEditLog.Transactions
						(namesystem, NumTransactions);
					threadId[i] = new Sharpen.Thread(trans, "TransactionThread-" + i);
					threadId[i].Start();
				}
				// wait for all transactions to get over
				for (int i_1 = 0; i_1 < NumThreads; i_1++)
				{
					try
					{
						threadId[i_1].Join();
					}
					catch (Exception)
					{
						i_1--;
					}
				}
				// retry 
				editLog.Close();
				// Verify that we can read in all the transactions that we have written.
				// If there were any corruptions, it is likely that the reading in
				// of these transactions will throw an exception.
				//
				namesystem.GetDelegationTokenSecretManager().StopThreads();
				int numKeys = namesystem.GetDelegationTokenSecretManager().GetNumberOfKeys();
				int expectedTransactions = NumThreads * opsPerTrans * NumTransactions + numKeys +
					 2;
				// + 2 for BEGIN and END txns
				foreach (Storage.StorageDirectory sd in fsimage.GetStorage().DirIterable(NNStorage.NameNodeDirType
					.Edits))
				{
					FilePath editFile = NNStorage.GetFinalizedEditsFile(sd, 1, 1 + expectedTransactions
						 - 1);
					System.Console.Out.WriteLine("Verifying file: " + editFile);
					FSEditLogLoader loader = new FSEditLogLoader(namesystem, 0);
					long numEdits = loader.LoadFSEdits(new EditLogFileInputStream(editFile), 1);
					NUnit.Framework.Assert.AreEqual("Verification for " + editFile, expectedTransactions
						, numEdits);
				}
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestEditsForCancelOnTokenExpire()
		{
			long renewInterval = 2000;
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			conf.SetLong(DfsNamenodeDelegationTokenRenewIntervalKey, renewInterval);
			conf.SetLong(DfsNamenodeDelegationTokenMaxLifetimeKey, renewInterval * 2);
			Text renewer = new Text(UserGroupInformation.GetCurrentUser().GetUserName());
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			FSEditLog log = Org.Mockito.Mockito.Mock<FSEditLog>();
			Org.Mockito.Mockito.DoReturn(log).When(fsImage).GetEditLog();
			FSNamesystem fsn = new FSNamesystem(conf, fsImage);
			DelegationTokenSecretManager dtsm = fsn.GetDelegationTokenSecretManager();
			try
			{
				dtsm.StartThreads();
				// get two tokens
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = fsn.GetDelegationToken
					(renewer);
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = fsn.GetDelegationToken
					(renewer);
				DelegationTokenIdentifier ident1 = token1.DecodeIdentifier();
				DelegationTokenIdentifier ident2 = token2.DecodeIdentifier();
				// verify we got the tokens
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogGetDelegationToken
					(Eq(ident1), AnyLong());
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogGetDelegationToken
					(Eq(ident2), AnyLong());
				// this is a little tricky because DTSM doesn't let us set scan interval
				// so need to periodically sleep, then stop/start threads to force scan
				// renew first token 1/2 to expire
				Sharpen.Thread.Sleep(renewInterval / 2);
				fsn.RenewDelegationToken(token2);
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogRenewDelegationToken
					(Eq(ident2), AnyLong());
				// force scan and give it a little time to complete
				dtsm.StopThreads();
				dtsm.StartThreads();
				Sharpen.Thread.Sleep(250);
				// no token has expired yet 
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(0)).LogCancelDelegationToken
					(Eq(ident1));
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(0)).LogCancelDelegationToken
					(Eq(ident2));
				// sleep past expiration of 1st non-renewed token
				Sharpen.Thread.Sleep(renewInterval / 2);
				dtsm.StopThreads();
				dtsm.StartThreads();
				Sharpen.Thread.Sleep(250);
				// non-renewed token should have implicitly been cancelled
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogCancelDelegationToken
					(Eq(ident1));
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(0)).LogCancelDelegationToken
					(Eq(ident2));
				// sleep past expiration of 2nd renewed token
				Sharpen.Thread.Sleep(renewInterval / 2);
				dtsm.StopThreads();
				dtsm.StartThreads();
				Sharpen.Thread.Sleep(250);
				// both tokens should have been implicitly cancelled by now
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogCancelDelegationToken
					(Eq(ident1));
				Org.Mockito.Mockito.Verify(log, Org.Mockito.Mockito.Times(1)).LogCancelDelegationToken
					(Eq(ident2));
			}
			finally
			{
				dtsm.StopThreads();
			}
		}
	}
}
