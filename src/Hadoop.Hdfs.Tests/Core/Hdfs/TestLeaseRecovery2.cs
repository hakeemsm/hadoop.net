using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestLeaseRecovery2
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestLeaseRecovery2));

		private const long BlockSize = 1024;

		private const int FileSize = (int)BlockSize * 2;

		internal const short ReplicationNum = (short)3;

		internal static readonly byte[] buffer = new byte[FileSize];

		private const string fakeUsername = "fakeUser1";

		private const string fakeGroup = "supergroup";

		private static MiniDFSCluster cluster;

		private static DistributedFileSystem dfs;

		private static readonly Configuration conf = new HdfsConfiguration();

		private static readonly int BufSize = conf.GetInt(CommonConfigurationKeys.IoFileBufferSizeKey
			, 4096);

		private const long ShortLeasePeriod = 1000L;

		private const long LongLeasePeriod = 60 * 60 * ShortLeasePeriod;

		/// <summary>start a dfs cluster</summary>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void StartUp()
		{
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(5).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
		}

		/// <summary>stop the cluster</summary>
		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TearDown()
		{
			IOUtils.CloseStream(dfs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test the NameNode's revoke lease on current lease holder function.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestImmediateRecoveryOfLease()
		{
			//create a file
			// write bytes into the file.
			byte[] actual = new byte[FileSize];
			int size = AppendTestUtil.NextInt(FileSize);
			Path filepath = CreateFile("/immediateRecoverLease-shortlease", size, true);
			// set the soft limit to be 1 second so that the
			// namenode triggers lease recovery on next attempt to write-for-open.
			cluster.SetLeasePeriod(ShortLeasePeriod, LongLeasePeriod);
			RecoverLeaseUsingCreate(filepath);
			VerifyFile(dfs, filepath, actual, size);
			//test recoverLease
			// set the soft limit to be 1 hour but recoverLease should
			// close the file immediately
			cluster.SetLeasePeriod(LongLeasePeriod, LongLeasePeriod);
			size = AppendTestUtil.NextInt(FileSize);
			filepath = CreateFile("/immediateRecoverLease-longlease", size, false);
			// test recoverLease from a different client
			RecoverLease(filepath, null);
			VerifyFile(dfs, filepath, actual, size);
			// test recoverlease from the same client
			size = AppendTestUtil.NextInt(FileSize);
			filepath = CreateFile("/immediateRecoverLease-sameclient", size, false);
			// create another file using the same client
			Path filepath1 = new Path(filepath.ToString() + AppendTestUtil.NextInt());
			FSDataOutputStream stm = dfs.Create(filepath1, true, BufSize, ReplicationNum, BlockSize
				);
			// recover the first file
			RecoverLease(filepath, dfs);
			VerifyFile(dfs, filepath, actual, size);
			// continue to write to the second file
			stm.Write(buffer, 0, size);
			stm.Close();
			VerifyFile(dfs, filepath1, actual, size);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseRecoverByAnotherUser()
		{
			byte[] actual = new byte[FileSize];
			cluster.SetLeasePeriod(ShortLeasePeriod, LongLeasePeriod);
			Path filepath = CreateFile("/immediateRecoverLease-x", 0, true);
			RecoverLeaseUsingCreate2(filepath);
			VerifyFile(dfs, filepath, actual, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private Path CreateFile(string filestr, int size, bool triggerLeaseRenewerInterrupt
			)
		{
			AppendTestUtil.Log.Info("filestr=" + filestr);
			Path filepath = new Path(filestr);
			FSDataOutputStream stm = dfs.Create(filepath, true, BufSize, ReplicationNum, BlockSize
				);
			NUnit.Framework.Assert.IsTrue(dfs.dfs.Exists(filestr));
			AppendTestUtil.Log.Info("size=" + size);
			stm.Write(buffer, 0, size);
			// hflush file
			AppendTestUtil.Log.Info("hflush");
			stm.Hflush();
			if (triggerLeaseRenewerInterrupt)
			{
				AppendTestUtil.Log.Info("leasechecker.interruptAndJoin()");
				dfs.dfs.GetLeaseRenewer().InterruptAndJoin();
			}
			return filepath;
		}

		/// <exception cref="System.Exception"/>
		private void RecoverLease(Path filepath, DistributedFileSystem dfs)
		{
			if (dfs == null)
			{
				dfs = (DistributedFileSystem)GetFSAsAnotherUser(conf);
			}
			while (!dfs.RecoverLease(filepath))
			{
				AppendTestUtil.Log.Info("sleep " + 5000 + "ms");
				Sharpen.Thread.Sleep(5000);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private FileSystem GetFSAsAnotherUser(Configuration c)
		{
			return FileSystem.Get(FileSystem.GetDefaultUri(c), c, UserGroupInformation.CreateUserForTesting
				(fakeUsername, new string[] { fakeGroup }).GetUserName());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void RecoverLeaseUsingCreate(Path filepath)
		{
			FileSystem dfs2 = GetFSAsAnotherUser(conf);
			for (int i = 0; i < 10; i++)
			{
				AppendTestUtil.Log.Info("i=" + i);
				try
				{
					dfs2.Create(filepath, false, BufSize, (short)1, BlockSize);
					NUnit.Framework.Assert.Fail("Creation of an existing file should never succeed.");
				}
				catch (FileAlreadyExistsException)
				{
					return;
				}
				catch (AlreadyBeingCreatedException)
				{
					// expected
					return;
				}
				catch (IOException ioe)
				{
					// expected
					AppendTestUtil.Log.Warn("UNEXPECTED ", ioe);
					AppendTestUtil.Log.Info("sleep " + 5000 + "ms");
					try
					{
						Sharpen.Thread.Sleep(5000);
					}
					catch (Exception)
					{
					}
				}
			}
			NUnit.Framework.Assert.Fail("recoverLeaseUsingCreate failed");
		}

		/// <exception cref="System.Exception"/>
		private void RecoverLeaseUsingCreate2(Path filepath)
		{
			FileSystem dfs2 = GetFSAsAnotherUser(conf);
			int size = AppendTestUtil.NextInt(FileSize);
			DistributedFileSystem dfsx = (DistributedFileSystem)dfs2;
			//create file using dfsx
			Path filepath2 = new Path("/immediateRecoverLease-x2");
			FSDataOutputStream stm = dfsx.Create(filepath2, true, BufSize, ReplicationNum, BlockSize
				);
			NUnit.Framework.Assert.IsTrue(dfsx.dfs.Exists("/immediateRecoverLease-x2"));
			try
			{
				Sharpen.Thread.Sleep(10000);
			}
			catch (Exception)
			{
			}
			dfsx.Append(filepath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyFile(FileSystem dfs, Path filepath, byte[] actual, int size)
		{
			AppendTestUtil.Log.Info("Lease for file " + filepath + " is recovered. " + "Validating its contents now..."
				);
			// verify that file-size matches
			NUnit.Framework.Assert.IsTrue("File should be " + size + " bytes, but is actually "
				 + " found to be " + dfs.GetFileStatus(filepath).GetLen() + " bytes", dfs.GetFileStatus
				(filepath).GetLen() == size);
			// verify that there is enough data to read.
			System.Console.Out.WriteLine("File size is good. Now validating sizes from datanodes..."
				);
			FSDataInputStream stmin = dfs.Open(filepath);
			stmin.ReadFully(0, actual, 0, size);
			stmin.Close();
		}

		/// <summary>
		/// This test makes the client does not renew its lease and also
		/// set the hard lease expiration period to be short 1s.
		/// </summary>
		/// <remarks>
		/// This test makes the client does not renew its lease and also
		/// set the hard lease expiration period to be short 1s. Thus triggering
		/// lease expiration to happen while the client is still alive.
		/// The test makes sure that the lease recovery completes and the client
		/// fails if it continues to write to the file.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHardLeaseRecovery()
		{
			//create a file
			string filestr = "/hardLeaseRecovery";
			AppendTestUtil.Log.Info("filestr=" + filestr);
			Path filepath = new Path(filestr);
			FSDataOutputStream stm = dfs.Create(filepath, true, BufSize, ReplicationNum, BlockSize
				);
			NUnit.Framework.Assert.IsTrue(dfs.dfs.Exists(filestr));
			// write bytes into the file.
			int size = AppendTestUtil.NextInt(FileSize);
			AppendTestUtil.Log.Info("size=" + size);
			stm.Write(buffer, 0, size);
			// hflush file
			AppendTestUtil.Log.Info("hflush");
			stm.Hflush();
			// kill the lease renewal thread
			AppendTestUtil.Log.Info("leasechecker.interruptAndJoin()");
			dfs.dfs.GetLeaseRenewer().InterruptAndJoin();
			// set the hard limit to be 1 second 
			cluster.SetLeasePeriod(LongLeasePeriod, ShortLeasePeriod);
			// wait for lease recovery to complete
			LocatedBlocks locatedBlocks;
			do
			{
				Sharpen.Thread.Sleep(ShortLeasePeriod);
				locatedBlocks = dfs.dfs.GetLocatedBlocks(filestr, 0L, size);
			}
			while (locatedBlocks.IsUnderConstruction());
			NUnit.Framework.Assert.AreEqual(size, locatedBlocks.GetFileLength());
			// make sure that the writer thread gets killed
			try
			{
				stm.Write('b');
				stm.Close();
				NUnit.Framework.Assert.Fail("Writer thread should have been killed");
			}
			catch (IOException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			// verify data
			AppendTestUtil.Log.Info("File size is good. Now validating sizes from datanodes..."
				);
			AppendTestUtil.CheckFullFile(dfs, filepath, size, buffer, filestr);
		}

		/// <summary>
		/// This test makes the client does not renew its lease and also
		/// set the soft lease expiration period to be short 1s.
		/// </summary>
		/// <remarks>
		/// This test makes the client does not renew its lease and also
		/// set the soft lease expiration period to be short 1s. Thus triggering
		/// soft lease expiration to happen immediately by having another client
		/// trying to create the same file.
		/// The test makes sure that the lease recovery completes.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSoftLeaseRecovery()
		{
			IDictionary<string, string[]> u2g_map = new Dictionary<string, string[]>(1);
			u2g_map[fakeUsername] = new string[] { fakeGroup };
			DFSTestUtil.UpdateConfWithFakeGroupMapping(conf, u2g_map);
			// Reset default lease periods
			cluster.SetLeasePeriod(HdfsConstants.LeaseSoftlimitPeriod, HdfsConstants.LeaseHardlimitPeriod
				);
			//create a file
			// create a random file name
			string filestr = "/foo" + AppendTestUtil.NextInt();
			AppendTestUtil.Log.Info("filestr=" + filestr);
			Path filepath = new Path(filestr);
			FSDataOutputStream stm = dfs.Create(filepath, true, BufSize, ReplicationNum, BlockSize
				);
			NUnit.Framework.Assert.IsTrue(dfs.dfs.Exists(filestr));
			// write random number of bytes into it.
			int size = AppendTestUtil.NextInt(FileSize);
			AppendTestUtil.Log.Info("size=" + size);
			stm.Write(buffer, 0, size);
			// hflush file
			AppendTestUtil.Log.Info("hflush");
			stm.Hflush();
			AppendTestUtil.Log.Info("leasechecker.interruptAndJoin()");
			dfs.dfs.GetLeaseRenewer().InterruptAndJoin();
			// set the soft limit to be 1 second so that the
			// namenode triggers lease recovery on next attempt to write-for-open.
			cluster.SetLeasePeriod(ShortLeasePeriod, LongLeasePeriod);
			{
				// try to re-open the file before closing the previous handle. This
				// should fail but will trigger lease recovery.
				UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(fakeUsername
					, new string[] { fakeGroup });
				FileSystem dfs2 = DFSTestUtil.GetFileSystemAs(ugi, conf);
				bool done = false;
				for (int i = 0; i < 10 && !done; i++)
				{
					AppendTestUtil.Log.Info("i=" + i);
					try
					{
						dfs2.Create(filepath, false, BufSize, ReplicationNum, BlockSize);
						NUnit.Framework.Assert.Fail("Creation of an existing file should never succeed.");
					}
					catch (FileAlreadyExistsException)
					{
						done = true;
					}
					catch (AlreadyBeingCreatedException ex)
					{
						AppendTestUtil.Log.Info("GOOD! got " + ex.Message);
					}
					catch (IOException ioe)
					{
						AppendTestUtil.Log.Warn("UNEXPECTED IOException", ioe);
					}
					if (!done)
					{
						AppendTestUtil.Log.Info("sleep " + 5000 + "ms");
						try
						{
							Sharpen.Thread.Sleep(5000);
						}
						catch (Exception)
						{
						}
					}
				}
				NUnit.Framework.Assert.IsTrue(done);
			}
			AppendTestUtil.Log.Info("Lease for file " + filepath + " is recovered. " + "Validating its contents now..."
				);
			// verify that file-size matches
			long fileSize = dfs.GetFileStatus(filepath).GetLen();
			NUnit.Framework.Assert.IsTrue("File should be " + size + " bytes, but is actually "
				 + " found to be " + fileSize + " bytes", fileSize == size);
			// verify data
			AppendTestUtil.Log.Info("File size is good. " + "Now validating data and sizes from datanodes..."
				);
			AppendTestUtil.CheckFullFile(dfs, filepath, size, buffer, filestr);
		}

		/// <summary>
		/// This test makes it so the client does not renew its lease and also
		/// set the hard lease expiration period to be short, thus triggering
		/// lease expiration to happen while the client is still alive.
		/// </summary>
		/// <remarks>
		/// This test makes it so the client does not renew its lease and also
		/// set the hard lease expiration period to be short, thus triggering
		/// lease expiration to happen while the client is still alive. The test
		/// also causes the NN to restart after lease recovery has begun, but before
		/// the DNs have completed the blocks. This test verifies that when the NN
		/// comes back up, the client no longer holds the lease.
		/// The test makes sure that the lease recovery completes and the client
		/// fails if it continues to write to the file, even after NN restart.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHardLeaseRecoveryAfterNameNodeRestart()
		{
			HardLeaseRecoveryRestartHelper(false, -1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHardLeaseRecoveryAfterNameNodeRestart2()
		{
			HardLeaseRecoveryRestartHelper(false, 1535);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHardLeaseRecoveryWithRenameAfterNameNodeRestart()
		{
			HardLeaseRecoveryRestartHelper(true, -1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void HardLeaseRecoveryRestartHelper(bool doRename, int size)
		{
			if (size < 0)
			{
				size = AppendTestUtil.NextInt(FileSize + 1);
			}
			//create a file
			string fileStr = "/hardLeaseRecovery";
			AppendTestUtil.Log.Info("filestr=" + fileStr);
			Path filePath = new Path(fileStr);
			FSDataOutputStream stm = dfs.Create(filePath, true, BufSize, ReplicationNum, BlockSize
				);
			NUnit.Framework.Assert.IsTrue(dfs.dfs.Exists(fileStr));
			// write bytes into the file.
			AppendTestUtil.Log.Info("size=" + size);
			stm.Write(buffer, 0, size);
			string originalLeaseHolder = NameNodeAdapter.GetLeaseHolderForPath(cluster.GetNameNode
				(), fileStr);
			NUnit.Framework.Assert.IsFalse("original lease holder should not be the NN", originalLeaseHolder
				.Equals(HdfsServerConstants.NamenodeLeaseHolder));
			// hflush file
			AppendTestUtil.Log.Info("hflush");
			stm.Hflush();
			// check visible length
			HdfsDataInputStream @in = (HdfsDataInputStream)dfs.Open(filePath);
			NUnit.Framework.Assert.AreEqual(size, @in.GetVisibleLength());
			@in.Close();
			if (doRename)
			{
				fileStr += ".renamed";
				Path renamedPath = new Path(fileStr);
				NUnit.Framework.Assert.IsTrue(dfs.Rename(filePath, renamedPath));
				filePath = renamedPath;
			}
			// kill the lease renewal thread
			AppendTestUtil.Log.Info("leasechecker.interruptAndJoin()");
			dfs.dfs.GetLeaseRenewer().InterruptAndJoin();
			// Make sure the DNs don't send a heartbeat for a while, so the blocks
			// won't actually get completed during lease recovery.
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn, true);
			}
			// set the hard limit to be 1 second 
			cluster.SetLeasePeriod(LongLeasePeriod, ShortLeasePeriod);
			// Make sure lease recovery begins.
			Sharpen.Thread.Sleep(HdfsServerConstants.NamenodeLeaseRecheckInterval * 2);
			CheckLease(fileStr, size);
			cluster.RestartNameNode(false);
			CheckLease(fileStr, size);
			// Let the DNs send heartbeats again.
			foreach (DataNode dn_1 in cluster.GetDataNodes())
			{
				DataNodeTestUtils.SetHeartbeatsDisabledForTests(dn_1, false);
			}
			cluster.WaitActive();
			// set the hard limit to be 1 second, to initiate lease recovery. 
			cluster.SetLeasePeriod(LongLeasePeriod, ShortLeasePeriod);
			// wait for lease recovery to complete
			LocatedBlocks locatedBlocks;
			do
			{
				Sharpen.Thread.Sleep(ShortLeasePeriod);
				locatedBlocks = dfs.dfs.GetLocatedBlocks(fileStr, 0L, size);
			}
			while (locatedBlocks.IsUnderConstruction());
			NUnit.Framework.Assert.AreEqual(size, locatedBlocks.GetFileLength());
			// make sure that the client can't write data anymore.
			try
			{
				stm.Write('b');
				stm.Hflush();
				NUnit.Framework.Assert.Fail("Should not be able to flush after we've lost the lease"
					);
			}
			catch (IOException e)
			{
				Log.Info("Expceted exception on write/hflush", e);
			}
			try
			{
				stm.Close();
				NUnit.Framework.Assert.Fail("Should not be able to close after we've lost the lease"
					);
			}
			catch (IOException e)
			{
				Log.Info("Expected exception on close", e);
			}
			// verify data
			AppendTestUtil.Log.Info("File size is good. Now validating sizes from datanodes..."
				);
			AppendTestUtil.CheckFullFile(dfs, filePath, size, buffer, fileStr);
		}

		internal static void CheckLease(string f, int size)
		{
			string holder = NameNodeAdapter.GetLeaseHolderForPath(cluster.GetNameNode(), f);
			if (size == 0)
			{
				NUnit.Framework.Assert.AreEqual("lease holder should null, file is closed", null, 
					holder);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("lease holder should now be the NN", HdfsServerConstants
					.NamenodeLeaseHolder, holder);
			}
		}

		public TestLeaseRecovery2()
		{
			{
				((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LeaseManager.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
			}
		}
	}
}
