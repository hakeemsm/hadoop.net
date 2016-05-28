using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Hamcrest;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFileTruncate
	{
		static TestFileTruncate()
		{
			GenericTestUtils.SetLogLevel(NameNode.stateChangeLog, Level.All);
			GenericTestUtils.SetLogLevel(FSEditLogLoader.Log, Level.All);
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(TestFileTruncate));

		internal const int BlockSize = 4;

		internal const short Replication = 3;

		internal const int DatanodeNum = 3;

		internal const int SuccessAttempts = 300;

		internal const int RecoveryAttempts = 600;

		internal const long Sleep = 100L;

		internal const long LowSoftlimit = 100L;

		internal const long LowHardlimit = 200L;

		internal const int ShortHeartbeat = 1;

		internal static Configuration conf;

		internal static MiniDFSCluster cluster;

		internal static DistributedFileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void StartUp()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, ShortHeartbeat);
			conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).Format(true).NumDataNodes(DatanodeNum)
				.NameNodePort(NameNode.DefaultPort).WaitSafeMode(true).Build();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TearDown()
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

		/// <summary>Truncate files of different sizes byte by byte.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicTruncate()
		{
			int startingFileSize = 3 * BlockSize;
			Path parent = new Path("/test");
			fs.Mkdirs(parent);
			fs.SetQuota(parent, 100, 1000);
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			for (int fileLength = startingFileSize; fileLength > 0; fileLength -= BlockSize -
				 1)
			{
				for (int toTruncate = 0; toTruncate <= fileLength; toTruncate++)
				{
					Path p = new Path(parent, "testBasicTruncate" + fileLength);
					WriteContents(contents, fileLength, p);
					int newLength = fileLength - toTruncate;
					bool isReady = fs.Truncate(p, newLength);
					Log.Info("fileLength=" + fileLength + ", newLength=" + newLength + ", toTruncate="
						 + toTruncate + ", isReady=" + isReady);
					NUnit.Framework.Assert.AreEqual("File must be closed for zero truncate" + " or truncating at the block boundary"
						, isReady, toTruncate == 0 || newLength % BlockSize == 0);
					if (!isReady)
					{
						CheckBlockRecovery(p);
					}
					ContentSummary cs = fs.GetContentSummary(parent);
					NUnit.Framework.Assert.AreEqual("Bad disk space usage", cs.GetSpaceConsumed(), newLength
						 * Replication);
					// validate the file content
					CheckFullFile(p, newLength, contents);
				}
			}
			fs.Delete(parent, true);
		}

		/// <summary>Truncate the same file multiple times until its size is zero.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleTruncate()
		{
			Path dir = new Path("/testMultipleTruncate");
			fs.Mkdirs(dir);
			Path p = new Path(dir, "file");
			byte[] data = new byte[100 * BlockSize];
			DFSUtil.GetRandom().NextBytes(data);
			WriteContents(data, data.Length, p);
			for (int n = data.Length; n > 0; )
			{
				int newLength = DFSUtil.GetRandom().Next(n);
				bool isReady = fs.Truncate(p, newLength);
				Log.Info("newLength=" + newLength + ", isReady=" + isReady);
				NUnit.Framework.Assert.AreEqual("File must be closed for truncating at the block boundary"
					, isReady, newLength % BlockSize == 0);
				NUnit.Framework.Assert.AreEqual("Truncate is not idempotent", isReady, fs.Truncate
					(p, newLength));
				if (!isReady)
				{
					CheckBlockRecovery(p);
				}
				CheckFullFile(p, newLength, data);
				n = newLength;
			}
			fs.Delete(dir, true);
		}

		/// <summary>Truncate the same file multiple times until its size is zero.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotTruncateThenDeleteSnapshot()
		{
			Path dir = new Path("/testSnapshotTruncateThenDeleteSnapshot");
			fs.Mkdirs(dir);
			fs.AllowSnapshot(dir);
			Path p = new Path(dir, "file");
			byte[] data = new byte[BlockSize];
			DFSUtil.GetRandom().NextBytes(data);
			WriteContents(data, data.Length, p);
			string snapshot = "s0";
			fs.CreateSnapshot(dir, snapshot);
			Block lastBlock = GetLocatedBlocks(p).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			int newLength = data.Length - 1;
			System.Diagnostics.Debug.Assert(newLength % BlockSize != 0, " newLength must not be multiple of BLOCK_SIZE"
				);
			bool isReady = fs.Truncate(p, newLength);
			Log.Info("newLength=" + newLength + ", isReady=" + isReady);
			NUnit.Framework.Assert.AreEqual("File must be closed for truncating at the block boundary"
				, isReady, newLength % BlockSize == 0);
			fs.DeleteSnapshot(dir, snapshot);
			if (!isReady)
			{
				CheckBlockRecovery(p);
			}
			CheckFullFile(p, newLength, data);
			AssertBlockNotPresent(lastBlock);
			fs.Delete(dir, true);
		}

		/// <summary>
		/// Truncate files and then run other operations such as
		/// rename, set replication, set permission, etc.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateWithOtherOperations()
		{
			Path dir = new Path("/testTruncateOtherOperations");
			fs.Mkdirs(dir);
			Path p = new Path(dir, "file");
			byte[] data = new byte[2 * BlockSize];
			DFSUtil.GetRandom().NextBytes(data);
			WriteContents(data, data.Length, p);
			int newLength = data.Length - 1;
			bool isReady = fs.Truncate(p, newLength);
			NUnit.Framework.Assert.IsFalse(isReady);
			fs.SetReplication(p, (short)(Replication - 1));
			fs.SetPermission(p, FsPermission.CreateImmutable((short)0x124));
			Path q = new Path(dir, "newFile");
			fs.Rename(p, q);
			CheckBlockRecovery(q);
			CheckFullFile(q, newLength, data);
			cluster.RestartNameNode();
			CheckFullFile(q, newLength, data);
			fs.Delete(dir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotWithAppendTruncate()
		{
			TestSnapshotWithAppendTruncate(0, 1, 2);
			TestSnapshotWithAppendTruncate(0, 2, 1);
			TestSnapshotWithAppendTruncate(1, 0, 2);
			TestSnapshotWithAppendTruncate(1, 2, 0);
			TestSnapshotWithAppendTruncate(2, 0, 1);
			TestSnapshotWithAppendTruncate(2, 1, 0);
		}

		/// <summary>Create three snapshots with appended and truncated file.</summary>
		/// <remarks>
		/// Create three snapshots with appended and truncated file.
		/// Delete snapshots in the specified order and verify that
		/// remaining snapshots are still readable.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void TestSnapshotWithAppendTruncate(params int[] deleteOrder)
		{
			FSDirectory fsDir = cluster.GetNamesystem().GetFSDirectory();
			Path parent = new Path("/test");
			fs.Mkdirs(parent);
			fs.SetQuota(parent, 100, 1000);
			fs.AllowSnapshot(parent);
			string truncateFile = "testSnapshotWithAppendTruncate";
			Path src = new Path(parent, truncateFile);
			int[] length = new int[4];
			length[0] = 2 * BlockSize + BlockSize / 2;
			DFSTestUtil.CreateFile(fs, src, 64, length[0], BlockSize, Replication, 0L);
			Block firstBlk = GetLocatedBlocks(src).Get(0).GetBlock().GetLocalBlock();
			Path[] snapshotFiles = new Path[4];
			// Diskspace consumed should be 10 bytes * 3. [blk 1,2,3]
			ContentSummary contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(30L));
			// Add file to snapshot and append
			string[] ss = new string[] { "ss0", "ss1", "ss2", "ss3" };
			Path snapshotDir = fs.CreateSnapshot(parent, ss[0]);
			snapshotFiles[0] = new Path(snapshotDir, truncateFile);
			length[1] = length[2] = length[0] + BlockSize + 1;
			DFSTestUtil.AppendFile(fs, src, BlockSize + 1);
			Block lastBlk = GetLocatedBlocks(src).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			// Diskspace consumed should be 15 bytes * 3. [blk 1,2,3,4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(45L));
			// Create another snapshot without changes
			snapshotDir = fs.CreateSnapshot(parent, ss[1]);
			snapshotFiles[1] = new Path(snapshotDir, truncateFile);
			// Create another snapshot and append
			snapshotDir = fs.CreateSnapshot(parent, ss[2]);
			snapshotFiles[2] = new Path(snapshotDir, truncateFile);
			DFSTestUtil.AppendFile(fs, src, BlockSize - 1 + BlockSize / 2);
			Block appendedBlk = GetLocatedBlocks(src).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			// Diskspace consumed should be 20 bytes * 3. [blk 1,2,3,4,5]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(60L));
			// Truncate to block boundary
			int newLength = length[0] + BlockSize / 2;
			bool isReady = fs.Truncate(src, newLength);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			AssertFileLength(snapshotFiles[2], length[2]);
			AssertFileLength(snapshotFiles[1], length[1]);
			AssertFileLength(snapshotFiles[0], length[0]);
			AssertBlockNotPresent(appendedBlk);
			// Diskspace consumed should be 16 bytes * 3. [blk 1,2,3 SS:4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(48L));
			// Truncate full block again
			newLength = length[0] - BlockSize / 2;
			isReady = fs.Truncate(src, newLength);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			AssertFileLength(snapshotFiles[2], length[2]);
			AssertFileLength(snapshotFiles[1], length[1]);
			AssertFileLength(snapshotFiles[0], length[0]);
			// Diskspace consumed should be 16 bytes * 3. [blk 1,2 SS:3,4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(48L));
			// Truncate half of the last block
			newLength -= BlockSize / 2;
			isReady = fs.Truncate(src, newLength);
			NUnit.Framework.Assert.IsFalse("Recovery is expected.", isReady);
			CheckBlockRecovery(src);
			AssertFileLength(snapshotFiles[2], length[2]);
			AssertFileLength(snapshotFiles[1], length[1]);
			AssertFileLength(snapshotFiles[0], length[0]);
			Block replacedBlk = GetLocatedBlocks(src).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			// Diskspace consumed should be 16 bytes * 3. [blk 1,6 SS:2,3,4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(54L));
			snapshotDir = fs.CreateSnapshot(parent, ss[3]);
			snapshotFiles[3] = new Path(snapshotDir, truncateFile);
			length[3] = newLength;
			// Delete file. Should still be able to read snapshots
			int numINodes = fsDir.GetInodeMapSize();
			isReady = fs.Delete(src, false);
			NUnit.Framework.Assert.IsTrue("Delete failed.", isReady);
			AssertFileLength(snapshotFiles[3], length[3]);
			AssertFileLength(snapshotFiles[2], length[2]);
			AssertFileLength(snapshotFiles[1], length[1]);
			AssertFileLength(snapshotFiles[0], length[0]);
			NUnit.Framework.Assert.AreEqual("Number of INodes should not change", numINodes, 
				fsDir.GetInodeMapSize());
			fs.DeleteSnapshot(parent, ss[3]);
			AssertBlockExists(firstBlk);
			AssertBlockExists(lastBlk);
			AssertBlockNotPresent(replacedBlk);
			// Diskspace consumed should be 16 bytes * 3. [SS:1,2,3,4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(48L));
			// delete snapshots in the specified order
			fs.DeleteSnapshot(parent, ss[deleteOrder[0]]);
			AssertFileLength(snapshotFiles[deleteOrder[1]], length[deleteOrder[1]]);
			AssertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
			AssertBlockExists(firstBlk);
			AssertBlockExists(lastBlk);
			NUnit.Framework.Assert.AreEqual("Number of INodes should not change", numINodes, 
				fsDir.GetInodeMapSize());
			// Diskspace consumed should be 16 bytes * 3. [SS:1,2,3,4]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(48L));
			fs.DeleteSnapshot(parent, ss[deleteOrder[1]]);
			AssertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
			AssertBlockExists(firstBlk);
			contentSummary = fs.GetContentSummary(parent);
			if (fs.Exists(snapshotFiles[0]))
			{
				// Diskspace consumed should be 0 bytes * 3. [SS:1,2,3]
				AssertBlockNotPresent(lastBlk);
				Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(36L));
			}
			else
			{
				// Diskspace consumed should be 48 bytes * 3. [SS:1,2,3,4]
				Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(48L));
			}
			NUnit.Framework.Assert.AreEqual("Number of INodes should not change", numINodes, 
				fsDir.GetInodeMapSize());
			fs.DeleteSnapshot(parent, ss[deleteOrder[2]]);
			AssertBlockNotPresent(firstBlk);
			AssertBlockNotPresent(lastBlk);
			// Diskspace consumed should be 0 bytes * 3. []
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(0L));
			Assert.AssertNotEquals("Number of INodes should change", numINodes, fsDir.GetInodeMapSize
				());
		}

		/// <summary>Create three snapshots with file truncated 3 times.</summary>
		/// <remarks>
		/// Create three snapshots with file truncated 3 times.
		/// Delete snapshots in the specified order and verify that
		/// remaining snapshots are still readable.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSnapshotWithTruncates()
		{
			TestSnapshotWithTruncates(0, 1, 2);
			TestSnapshotWithTruncates(0, 2, 1);
			TestSnapshotWithTruncates(1, 0, 2);
			TestSnapshotWithTruncates(1, 2, 0);
			TestSnapshotWithTruncates(2, 0, 1);
			TestSnapshotWithTruncates(2, 1, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void TestSnapshotWithTruncates(params int[] deleteOrder)
		{
			Path parent = new Path("/test");
			fs.Mkdirs(parent);
			fs.SetQuota(parent, 100, 1000);
			fs.AllowSnapshot(parent);
			string truncateFile = "testSnapshotWithTruncates";
			Path src = new Path(parent, truncateFile);
			int[] length = new int[3];
			length[0] = 3 * BlockSize;
			DFSTestUtil.CreateFile(fs, src, 64, length[0], BlockSize, Replication, 0L);
			Block firstBlk = GetLocatedBlocks(src).Get(0).GetBlock().GetLocalBlock();
			Block lastBlk = GetLocatedBlocks(src).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			Path[] snapshotFiles = new Path[3];
			// Diskspace consumed should be 12 bytes * 3. [blk 1,2,3]
			ContentSummary contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(36L));
			// Add file to snapshot and append
			string[] ss = new string[] { "ss0", "ss1", "ss2" };
			Path snapshotDir = fs.CreateSnapshot(parent, ss[0]);
			snapshotFiles[0] = new Path(snapshotDir, truncateFile);
			length[1] = 2 * BlockSize;
			bool isReady = fs.Truncate(src, 2 * BlockSize);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			// Diskspace consumed should be 12 bytes * 3. [blk 1,2 SS:3]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(36L));
			snapshotDir = fs.CreateSnapshot(parent, ss[1]);
			snapshotFiles[1] = new Path(snapshotDir, truncateFile);
			// Create another snapshot with truncate
			length[2] = BlockSize + BlockSize / 2;
			isReady = fs.Truncate(src, BlockSize + BlockSize / 2);
			NUnit.Framework.Assert.IsFalse("Recovery is expected.", isReady);
			CheckBlockRecovery(src);
			snapshotDir = fs.CreateSnapshot(parent, ss[2]);
			snapshotFiles[2] = new Path(snapshotDir, truncateFile);
			AssertFileLength(snapshotFiles[0], length[0]);
			AssertBlockExists(lastBlk);
			// Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(42L));
			fs.DeleteSnapshot(parent, ss[deleteOrder[0]]);
			AssertFileLength(snapshotFiles[deleteOrder[1]], length[deleteOrder[1]]);
			AssertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
			AssertFileLength(src, length[2]);
			AssertBlockExists(firstBlk);
			contentSummary = fs.GetContentSummary(parent);
			if (fs.Exists(snapshotFiles[0]))
			{
				// Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
				Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(42L));
				AssertBlockExists(lastBlk);
			}
			else
			{
				// Diskspace consumed should be 10 bytes * 3. [blk 1,4 SS:2]
				Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(30L));
				AssertBlockNotPresent(lastBlk);
			}
			fs.DeleteSnapshot(parent, ss[deleteOrder[1]]);
			AssertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
			AssertFileLength(src, length[2]);
			AssertBlockExists(firstBlk);
			contentSummary = fs.GetContentSummary(parent);
			if (fs.Exists(snapshotFiles[0]))
			{
				// Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
				Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(42L));
				AssertBlockExists(lastBlk);
			}
			else
			{
				if (fs.Exists(snapshotFiles[1]))
				{
					// Diskspace consumed should be 10 bytes * 3. [blk 1,4 SS:2]
					Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(30L));
					AssertBlockNotPresent(lastBlk);
				}
				else
				{
					// Diskspace consumed should be 6 bytes * 3. [blk 1,4 SS:]
					Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(18L));
					AssertBlockNotPresent(lastBlk);
				}
			}
			fs.DeleteSnapshot(parent, ss[deleteOrder[2]]);
			AssertFileLength(src, length[2]);
			AssertBlockExists(firstBlk);
			// Diskspace consumed should be 6 bytes * 3. [blk 1,4 SS:]
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(18L));
			Assert.AssertThat(contentSummary.GetLength(), IS.Is(6L));
			fs.Delete(src, false);
			AssertBlockNotPresent(firstBlk);
			// Diskspace consumed should be 0 bytes * 3. []
			contentSummary = fs.GetContentSummary(parent);
			Assert.AssertThat(contentSummary.GetSpaceConsumed(), IS.Is(0L));
		}

		/// <summary>Failure / recovery test for truncate.</summary>
		/// <remarks>
		/// Failure / recovery test for truncate.
		/// In this failure the DNs fail to recover the blocks and the NN triggers
		/// lease recovery.
		/// File stays in RecoveryInProgress until DataNodes report recovery.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateFailure()
		{
			int startingFileSize = 2 * BlockSize + BlockSize / 2;
			int toTruncate = 1;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			Path dir = new Path("/dir");
			Path p = new Path(dir, "testTruncateFailure");
			{
				FSDataOutputStream @out = fs.Create(p, false, BlockSize, Replication, BlockSize);
				@out.Write(contents, 0, startingFileSize);
				try
				{
					fs.Truncate(p, 0);
					NUnit.Framework.Assert.Fail("Truncate must fail on open file.");
				}
				catch (IOException expected)
				{
					GenericTestUtils.AssertExceptionContains("Failed to TRUNCATE_FILE", expected);
				}
				finally
				{
					@out.Close();
				}
			}
			{
				FSDataOutputStream @out = fs.Append(p);
				try
				{
					fs.Truncate(p, 0);
					NUnit.Framework.Assert.Fail("Truncate must fail for append.");
				}
				catch (IOException expected)
				{
					GenericTestUtils.AssertExceptionContains("Failed to TRUNCATE_FILE", expected);
				}
				finally
				{
					@out.Close();
				}
			}
			try
			{
				fs.Truncate(p, -1);
				NUnit.Framework.Assert.Fail("Truncate must fail for a negative new length.");
			}
			catch (HadoopIllegalArgumentException expected)
			{
				GenericTestUtils.AssertExceptionContains("Cannot truncate to a negative file size"
					, expected);
			}
			try
			{
				fs.Truncate(p, startingFileSize + 1);
				NUnit.Framework.Assert.Fail("Truncate must fail for a larger new length.");
			}
			catch (Exception expected)
			{
				GenericTestUtils.AssertExceptionContains("Cannot truncate to a larger file size", 
					expected);
			}
			try
			{
				fs.Truncate(dir, 0);
				NUnit.Framework.Assert.Fail("Truncate must fail for a directory.");
			}
			catch (Exception expected)
			{
				GenericTestUtils.AssertExceptionContains("Path is not a file", expected);
			}
			try
			{
				fs.Truncate(new Path(dir, "non-existing"), 0);
				NUnit.Framework.Assert.Fail("Truncate must fail for a non-existing file.");
			}
			catch (Exception expected)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist", expected);
			}
			fs.SetPermission(p, FsPermission.CreateImmutable((short)0x1b4));
			{
				UserGroupInformation fooUgi = UserGroupInformation.CreateUserForTesting("foo", new 
					string[] { "foo" });
				try
				{
					FileSystem foofs = DFSTestUtil.GetFileSystemAs(fooUgi, conf);
					foofs.Truncate(p, 0);
					NUnit.Framework.Assert.Fail("Truncate must fail for no WRITE permission.");
				}
				catch (Exception expected)
				{
					GenericTestUtils.AssertExceptionContains("Permission denied", expected);
				}
			}
			cluster.ShutdownDataNodes();
			NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem()).SetLeasePeriod(LowSoftlimit
				, LowHardlimit);
			int newLength = startingFileSize - toTruncate;
			bool isReady = fs.Truncate(p, newLength);
			Assert.AssertThat("truncate should have triggered block recovery.", isReady, IS.Is
				(false));
			{
				try
				{
					fs.Truncate(p, 0);
					NUnit.Framework.Assert.Fail("Truncate must fail since a trancate is already in pregress."
						);
				}
				catch (IOException expected)
				{
					GenericTestUtils.AssertExceptionContains("Failed to TRUNCATE_FILE", expected);
				}
			}
			bool recoveryTriggered = false;
			for (int i = 0; i < RecoveryAttempts; i++)
			{
				string leaseHolder = NameNodeAdapter.GetLeaseHolderForPath(cluster.GetNameNode(), 
					p.ToUri().GetPath());
				if (leaseHolder.Equals(HdfsServerConstants.NamenodeLeaseHolder))
				{
					recoveryTriggered = true;
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(Sleep);
				}
				catch (Exception)
				{
				}
			}
			Assert.AssertThat("lease recovery should have occurred in ~" + Sleep * RecoveryAttempts
				 + " ms.", recoveryTriggered, IS.Is(true));
			cluster.StartDataNodes(conf, DatanodeNum, true, HdfsServerConstants.StartupOption
				.Regular, null);
			cluster.WaitActive();
			CheckBlockRecovery(p);
			NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem()).SetLeasePeriod(HdfsConstants
				.LeaseSoftlimitPeriod, HdfsConstants.LeaseHardlimitPeriod);
			CheckFullFile(p, newLength, contents);
			fs.Delete(p, false);
		}

		/// <summary>The last block is truncated at mid.</summary>
		/// <remarks>
		/// The last block is truncated at mid. (non copy-on-truncate)
		/// dn0 is shutdown before truncate and restart after truncate successful.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTruncateWithDataNodesRestart()
		{
			int startingFileSize = 3 * BlockSize;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			Path parent = new Path("/test");
			Path p = new Path(parent, "testTruncateWithDataNodesRestart");
			WriteContents(contents, startingFileSize, p);
			LocatedBlock oldBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			int dn = 0;
			int toTruncateLength = 1;
			int newLength = startingFileSize - toTruncateLength;
			cluster.GetDataNodes()[dn].Shutdown();
			try
			{
				bool isReady = fs.Truncate(p, newLength);
				NUnit.Framework.Assert.IsFalse(isReady);
			}
			finally
			{
				cluster.RestartDataNode(dn, true, true);
				cluster.WaitActive();
			}
			CheckBlockRecovery(p);
			LocatedBlock newBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			/*
			* For non copy-on-truncate, the truncated block id is the same, but the
			* GS should increase.
			* The truncated block will be replicated to dn0 after it restarts.
			*/
			NUnit.Framework.Assert.AreEqual(newBlock.GetBlock().GetBlockId(), oldBlock.GetBlock
				().GetBlockId());
			NUnit.Framework.Assert.AreEqual(newBlock.GetBlock().GetGenerationStamp(), oldBlock
				.GetBlock().GetGenerationStamp() + 1);
			// Wait replicas come to 3
			DFSTestUtil.WaitReplication(fs, p, Replication);
			// Old replica is disregarded and replaced with the truncated one
			NUnit.Framework.Assert.AreEqual(cluster.GetBlockFile(dn, newBlock.GetBlock()).Length
				(), newBlock.GetBlockSize());
			NUnit.Framework.Assert.IsTrue(cluster.GetBlockMetadataFile(dn, newBlock.GetBlock(
				)).GetName().EndsWith(newBlock.GetBlock().GetGenerationStamp() + ".meta"));
			// Validate the file
			FileStatus fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLength));
			CheckFullFile(p, newLength, contents);
			fs.Delete(parent, true);
		}

		/// <summary>The last block is truncated at mid.</summary>
		/// <remarks>
		/// The last block is truncated at mid. (copy-on-truncate)
		/// dn1 is shutdown before truncate and restart after truncate successful.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCopyOnTruncateWithDataNodesRestart()
		{
			int startingFileSize = 3 * BlockSize;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			Path parent = new Path("/test");
			Path p = new Path(parent, "testCopyOnTruncateWithDataNodesRestart");
			WriteContents(contents, startingFileSize, p);
			LocatedBlock oldBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			fs.AllowSnapshot(parent);
			fs.CreateSnapshot(parent, "ss0");
			int dn = 1;
			int toTruncateLength = 1;
			int newLength = startingFileSize - toTruncateLength;
			cluster.GetDataNodes()[dn].Shutdown();
			try
			{
				bool isReady = fs.Truncate(p, newLength);
				NUnit.Framework.Assert.IsFalse(isReady);
			}
			finally
			{
				cluster.RestartDataNode(dn, true, true);
				cluster.WaitActive();
			}
			CheckBlockRecovery(p);
			LocatedBlock newBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			/*
			* For copy-on-truncate, new block is made with new block id and new GS.
			* The replicas of the new block is 2, then it will be replicated to dn1.
			*/
			Assert.AssertNotEquals(newBlock.GetBlock().GetBlockId(), oldBlock.GetBlock().GetBlockId
				());
			NUnit.Framework.Assert.AreEqual(newBlock.GetBlock().GetGenerationStamp(), oldBlock
				.GetBlock().GetGenerationStamp() + 1);
			// Wait replicas come to 3
			DFSTestUtil.WaitReplication(fs, p, Replication);
			// New block is replicated to dn1
			NUnit.Framework.Assert.AreEqual(cluster.GetBlockFile(dn, newBlock.GetBlock()).Length
				(), newBlock.GetBlockSize());
			// Old replica exists too since there is snapshot
			NUnit.Framework.Assert.AreEqual(cluster.GetBlockFile(dn, oldBlock.GetBlock()).Length
				(), oldBlock.GetBlockSize());
			NUnit.Framework.Assert.IsTrue(cluster.GetBlockMetadataFile(dn, oldBlock.GetBlock(
				)).GetName().EndsWith(oldBlock.GetBlock().GetGenerationStamp() + ".meta"));
			// Validate the file
			FileStatus fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLength));
			CheckFullFile(p, newLength, contents);
			fs.DeleteSnapshot(parent, "ss0");
			fs.Delete(parent, true);
		}

		/// <summary>The last block is truncated at mid.</summary>
		/// <remarks>
		/// The last block is truncated at mid. (non copy-on-truncate)
		/// dn0, dn1 are restarted immediately after truncate.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTruncateWithDataNodesRestartImmediately()
		{
			int startingFileSize = 3 * BlockSize;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			Path parent = new Path("/test");
			Path p = new Path(parent, "testTruncateWithDataNodesRestartImmediately");
			WriteContents(contents, startingFileSize, p);
			LocatedBlock oldBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			int dn0 = 0;
			int dn1 = 1;
			int toTruncateLength = 1;
			int newLength = startingFileSize - toTruncateLength;
			bool isReady = fs.Truncate(p, newLength);
			NUnit.Framework.Assert.IsFalse(isReady);
			cluster.RestartDataNode(dn0, true, true);
			cluster.RestartDataNode(dn1, true, true);
			cluster.WaitActive();
			CheckBlockRecovery(p);
			LocatedBlock newBlock = GetLocatedBlocks(p).GetLastLocatedBlock();
			/*
			* For non copy-on-truncate, the truncated block id is the same, but the
			* GS should increase.
			*/
			NUnit.Framework.Assert.AreEqual(newBlock.GetBlock().GetBlockId(), oldBlock.GetBlock
				().GetBlockId());
			NUnit.Framework.Assert.AreEqual(newBlock.GetBlock().GetGenerationStamp(), oldBlock
				.GetBlock().GetGenerationStamp() + 1);
			// Wait replicas come to 3
			DFSTestUtil.WaitReplication(fs, p, Replication);
			// Old replica is disregarded and replaced with the truncated one on dn0
			NUnit.Framework.Assert.AreEqual(cluster.GetBlockFile(dn0, newBlock.GetBlock()).Length
				(), newBlock.GetBlockSize());
			NUnit.Framework.Assert.IsTrue(cluster.GetBlockMetadataFile(dn0, newBlock.GetBlock
				()).GetName().EndsWith(newBlock.GetBlock().GetGenerationStamp() + ".meta"));
			// Old replica is disregarded and replaced with the truncated one on dn1
			NUnit.Framework.Assert.AreEqual(cluster.GetBlockFile(dn1, newBlock.GetBlock()).Length
				(), newBlock.GetBlockSize());
			NUnit.Framework.Assert.IsTrue(cluster.GetBlockMetadataFile(dn1, newBlock.GetBlock
				()).GetName().EndsWith(newBlock.GetBlock().GetGenerationStamp() + ".meta"));
			// Validate the file
			FileStatus fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLength));
			CheckFullFile(p, newLength, contents);
			fs.Delete(parent, true);
		}

		/// <summary>The last block is truncated at mid.</summary>
		/// <remarks>
		/// The last block is truncated at mid. (non copy-on-truncate)
		/// shutdown the datanodes immediately after truncate.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTruncateWithDataNodesShutdownImmediately()
		{
			int startingFileSize = 3 * BlockSize;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			Path parent = new Path("/test");
			Path p = new Path(parent, "testTruncateWithDataNodesShutdownImmediately");
			WriteContents(contents, startingFileSize, p);
			int toTruncateLength = 1;
			int newLength = startingFileSize - toTruncateLength;
			bool isReady = fs.Truncate(p, newLength);
			NUnit.Framework.Assert.IsFalse(isReady);
			cluster.ShutdownDataNodes();
			cluster.SetDataNodesDead();
			try
			{
				for (int i = 0; i < SuccessAttempts && cluster.IsDataNodeUp(); i++)
				{
					Sharpen.Thread.Sleep(Sleep);
				}
				NUnit.Framework.Assert.IsFalse("All DataNodes should be down.", cluster.IsDataNodeUp
					());
				LocatedBlocks blocks = GetLocatedBlocks(p);
				NUnit.Framework.Assert.IsTrue(blocks.IsUnderConstruction());
			}
			finally
			{
				cluster.StartDataNodes(conf, DatanodeNum, true, HdfsServerConstants.StartupOption
					.Regular, null);
				cluster.WaitActive();
			}
			CheckBlockRecovery(p);
			fs.Delete(parent, true);
		}

		/// <summary>EditLogOp load test for Truncate.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateEditLogLoad()
		{
			// purge previously accumulated edits
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			fs.SaveNamespace();
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			int startingFileSize = 2 * BlockSize + BlockSize / 2;
			int toTruncate = 1;
			string s = "/testTruncateEditLogLoad";
			Path p = new Path(s);
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			WriteContents(contents, startingFileSize, p);
			int newLength = startingFileSize - toTruncate;
			bool isReady = fs.Truncate(p, newLength);
			Assert.AssertThat("truncate should have triggered block recovery.", isReady, IS.Is
				(false));
			cluster.RestartNameNode();
			string holder = UserGroupInformation.GetCurrentUser().GetUserName();
			cluster.GetNamesystem().RecoverLease(s, holder, string.Empty);
			CheckBlockRecovery(p);
			CheckFullFile(p, newLength, contents);
			fs.Delete(p, false);
		}

		/// <summary>Upgrade, RollBack, and restart test for Truncate.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeAndRestart()
		{
			Path parent = new Path("/test");
			fs.Mkdirs(parent);
			fs.SetQuota(parent, 100, 1000);
			fs.AllowSnapshot(parent);
			string truncateFile = "testUpgrade";
			Path p = new Path(parent, truncateFile);
			int startingFileSize = 2 * BlockSize;
			int toTruncate = 1;
			byte[] contents = AppendTestUtil.InitBuffer(startingFileSize);
			WriteContents(contents, startingFileSize, p);
			Path snapshotDir = fs.CreateSnapshot(parent, "ss0");
			Path snapshotFile = new Path(snapshotDir, truncateFile);
			int newLengthBeforeUpgrade = startingFileSize - toTruncate;
			bool isReady = fs.Truncate(p, newLengthBeforeUpgrade);
			Assert.AssertThat("truncate should have triggered block recovery.", isReady, IS.Is
				(false));
			CheckBlockRecovery(p);
			CheckFullFile(p, newLengthBeforeUpgrade, contents);
			AssertFileLength(snapshotFile, startingFileSize);
			long totalBlockBefore = cluster.GetNamesystem().GetBlocksTotal();
			RestartCluster(HdfsServerConstants.StartupOption.Upgrade);
			Assert.AssertThat("SafeMode should be OFF", cluster.GetNamesystem().IsInSafeMode(
				), IS.Is(false));
			Assert.AssertThat("NameNode should be performing upgrade.", cluster.GetNamesystem
				().IsUpgradeFinalized(), IS.Is(false));
			FileStatus fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLengthBeforeUpgrade));
			int newLengthAfterUpgrade = newLengthBeforeUpgrade - toTruncate;
			Block oldBlk = GetLocatedBlocks(p).GetLastLocatedBlock().GetBlock().GetLocalBlock
				();
			isReady = fs.Truncate(p, newLengthAfterUpgrade);
			Assert.AssertThat("truncate should have triggered block recovery.", isReady, IS.Is
				(false));
			fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLengthAfterUpgrade));
			Assert.AssertThat("Should copy on truncate during upgrade", GetLocatedBlocks(p).GetLastLocatedBlock
				().GetBlock().GetLocalBlock().GetBlockId(), IS.Is(CoreMatchers.Not(CoreMatchers.EqualTo
				(oldBlk.GetBlockId()))));
			CheckBlockRecovery(p);
			CheckFullFile(p, newLengthAfterUpgrade, contents);
			Assert.AssertThat("Total block count should be unchanged from copy-on-truncate", 
				cluster.GetNamesystem().GetBlocksTotal(), IS.Is(totalBlockBefore));
			RestartCluster(HdfsServerConstants.StartupOption.Rollback);
			Assert.AssertThat("File does not exist " + p, fs.Exists(p), IS.Is(true));
			fileStatus = fs.GetFileStatus(p);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLengthBeforeUpgrade));
			CheckFullFile(p, newLengthBeforeUpgrade, contents);
			Assert.AssertThat("Total block count should be unchanged from rolling back", cluster
				.GetNamesystem().GetBlocksTotal(), IS.Is(totalBlockBefore));
			RestartCluster(HdfsServerConstants.StartupOption.Regular);
			Assert.AssertThat("Total block count should be unchanged from start-up", cluster.
				GetNamesystem().GetBlocksTotal(), IS.Is(totalBlockBefore));
			CheckFullFile(p, newLengthBeforeUpgrade, contents);
			AssertFileLength(snapshotFile, startingFileSize);
			// empty edits and restart
			fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			fs.SaveNamespace();
			cluster.RestartNameNode(true);
			Assert.AssertThat("Total block count should be unchanged from start-up", cluster.
				GetNamesystem().GetBlocksTotal(), IS.Is(totalBlockBefore));
			CheckFullFile(p, newLengthBeforeUpgrade, contents);
			AssertFileLength(snapshotFile, startingFileSize);
			fs.DeleteSnapshot(parent, "ss0");
			fs.Delete(parent, true);
			Assert.AssertThat("File " + p + " shouldn't exist", fs.Exists(p), IS.Is(false));
		}

		/// <summary>Check truncate recovery.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateRecovery()
		{
			FSNamesystem fsn = cluster.GetNamesystem();
			string client = "client";
			string clientMachine = "clientMachine";
			Path parent = new Path("/test");
			string src = "/test/testTruncateRecovery";
			Path srcPath = new Path(src);
			byte[] contents = AppendTestUtil.InitBuffer(BlockSize);
			WriteContents(contents, BlockSize, srcPath);
			INodesInPath iip = fsn.GetFSDirectory().GetINodesInPath4Write(src, true);
			INodeFile file = iip.GetLastINode().AsFile();
			long initialGenStamp = file.GetLastBlock().GetGenerationStamp();
			// Test that prepareFileForTruncate sets up in-place truncate.
			fsn.WriteLock();
			try
			{
				Block oldBlock = file.GetLastBlock();
				Block truncateBlock = fsn.PrepareFileForTruncate(iip, client, clientMachine, 1, null
					);
				// In-place truncate uses old block id with new genStamp.
				Assert.AssertThat(truncateBlock.GetBlockId(), IS.Is(CoreMatchers.EqualTo(oldBlock
					.GetBlockId())));
				Assert.AssertThat(truncateBlock.GetNumBytes(), IS.Is(oldBlock.GetNumBytes()));
				Assert.AssertThat(truncateBlock.GetGenerationStamp(), IS.Is(fsn.GetBlockIdManager
					().GetGenerationStampV2()));
				Assert.AssertThat(file.GetLastBlock().GetBlockUCState(), IS.Is(HdfsServerConstants.BlockUCState
					.UnderRecovery));
				long blockRecoveryId = ((BlockInfoContiguousUnderConstruction)file.GetLastBlock()
					).GetBlockRecoveryId();
				Assert.AssertThat(blockRecoveryId, IS.Is(initialGenStamp + 1));
				fsn.GetEditLog().LogTruncate(src, client, clientMachine, BlockSize - 1, Time.Now(
					), truncateBlock);
			}
			finally
			{
				fsn.WriteUnlock();
			}
			// Re-create file and ensure we are ready to copy on truncate
			WriteContents(contents, BlockSize, srcPath);
			fs.AllowSnapshot(parent);
			fs.CreateSnapshot(parent, "ss0");
			iip = fsn.GetFSDirectory().GetINodesInPath(src, true);
			file = iip.GetLastINode().AsFile();
			file.RecordModification(iip.GetLatestSnapshotId(), true);
			Assert.AssertThat(file.IsBlockInLatestSnapshot(file.GetLastBlock()), IS.Is(true));
			initialGenStamp = file.GetLastBlock().GetGenerationStamp();
			// Test that prepareFileForTruncate sets up copy-on-write truncate
			fsn.WriteLock();
			try
			{
				Block oldBlock = file.GetLastBlock();
				Block truncateBlock = fsn.PrepareFileForTruncate(iip, client, clientMachine, 1, null
					);
				// Copy-on-write truncate makes new block with new id and genStamp
				Assert.AssertThat(truncateBlock.GetBlockId(), IS.Is(CoreMatchers.Not(CoreMatchers.EqualTo
					(oldBlock.GetBlockId()))));
				Assert.AssertThat(truncateBlock.GetNumBytes() < oldBlock.GetNumBytes(), IS.Is(true
					));
				Assert.AssertThat(truncateBlock.GetGenerationStamp(), IS.Is(fsn.GetBlockIdManager
					().GetGenerationStampV2()));
				Assert.AssertThat(file.GetLastBlock().GetBlockUCState(), IS.Is(HdfsServerConstants.BlockUCState
					.UnderRecovery));
				long blockRecoveryId = ((BlockInfoContiguousUnderConstruction)file.GetLastBlock()
					).GetBlockRecoveryId();
				Assert.AssertThat(blockRecoveryId, IS.Is(initialGenStamp + 1));
				fsn.GetEditLog().LogTruncate(src, client, clientMachine, BlockSize - 1, Time.Now(
					), truncateBlock);
			}
			finally
			{
				fsn.WriteUnlock();
			}
			CheckBlockRecovery(srcPath);
			fs.DeleteSnapshot(parent, "ss0");
			fs.Delete(parent, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateShellCommand()
		{
			Path parent = new Path("/test");
			Path src = new Path("/test/testTruncateShellCommand");
			int oldLength = 2 * BlockSize + 1;
			int newLength = BlockSize + 1;
			string[] argv = new string[] { "-truncate", newLength.ToString(), src.ToString() };
			RunTruncateShellCommand(src, oldLength, argv);
			// wait for block recovery
			CheckBlockRecovery(src);
			Assert.AssertThat(fs.GetFileStatus(src).GetLen(), IS.Is((long)newLength));
			fs.Delete(parent, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateShellCommandOnBlockBoundary()
		{
			Path parent = new Path("/test");
			Path src = new Path("/test/testTruncateShellCommandOnBoundary");
			int oldLength = 2 * BlockSize;
			int newLength = BlockSize;
			string[] argv = new string[] { "-truncate", newLength.ToString(), src.ToString() };
			RunTruncateShellCommand(src, oldLength, argv);
			// shouldn't need to wait for block recovery
			Assert.AssertThat(fs.GetFileStatus(src).GetLen(), IS.Is((long)newLength));
			fs.Delete(parent, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateShellCommandWithWaitOption()
		{
			Path parent = new Path("/test");
			Path src = new Path("/test/testTruncateShellCommandWithWaitOption");
			int oldLength = 2 * BlockSize + 1;
			int newLength = BlockSize + 1;
			string[] argv = new string[] { "-truncate", "-w", newLength.ToString(), src.ToString
				() };
			RunTruncateShellCommand(src, oldLength, argv);
			// shouldn't need to wait for block recovery
			Assert.AssertThat(fs.GetFileStatus(src).GetLen(), IS.Is((long)newLength));
			fs.Delete(parent, true);
		}

		/// <exception cref="System.Exception"/>
		private void RunTruncateShellCommand(Path src, int oldLength, string[] shellOpts)
		{
			// create file and write data
			WriteContents(AppendTestUtil.InitBuffer(oldLength), oldLength, src);
			Assert.AssertThat(fs.GetFileStatus(src).GetLen(), IS.Is((long)oldLength));
			// truncate file using shell
			FsShell shell = null;
			try
			{
				shell = new FsShell(conf);
				Assert.AssertThat(ToolRunner.Run(shell, shellOpts), IS.Is(0));
			}
			finally
			{
				if (shell != null)
				{
					shell.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncate4Symlink()
		{
			int fileLength = 3 * BlockSize;
			Path parent = new Path("/test");
			fs.Mkdirs(parent);
			byte[] contents = AppendTestUtil.InitBuffer(fileLength);
			Path file = new Path(parent, "testTruncate4Symlink");
			WriteContents(contents, fileLength, file);
			Path link = new Path(parent, "link");
			fs.CreateSymlink(file, link, false);
			int newLength = fileLength / 3;
			bool isReady = fs.Truncate(link, newLength);
			NUnit.Framework.Assert.IsTrue("Recovery is not expected.", isReady);
			FileStatus fileStatus = fs.GetFileStatus(file);
			Assert.AssertThat(fileStatus.GetLen(), IS.Is((long)newLength));
			ContentSummary cs = fs.GetContentSummary(parent);
			NUnit.Framework.Assert.AreEqual("Bad disk space usage", cs.GetSpaceConsumed(), newLength
				 * Replication);
			// validate the file content
			CheckFullFile(file, newLength, contents);
			fs.Delete(parent, true);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void WriteContents(byte[] contents, int fileLength, Path p)
		{
			FSDataOutputStream @out = fs.Create(p, true, BlockSize, Replication, BlockSize);
			@out.Write(contents, 0, fileLength);
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CheckBlockRecovery(Path p)
		{
			CheckBlockRecovery(p, fs);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckBlockRecovery(Path p, DistributedFileSystem dfs)
		{
			CheckBlockRecovery(p, dfs, SuccessAttempts, Sleep);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CheckBlockRecovery(Path p, DistributedFileSystem dfs, int attempts
			, long sleepMs)
		{
			bool success = false;
			for (int i = 0; i < attempts; i++)
			{
				LocatedBlocks blocks = GetLocatedBlocks(p, dfs);
				bool noLastBlock = blocks.GetLastLocatedBlock() == null;
				if (!blocks.IsUnderConstruction() && (noLastBlock || blocks.IsLastBlockComplete()
					))
				{
					success = true;
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(sleepMs);
				}
				catch (Exception)
				{
				}
			}
			Assert.AssertThat("inode should complete in ~" + sleepMs * attempts + " ms.", success
				, IS.Is(true));
		}

		/// <exception cref="System.IO.IOException"/>
		internal static LocatedBlocks GetLocatedBlocks(Path src)
		{
			return GetLocatedBlocks(src, fs);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static LocatedBlocks GetLocatedBlocks(Path src, DistributedFileSystem dfs
			)
		{
			return dfs.GetClient().GetLocatedBlocks(src.ToString(), 0, long.MaxValue);
		}

		internal static void AssertBlockExists(Block blk)
		{
			NUnit.Framework.Assert.IsNotNull("BlocksMap does not contain block: " + blk, cluster
				.GetNamesystem().GetStoredBlock(blk));
		}

		internal static void AssertBlockNotPresent(Block blk)
		{
			NUnit.Framework.Assert.IsNull("BlocksMap should not contain block: " + blk, cluster
				.GetNamesystem().GetStoredBlock(blk));
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void AssertFileLength(Path file, long length)
		{
			byte[] data = DFSTestUtil.ReadFileBuffer(fs, file);
			NUnit.Framework.Assert.AreEqual("Wrong data size in snapshot.", length, data.Length
				);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CheckFullFile(Path p, int newLength, byte[] contents)
		{
			AppendTestUtil.CheckFullFile(fs, p, newLength, contents, p.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RestartCluster(HdfsServerConstants.StartupOption o)
		{
			cluster.Shutdown();
			if (HdfsServerConstants.StartupOption.Rollback == o)
			{
				NameNode.DoRollback(conf, false);
			}
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum).Format(false
				).NameNodePort(NameNode.DefaultPort).StartupOption(o == HdfsServerConstants.StartupOption
				.Rollback ? HdfsServerConstants.StartupOption.Regular : o).DnStartupOption(o != 
				HdfsServerConstants.StartupOption.Rollback ? HdfsServerConstants.StartupOption.Regular
				 : o).Build();
			fs = cluster.GetFileSystem();
		}
	}
}
