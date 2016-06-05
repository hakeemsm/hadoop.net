using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Make sure we correctly update the quota usage for truncate.</summary>
	/// <remarks>
	/// Make sure we correctly update the quota usage for truncate.
	/// We need to cover the following cases:
	/// 1. No snapshot, truncate to 0
	/// 2. No snapshot, truncate at block boundary
	/// 3. No snapshot, not on block boundary
	/// 4~6. With snapshot, all the current blocks are included in latest
	/// snapshots, repeat 1~3
	/// 7~9. With snapshot, blocks in the latest snapshot and blocks in the current
	/// file diverged, repeat 1~3
	/// </remarks>
	public class TestTruncateQuotaUpdate
	{
		private const int Blocksize = 1024;

		private const short Replication = 4;

		private const long Diskquota = Blocksize * 20;

		internal const long seed = 0L;

		private static readonly Path dir = new Path("/TestTruncateQuotaUpdate");

		private static readonly Path file = new Path(dir, "file");

		private MiniDFSCluster cluster;

		private FSDirectory fsdir;

		private DistributedFileSystem dfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsdir = cluster.GetNamesystem().GetFSDirectory();
			dfs = cluster.GetFileSystem();
			dfs.Mkdirs(dir);
			dfs.SetQuota(dir, long.MaxValue - 1, Diskquota);
			dfs.SetQuotaByStorageType(dir, StorageType.Disk, Diskquota);
			dfs.SetStoragePolicy(dir, HdfsConstants.HotStoragePolicyName);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTruncateQuotaUpdate()
		{
		}

		public interface TruncateCase
		{
			/// <exception cref="System.Exception"/>
			void Prepare();

			/// <exception cref="System.Exception"/>
			void Run();
		}

		/// <exception cref="System.Exception"/>
		private void TestTruncate(long newLength, long expectedDiff, long expectedUsage)
		{
			// before doing the real truncation, make sure the computation is correct
			INodesInPath iip = fsdir.GetINodesInPath4Write(file.ToString());
			INodeFile fileNode = iip.GetLastINode().AsFile();
			fileNode.RecordModification(iip.GetLatestSnapshotId(), true);
			long diff = fileNode.ComputeQuotaDeltaForTruncate(newLength);
			NUnit.Framework.Assert.AreEqual(expectedDiff, diff);
			// do the real truncation
			dfs.Truncate(file, newLength);
			// wait for truncate to finish
			TestFileTruncate.CheckBlockRecovery(file, dfs);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			long spaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			long diskUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetTypeSpaces
				().Get(StorageType.Disk);
			NUnit.Framework.Assert.AreEqual(expectedUsage, spaceUsed);
			NUnit.Framework.Assert.AreEqual(expectedUsage, diskUsed);
		}

		/// <summary>case 1~3</summary>
		private class TruncateWithoutSnapshot : TestTruncateQuotaUpdate.TruncateCase
		{
			/// <exception cref="System.Exception"/>
			public virtual void Prepare()
			{
				// original file size: 2.5 block
				DFSTestUtil.CreateFile(this._enclosing.dfs, TestTruncateQuotaUpdate.file, TestTruncateQuotaUpdate
					.Blocksize * 2 + TestTruncateQuotaUpdate.Blocksize / 2, TestTruncateQuotaUpdate.
					Replication, 0L);
			}

			/// <exception cref="System.Exception"/>
			public virtual void Run()
			{
				// case 1: first truncate to 1.5 blocks
				long newLength = TestTruncateQuotaUpdate.Blocksize + TestTruncateQuotaUpdate.Blocksize
					 / 2;
				// we truncate 1 blocks, but not on the boundary, thus the diff should
				// be -block + (block - 0.5 block) = -0.5 block
				long diff = -TestTruncateQuotaUpdate.Blocksize / 2;
				// the new quota usage should be BLOCKSIZE * 1.5 * replication
				long usage = (TestTruncateQuotaUpdate.Blocksize + TestTruncateQuotaUpdate.Blocksize
					 / 2) * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 2: truncate to 1 block
				newLength = TestTruncateQuotaUpdate.Blocksize;
				// the diff should be -0.5 block since this is not on boundary
				diff = -TestTruncateQuotaUpdate.Blocksize / 2;
				// after truncation the quota usage should be BLOCKSIZE * replication
				usage = TestTruncateQuotaUpdate.Blocksize * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 3: truncate to 0
				this._enclosing.TestTruncate(0, -TestTruncateQuotaUpdate.Blocksize, 0);
			}

			internal TruncateWithoutSnapshot(TestTruncateQuotaUpdate _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestTruncateQuotaUpdate _enclosing;
		}

		/// <summary>case 4~6</summary>
		private class TruncateWithSnapshot : TestTruncateQuotaUpdate.TruncateCase
		{
			/// <exception cref="System.Exception"/>
			public virtual void Prepare()
			{
				DFSTestUtil.CreateFile(this._enclosing.dfs, TestTruncateQuotaUpdate.file, TestTruncateQuotaUpdate
					.Blocksize * 2 + TestTruncateQuotaUpdate.Blocksize / 2, TestTruncateQuotaUpdate.
					Replication, 0L);
				SnapshotTestHelper.CreateSnapshot(this._enclosing.dfs, TestTruncateQuotaUpdate.dir
					, "s1");
			}

			/// <exception cref="System.Exception"/>
			public virtual void Run()
			{
				// case 4: truncate to 1.5 blocks
				long newLength = TestTruncateQuotaUpdate.Blocksize + TestTruncateQuotaUpdate.Blocksize
					 / 2;
				// all the blocks are in snapshot. truncate need to allocate a new block
				// diff should be +BLOCKSIZE
				long diff = TestTruncateQuotaUpdate.Blocksize;
				// the new quota usage should be BLOCKSIZE * 3 * replication
				long usage = TestTruncateQuotaUpdate.Blocksize * 3 * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 5: truncate to 1 block
				newLength = TestTruncateQuotaUpdate.Blocksize;
				// the block for truncation is not in snapshot, diff should be -0.5 block
				diff = -TestTruncateQuotaUpdate.Blocksize / 2;
				// after truncation the quota usage should be 2.5 block * repl
				usage = (TestTruncateQuotaUpdate.Blocksize * 2 + TestTruncateQuotaUpdate.Blocksize
					 / 2) * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 6: truncate to 0
				this._enclosing.TestTruncate(0, 0, usage);
			}

			internal TruncateWithSnapshot(TestTruncateQuotaUpdate _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestTruncateQuotaUpdate _enclosing;
		}

		/// <summary>case 7~9</summary>
		private class TruncateWithSnapshot2 : TestTruncateQuotaUpdate.TruncateCase
		{
			/// <exception cref="System.Exception"/>
			public virtual void Prepare()
			{
				// original size: 2.5 blocks
				DFSTestUtil.CreateFile(this._enclosing.dfs, TestTruncateQuotaUpdate.file, TestTruncateQuotaUpdate
					.Blocksize * 2 + TestTruncateQuotaUpdate.Blocksize / 2, TestTruncateQuotaUpdate.
					Replication, 0L);
				SnapshotTestHelper.CreateSnapshot(this._enclosing.dfs, TestTruncateQuotaUpdate.dir
					, "s1");
				// truncate to 1.5 block
				this._enclosing.dfs.Truncate(TestTruncateQuotaUpdate.file, TestTruncateQuotaUpdate
					.Blocksize + TestTruncateQuotaUpdate.Blocksize / 2);
				TestFileTruncate.CheckBlockRecovery(TestTruncateQuotaUpdate.file, this._enclosing
					.dfs);
				// append another 1 BLOCK
				DFSTestUtil.AppendFile(this._enclosing.dfs, TestTruncateQuotaUpdate.file, TestTruncateQuotaUpdate
					.Blocksize);
			}

			/// <exception cref="System.Exception"/>
			public virtual void Run()
			{
				// case 8: truncate to 2 blocks
				long newLength = TestTruncateQuotaUpdate.Blocksize * 2;
				// the original 2.5 blocks are in snapshot. the block truncated is not
				// in snapshot. diff should be -0.5 block
				long diff = -TestTruncateQuotaUpdate.Blocksize / 2;
				// the new quota usage should be BLOCKSIZE * 3.5 * replication
				long usage = (TestTruncateQuotaUpdate.Blocksize * 3 + TestTruncateQuotaUpdate.Blocksize
					 / 2) * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 7: truncate to 1.5 block
				newLength = TestTruncateQuotaUpdate.Blocksize + TestTruncateQuotaUpdate.Blocksize
					 / 2;
				// the block for truncation is not in snapshot, diff should be
				// -0.5 block + (block - 0.5block) = 0
				diff = 0;
				// after truncation the quota usage should be 3 block * repl
				usage = (TestTruncateQuotaUpdate.Blocksize * 3) * TestTruncateQuotaUpdate.Replication;
				this._enclosing.TestTruncate(newLength, diff, usage);
				// case 9: truncate to 0
				this._enclosing.TestTruncate(0, -TestTruncateQuotaUpdate.Blocksize / 2, (TestTruncateQuotaUpdate
					.Blocksize * 2 + TestTruncateQuotaUpdate.Blocksize / 2) * TestTruncateQuotaUpdate
					.Replication);
			}

			internal TruncateWithSnapshot2(TestTruncateQuotaUpdate _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestTruncateQuotaUpdate _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private void TestTruncateQuotaUpdate(TestTruncateQuotaUpdate.TruncateCase t)
		{
			t.Prepare();
			t.Run();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaNoSnapshot()
		{
			TestTruncateQuotaUpdate(new TestTruncateQuotaUpdate.TruncateWithoutSnapshot(this)
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaWithSnapshot()
		{
			TestTruncateQuotaUpdate(new TestTruncateQuotaUpdate.TruncateWithSnapshot(this));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaWithSnapshot2()
		{
			TestTruncateQuotaUpdate(new TestTruncateQuotaUpdate.TruncateWithSnapshot2(this));
		}
	}
}
