using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestQuotaByStorageType
	{
		private const int Blocksize = 1024;

		private const short Replication = 3;

		private const long seed = 0L;

		private static readonly Path dir = new Path("/TestQuotaByStorageType");

		private Configuration conf;

		private MiniDFSCluster cluster;

		private FSDirectory fsdir;

		private DistributedFileSystem dfs;

		private FSNamesystem fsn;

		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestQuotaByStorageType
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			// Setup a 3-node cluster and configure
			// each node with 1 SSD and 1 DISK without capacity limitation
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).StorageTypes
				(new StorageType[] { StorageType.Ssd, StorageType.Default }).Build();
			cluster.WaitActive();
			RefreshClusterState();
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

		// Cluster state must be refreshed after each start/restart in the test
		/// <exception cref="System.IO.IOException"/>
		private void RefreshClusterState()
		{
			fsdir = cluster.GetNamesystem().GetFSDirectory();
			dfs = cluster.GetFileSystem();
			fsn = cluster.GetNamesystem();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateOneSSD()
		{
			TestQuotaByStorageTypeWithFileCreateCase(HdfsConstants.OnessdStoragePolicyName, StorageType
				.Ssd, (short)1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateAllSSD()
		{
			TestQuotaByStorageTypeWithFileCreateCase(HdfsConstants.AllssdStoragePolicyName, StorageType
				.Ssd, (short)3);
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestQuotaByStorageTypeWithFileCreateCase(string storagePolicy
			, StorageType storageType, short replication)
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			// set storage policy on directory "foo" to storagePolicy
			dfs.SetStoragePolicy(foo, storagePolicy);
			// set quota by storage type on directory "foo"
			dfs.SetQuotaByStorageType(foo, storageType, Blocksize * 10);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify space consumed and remaining quota
			long storageTypeConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(storageType);
			NUnit.Framework.Assert.AreEqual(file1Len * replication, storageTypeConsumed);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateAppend()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			// set storage policy on directory "foo" to ONESSD
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on directory "foo"
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 4);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify space consumed and remaining quota
			long ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// append several blocks
			int appendLen = Blocksize * 2;
			DFSTestUtil.AppendFile(dfs, createdFile1, appendLen);
			file1Len += appendLen;
			ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), file1Len * Replication);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Ssd), file1Len);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Disk), file1Len * 
				2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateDelete()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on directory "foo"
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 10);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create file of size 2.5 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify space consumed and remaining quota
			long storageTypeConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, storageTypeConsumed);
			// Delete file and verify the consumed space of the storage type is updated
			dfs.Delete(createdFile1, false);
			storageTypeConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(0, storageTypeConsumed);
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			fnode.ComputeQuotaUsage(fsn.GetBlockManager().GetStoragePolicySuite(), counts, true
				);
			NUnit.Framework.Assert.AreEqual(fnode.DumpTreeRecursively().ToString(), 0, counts
				.GetTypeSpaces().Get(StorageType.Ssd));
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), 0);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Ssd), 0);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Disk), 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateRename()
		{
			Path foo = new Path(dir, "foo");
			dfs.Mkdirs(foo);
			Path createdFile1foo = new Path(foo, "created_file1.data");
			Path bar = new Path(dir, "bar");
			dfs.Mkdirs(bar);
			Path createdFile1bar = new Path(bar, "created_file1.data");
			// set storage policy on directory "foo" and "bar" to ONESSD
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetStoragePolicy(bar, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on directory "foo"
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 4);
			dfs.SetQuotaByStorageType(bar, StorageType.Ssd, Blocksize * 2);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create file of size 3 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 3;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1foo, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify space consumed and remaining quota
			long ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// move file from foo to bar
			try
			{
				dfs.Rename(createdFile1foo, createdFile1bar);
				NUnit.Framework.Assert.Fail("Should have failed with QuotaByStorageTypeExceededException "
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), file1Len * Replication);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Ssd), file1Len);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Disk), file1Len * 
				2);
		}

		/// <summary>
		/// Test if the quota can be correctly updated for create file even
		/// QuotaByStorageTypeExceededException is thrown
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeExceptionWithFileCreate()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 4);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create the 1st file of size 2 * BLOCKSIZE under directory "foo" and expect no exception
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			long currentSSDConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, currentSSDConsumed);
			// Create the 2nd file of size 1.5 * BLOCKSIZE under directory "foo" and expect no exception
			Path createdFile2 = new Path(foo, "created_file2.data");
			long file2Len = Blocksize + Blocksize / 2;
			DFSTestUtil.CreateFile(dfs, createdFile2, bufLen, file2Len, Blocksize, Replication
				, seed);
			currentSSDConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len + file2Len, currentSSDConsumed);
			// Create the 3rd file of size BLOCKSIZE under directory "foo" and expect quota exceeded exception
			Path createdFile3 = new Path(foo, "created_file3.data");
			long file3Len = Blocksize;
			try
			{
				DFSTestUtil.CreateFile(dfs, createdFile3, bufLen, file3Len, Blocksize, Replication
					, seed);
				NUnit.Framework.Assert.Fail("Should have failed with QuotaByStorageTypeExceededException "
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
				currentSSDConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
					().GetTypeSpaces().Get(StorageType.Ssd);
				NUnit.Framework.Assert.AreEqual(file1Len + file2Len, currentSSDConsumed);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeParentOffChildOff()
		{
			Path parent = new Path(dir, "parent");
			Path child = new Path(parent, "child");
			dfs.Mkdirs(parent);
			dfs.Mkdirs(child);
			dfs.SetStoragePolicy(parent, HdfsConstants.OnessdStoragePolicyName);
			// Create file of size 2.5 * BLOCKSIZE under child directory.
			// Since both parent and child directory do not have SSD quota set,
			// expect succeed without exception
			Path createdFile1 = new Path(child, "created_file1.data");
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify SSD usage at the root level as both parent/child don't have DirectoryWithQuotaFeature
			INode fnode = fsdir.GetINode4Write("/");
			long ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeParentOffChildOn()
		{
			Path parent = new Path(dir, "parent");
			Path child = new Path(parent, "child");
			dfs.Mkdirs(parent);
			dfs.Mkdirs(child);
			dfs.SetStoragePolicy(parent, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(child, StorageType.Ssd, 2 * Blocksize);
			// Create file of size 2.5 * BLOCKSIZE under child directory
			// Since child directory have SSD quota of 2 * BLOCKSIZE,
			// expect an exception when creating files under child directory.
			Path createdFile1 = new Path(child, "created_file1.data");
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			try
			{
				DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
					, seed);
				NUnit.Framework.Assert.Fail("Should have failed with QuotaByStorageTypeExceededException "
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeParentOnChildOff()
		{
			short replication = 1;
			Path parent = new Path(dir, "parent");
			Path child = new Path(parent, "child");
			dfs.Mkdirs(parent);
			dfs.Mkdirs(child);
			dfs.SetStoragePolicy(parent, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(parent, StorageType.Ssd, 3 * Blocksize);
			// Create file of size 2.5 * BLOCKSIZE under child directory
			// Verify parent Quota applies
			Path createdFile1 = new Path(child, "created_file1.data");
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, replication
				, seed);
			INode fnode = fsdir.GetINode4Write(parent.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			long currentSSDConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, currentSSDConsumed);
			// Create the 2nd file of size BLOCKSIZE under child directory and expect quota exceeded exception
			Path createdFile2 = new Path(child, "created_file2.data");
			long file2Len = Blocksize;
			try
			{
				DFSTestUtil.CreateFile(dfs, createdFile2, bufLen, file2Len, Blocksize, replication
					, seed);
				NUnit.Framework.Assert.Fail("Should have failed with QuotaByStorageTypeExceededException "
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
				currentSSDConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
					().GetTypeSpaces().Get(StorageType.Ssd);
				NUnit.Framework.Assert.AreEqual(file1Len, currentSSDConsumed);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeParentOnChildOn()
		{
			Path parent = new Path(dir, "parent");
			Path child = new Path(parent, "child");
			dfs.Mkdirs(parent);
			dfs.Mkdirs(child);
			dfs.SetStoragePolicy(parent, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(parent, StorageType.Ssd, 2 * Blocksize);
			dfs.SetQuotaByStorageType(child, StorageType.Ssd, 3 * Blocksize);
			// Create file of size 2.5 * BLOCKSIZE under child directory
			// Verify parent Quota applies
			Path createdFile1 = new Path(child, "created_file1.data");
			long file1Len = Blocksize * 2 + Blocksize / 2;
			int bufLen = Blocksize / 16;
			try
			{
				DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
					, seed);
				NUnit.Framework.Assert.Fail("Should have failed with QuotaByStorageTypeExceededException "
					);
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
			}
		}

		/// <summary>
		/// Both traditional space quota and the storage type quota for SSD are set and
		/// not exceeded.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithTraditionalQuota()
		{
			Path foo = new Path(dir, "foo");
			dfs.Mkdirs(foo);
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 10);
			dfs.SetQuota(foo, long.MaxValue - 1, Replication * Blocksize * 10);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			Path createdFile = new Path(foo, "created_file.data");
			long fileLen = Blocksize * 2 + Blocksize / 2;
			DFSTestUtil.CreateFile(dfs, createdFile, Blocksize / 16, fileLen, Blocksize, Replication
				, seed);
			QuotaCounts cnt = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				();
			NUnit.Framework.Assert.AreEqual(2, cnt.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(fileLen * Replication, cnt.GetStorageSpace());
			dfs.Delete(createdFile, true);
			QuotaCounts cntAfterDelete = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				();
			NUnit.Framework.Assert.AreEqual(1, cntAfterDelete.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(0, cntAfterDelete.GetStorageSpace());
			// Validate the computeQuotaUsage()
			QuotaCounts counts = new QuotaCounts.Builder().Build();
			fnode.ComputeQuotaUsage(fsn.GetBlockManager().GetStoragePolicySuite(), counts, true
				);
			NUnit.Framework.Assert.AreEqual(fnode.DumpTreeRecursively().ToString(), 1, counts
				.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(fnode.DumpTreeRecursively().ToString(), 0, counts
				.GetStorageSpace());
		}

		/// <summary>
		/// Both traditional space quota and the storage type quota for SSD are set and
		/// exceeded.
		/// </summary>
		/// <remarks>
		/// Both traditional space quota and the storage type quota for SSD are set and
		/// exceeded. expect DSQuotaExceededException is thrown as we check traditional
		/// space quota first and then storage type quota.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeAndTraditionalQuotaException1()
		{
			TestQuotaByStorageTypeOrTraditionalQuotaExceededCase(4 * Replication, 4, 5, Replication
				);
		}

		/// <summary>
		/// Both traditional space quota and the storage type quota for SSD are set and
		/// SSD quota is exceeded but traditional space quota is not exceeded.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeAndTraditionalQuotaException2()
		{
			TestQuotaByStorageTypeOrTraditionalQuotaExceededCase(5 * Replication, 4, 5, Replication
				);
		}

		/// <summary>
		/// Both traditional space quota and the storage type quota for SSD are set and
		/// traditional space quota is exceeded but SSD quota is not exceeded.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeAndTraditionalQuotaException3()
		{
			TestQuotaByStorageTypeOrTraditionalQuotaExceededCase(4 * Replication, 5, 5, Replication
				);
		}

		/// <exception cref="System.Exception"/>
		private void TestQuotaByStorageTypeOrTraditionalQuotaExceededCase(long storageSpaceQuotaInBlocks
			, long ssdQuotaInBlocks, long testFileLenInBlocks, short replication)
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path testDir = new Path(dir, MethodName);
			dfs.Mkdirs(testDir);
			dfs.SetStoragePolicy(testDir, HdfsConstants.OnessdStoragePolicyName);
			long ssdQuota = Blocksize * ssdQuotaInBlocks;
			long storageSpaceQuota = Blocksize * storageSpaceQuotaInBlocks;
			dfs.SetQuota(testDir, long.MaxValue - 1, storageSpaceQuota);
			dfs.SetQuotaByStorageType(testDir, StorageType.Ssd, ssdQuota);
			INode testDirNode = fsdir.GetINode4Write(testDir.ToString());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsQuotaSet());
			Path createdFile = new Path(testDir, "created_file.data");
			long fileLen = testFileLenInBlocks * Blocksize;
			try
			{
				DFSTestUtil.CreateFile(dfs, createdFile, Blocksize / 16, fileLen, Blocksize, replication
					, seed);
				NUnit.Framework.Assert.Fail("Should have failed with DSQuotaExceededException or "
					 + "QuotaByStorageTypeExceededException ");
			}
			catch (Exception t)
			{
				Log.Info("Got expected exception ", t);
				long currentSSDConsumed = testDirNode.AsDirectory().GetDirectoryWithQuotaFeature(
					).GetSpaceConsumed().GetTypeSpaces().Get(StorageType.Ssd);
				NUnit.Framework.Assert.AreEqual(Math.Min(ssdQuota, storageSpaceQuota / replication
					), currentSSDConsumed);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithSnapshot()
		{
			Path sub1 = new Path(dir, "Sub1");
			dfs.Mkdirs(sub1);
			// Setup ONE_SSD policy and SSD quota of 4 * BLOCKSIZE on sub1
			dfs.SetStoragePolicy(sub1, HdfsConstants.OnessdStoragePolicyName);
			dfs.SetQuotaByStorageType(sub1, StorageType.Ssd, 4 * Blocksize);
			INode sub1Node = fsdir.GetINode4Write(sub1.ToString());
			NUnit.Framework.Assert.IsTrue(sub1Node.IsDirectory());
			NUnit.Framework.Assert.IsTrue(sub1Node.IsQuotaSet());
			// Create file1 of size 2 * BLOCKSIZE under sub1
			Path file1 = new Path(sub1, "file1");
			long file1Len = 2 * Blocksize;
			DFSTestUtil.CreateFile(dfs, file1, file1Len, Replication, seed);
			// Create snapshot on sub1 named s1
			SnapshotTestHelper.CreateSnapshot(dfs, sub1, "s1");
			// Verify sub1 SSD usage is unchanged after creating snapshot s1
			long ssdConsumed = sub1Node.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// Delete file1
			dfs.Delete(file1, false);
			// Verify sub1 SSD usage is unchanged due to the existence of snapshot s1
			ssdConsumed = sub1Node.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			QuotaCounts counts1 = new QuotaCounts.Builder().Build();
			sub1Node.ComputeQuotaUsage(fsn.GetBlockManager().GetStoragePolicySuite(), counts1
				, true);
			NUnit.Framework.Assert.AreEqual(sub1Node.DumpTreeRecursively().ToString(), file1Len
				, counts1.GetTypeSpaces().Get(StorageType.Ssd));
			ContentSummary cs1 = dfs.GetContentSummary(sub1);
			NUnit.Framework.Assert.AreEqual(cs1.GetSpaceConsumed(), file1Len * Replication);
			NUnit.Framework.Assert.AreEqual(cs1.GetTypeConsumed(StorageType.Ssd), file1Len);
			NUnit.Framework.Assert.AreEqual(cs1.GetTypeConsumed(StorageType.Disk), file1Len *
				 2);
			// Delete the snapshot s1
			dfs.DeleteSnapshot(sub1, "s1");
			// Verify sub1 SSD usage is fully reclaimed and changed to 0
			ssdConsumed = sub1Node.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(0, ssdConsumed);
			QuotaCounts counts2 = new QuotaCounts.Builder().Build();
			sub1Node.ComputeQuotaUsage(fsn.GetBlockManager().GetStoragePolicySuite(), counts2
				, true);
			NUnit.Framework.Assert.AreEqual(sub1Node.DumpTreeRecursively().ToString(), 0, counts2
				.GetTypeSpaces().Get(StorageType.Ssd));
			ContentSummary cs2 = dfs.GetContentSummary(sub1);
			NUnit.Framework.Assert.AreEqual(cs2.GetSpaceConsumed(), 0);
			NUnit.Framework.Assert.AreEqual(cs2.GetTypeConsumed(StorageType.Ssd), 0);
			NUnit.Framework.Assert.AreEqual(cs2.GetTypeConsumed(StorageType.Disk), 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaByStorageTypeWithFileCreateTruncate()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			// set storage policy on directory "foo" to ONESSD
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on directory "foo"
			dfs.SetQuotaByStorageType(foo, StorageType.Ssd, Blocksize * 4);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify SSD consumed before truncate
			long ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// Truncate file to 1 * BLOCKSIZE
			int newFile1Len = Blocksize * 1;
			dfs.Truncate(createdFile1, newFile1Len);
			// Verify SSD consumed after truncate
			ssdConsumed = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(newFile1Len, ssdConsumed);
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), newFile1Len * Replication);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Ssd), newFile1Len);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Disk), newFile1Len
				 * 2);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaByStorageTypePersistenceInEditLog()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path testDir = new Path(dir, MethodName);
			Path createdFile1 = new Path(testDir, "created_file1.data");
			dfs.Mkdirs(testDir);
			// set storage policy on testDir to ONESSD
			dfs.SetStoragePolicy(testDir, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on testDir
			long SsdQuota = Blocksize * 4;
			dfs.SetQuotaByStorageType(testDir, StorageType.Ssd, SsdQuota);
			INode testDirNode = fsdir.GetINode4Write(testDir.ToString());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under testDir
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify SSD consumed before namenode restart
			long ssdConsumed = testDirNode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// Restart namenode to make sure the editlog is correct
			cluster.RestartNameNode(true);
			RefreshClusterState();
			INode testDirNodeAfterNNRestart = fsdir.GetINode4Write(testDir.ToString());
			// Verify quota is still set
			NUnit.Framework.Assert.IsTrue(testDirNode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsQuotaSet());
			QuotaCounts qc = testDirNodeAfterNNRestart.GetQuotaCounts();
			NUnit.Framework.Assert.AreEqual(SsdQuota, qc.GetTypeSpace(StorageType.Ssd));
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				if (t != StorageType.Ssd)
				{
					NUnit.Framework.Assert.AreEqual(HdfsConstants.QuotaReset, qc.GetTypeSpace(t));
				}
			}
			long ssdConsumedAfterNNRestart = testDirNodeAfterNNRestart.AsDirectory().GetDirectoryWithQuotaFeature
				().GetSpaceConsumed().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumedAfterNNRestart);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaByStorageTypePersistenceInFsImage()
		{
			string MethodName = GenericTestUtils.GetMethodName();
			Path testDir = new Path(dir, MethodName);
			Path createdFile1 = new Path(testDir, "created_file1.data");
			dfs.Mkdirs(testDir);
			// set storage policy on testDir to ONESSD
			dfs.SetStoragePolicy(testDir, HdfsConstants.OnessdStoragePolicyName);
			// set quota by storage type on testDir
			long SsdQuota = Blocksize * 4;
			dfs.SetQuotaByStorageType(testDir, StorageType.Ssd, SsdQuota);
			INode testDirNode = fsdir.GetINode4Write(testDir.ToString());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under testDir
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify SSD consumed before namenode restart
			long ssdConsumed = testDirNode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumed);
			// Restart the namenode with checkpoint to make sure fsImage is correct
			dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			dfs.SaveNamespace();
			dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			cluster.RestartNameNode(true);
			RefreshClusterState();
			INode testDirNodeAfterNNRestart = fsdir.GetINode4Write(testDir.ToString());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(testDirNode.IsQuotaSet());
			QuotaCounts qc = testDirNodeAfterNNRestart.GetQuotaCounts();
			NUnit.Framework.Assert.AreEqual(SsdQuota, qc.GetTypeSpace(StorageType.Ssd));
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				if (t != StorageType.Ssd)
				{
					NUnit.Framework.Assert.AreEqual(HdfsConstants.QuotaReset, qc.GetTypeSpace(t));
				}
			}
			long ssdConsumedAfterNNRestart = testDirNodeAfterNNRestart.AsDirectory().GetDirectoryWithQuotaFeature
				().GetSpaceConsumed().GetTypeSpaces().Get(StorageType.Ssd);
			NUnit.Framework.Assert.AreEqual(file1Len, ssdConsumedAfterNNRestart);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContentSummaryWithoutQuotaByStorageType()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			// set storage policy on directory "foo" to ONESSD
			dfs.SetStoragePolicy(foo, HdfsConstants.OnessdStoragePolicyName);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(!fnode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify getContentSummary without any quota set
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), file1Len * Replication);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Ssd), file1Len);
			NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(StorageType.Disk), file1Len * 
				2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContentSummaryWithoutStoragePolicy()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile1 = new Path(foo, "created_file1.data");
			dfs.Mkdirs(foo);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(!fnode.IsQuotaSet());
			// Create file of size 2 * BLOCKSIZE under directory "foo"
			long file1Len = Blocksize * 2;
			int bufLen = Blocksize / 16;
			DFSTestUtil.CreateFile(dfs, createdFile1, bufLen, file1Len, Blocksize, Replication
				, seed);
			// Verify getContentSummary without any quota set
			// Expect no type quota and usage information available
			ContentSummary cs = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(cs.GetSpaceConsumed(), file1Len * Replication);
			foreach (StorageType t in StorageType.Values())
			{
				NUnit.Framework.Assert.AreEqual(cs.GetTypeConsumed(t), 0);
				NUnit.Framework.Assert.AreEqual(cs.GetTypeQuota(t), -1);
			}
		}
	}
}
