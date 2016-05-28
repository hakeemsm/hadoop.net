using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestDiskspaceQuotaUpdate
	{
		private const int Blocksize = 1024;

		private const short Replication = 4;

		internal const long seed = 0L;

		private static readonly Path dir = new Path("/TestQuotaUpdate");

		private Configuration conf;

		private MiniDFSCluster cluster;

		private FSDirectory fsdir;

		private DistributedFileSystem dfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			fsdir = cluster.GetNamesystem().GetFSDirectory();
			dfs = cluster.GetFileSystem();
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

		/// <summary>Test if the quota can be correctly updated for create file</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQuotaUpdateWithFileCreate()
		{
			Path foo = new Path(dir, "foo");
			Path createdFile = new Path(foo, "created_file.data");
			dfs.Mkdirs(foo);
			dfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			long fileLen = Blocksize * 2 + Blocksize / 2;
			DFSTestUtil.CreateFile(dfs, createdFile, Blocksize / 16, fileLen, Blocksize, Replication
				, seed);
			INode fnode = fsdir.GetINode4Write(foo.ToString());
			NUnit.Framework.Assert.IsTrue(fnode.IsDirectory());
			NUnit.Framework.Assert.IsTrue(fnode.IsQuotaSet());
			QuotaCounts cnt = fnode.AsDirectory().GetDirectoryWithQuotaFeature().GetSpaceConsumed
				();
			NUnit.Framework.Assert.AreEqual(2, cnt.GetNameSpace());
			NUnit.Framework.Assert.AreEqual(fileLen * Replication, cnt.GetStorageSpace());
		}

		/// <summary>Test if the quota can be correctly updated for append</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateQuotaForAppend()
		{
			Path foo = new Path(dir, "foo");
			Path bar = new Path(foo, "bar");
			long currentFileLen = Blocksize;
			DFSTestUtil.CreateFile(dfs, bar, currentFileLen, Replication, seed);
			dfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			// append half of the block data, the previous file length is at block
			// boundary
			DFSTestUtil.AppendFile(dfs, bar, Blocksize / 2);
			currentFileLen += (Blocksize / 2);
			INodeDirectory fooNode = fsdir.GetINode4Write(foo.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(fooNode.IsQuotaSet());
			QuotaCounts quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			long ns = quota.GetNameSpace();
			long ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			// foo and bar
			NUnit.Framework.Assert.AreEqual(currentFileLen * Replication, ds);
			ContentSummary c = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), ds);
			// append another block, the previous file length is not at block boundary
			DFSTestUtil.AppendFile(dfs, bar, Blocksize);
			currentFileLen += Blocksize;
			quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			ns = quota.GetNameSpace();
			ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			// foo and bar
			NUnit.Framework.Assert.AreEqual(currentFileLen * Replication, ds);
			c = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), ds);
			// append several blocks
			DFSTestUtil.AppendFile(dfs, bar, Blocksize * 3 + Blocksize / 8);
			currentFileLen += (Blocksize * 3 + Blocksize / 8);
			quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			ns = quota.GetNameSpace();
			ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			// foo and bar
			NUnit.Framework.Assert.AreEqual(currentFileLen * Replication, ds);
			c = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), ds);
		}

		/// <summary>
		/// Test if the quota can be correctly updated when file length is updated
		/// through fsync
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateQuotaForFSync()
		{
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			DFSTestUtil.CreateFile(dfs, bar, Blocksize, Replication, 0L);
			dfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			FSDataOutputStream @out = dfs.Append(bar);
			@out.Write(new byte[Blocksize / 4]);
			((DFSOutputStream)@out.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
				.UpdateLength));
			INodeDirectory fooNode = fsdir.GetINode4Write(foo.ToString()).AsDirectory();
			QuotaCounts quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			long ns = quota.GetNameSpace();
			long ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			// foo and bar
			NUnit.Framework.Assert.AreEqual(Blocksize * 2 * Replication, ds);
			// file is under construction
			@out.Write(new byte[Blocksize / 4]);
			@out.Close();
			fooNode = fsdir.GetINode4Write(foo.ToString()).AsDirectory();
			quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			ns = quota.GetNameSpace();
			ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			NUnit.Framework.Assert.AreEqual((Blocksize + Blocksize / 2) * Replication, ds);
			// append another block
			DFSTestUtil.AppendFile(dfs, bar, Blocksize);
			quota = fooNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed();
			ns = quota.GetNameSpace();
			ds = quota.GetStorageSpace();
			NUnit.Framework.Assert.AreEqual(2, ns);
			// foo and bar
			NUnit.Framework.Assert.AreEqual((Blocksize * 2 + Blocksize / 2) * Replication, ds
				);
		}

		/// <summary>Test append over storage quota does not mark file as UC or create lease</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppendOverStorageQuota()
		{
			Path dir = new Path("/TestAppendOverQuota");
			Path file = new Path(dir, "file");
			// create partial block file
			dfs.Mkdirs(dir);
			DFSTestUtil.CreateFile(dfs, file, Blocksize / 2, Replication, seed);
			// lower quota to cause exception when appending to partial block
			dfs.SetQuota(dir, long.MaxValue - 1, 1);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			long spaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			try
			{
				DFSTestUtil.AppendFile(dfs, file, Blocksize);
				NUnit.Framework.Assert.Fail("append didn't fail");
			}
			catch (DSQuotaExceededException)
			{
			}
			// ignore
			// check that the file exists, isn't UC, and has no dangling lease
			INodeFile inode = fsdir.GetINode(file.ToString()).AsFile();
			NUnit.Framework.Assert.IsNotNull(inode);
			NUnit.Framework.Assert.IsFalse("should not be UC", inode.IsUnderConstruction());
			NUnit.Framework.Assert.IsNull("should not have a lease", cluster.GetNamesystem().
				GetLeaseManager().GetLeaseByPath(file.ToString()));
			// make sure the quota usage is unchanged
			long newSpaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			NUnit.Framework.Assert.AreEqual(spaceUsed, newSpaceUsed);
			// make sure edits aren't corrupted
			dfs.RecoverLease(file);
			cluster.RestartNameNodes();
		}

		/// <summary>
		/// Test append over a specific type of storage quota does not mark file as
		/// UC or create a lease
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppendOverTypeQuota()
		{
			Path dir = new Path("/TestAppendOverTypeQuota");
			Path file = new Path(dir, "file");
			// create partial block file
			dfs.Mkdirs(dir);
			// set the storage policy on dir
			dfs.SetStoragePolicy(dir, HdfsConstants.OnessdStoragePolicyName);
			DFSTestUtil.CreateFile(dfs, file, Blocksize / 2, Replication, seed);
			// set quota of SSD to 1L
			dfs.SetQuotaByStorageType(dir, StorageType.Ssd, 1L);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			long spaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			try
			{
				DFSTestUtil.AppendFile(dfs, file, Blocksize);
				NUnit.Framework.Assert.Fail("append didn't fail");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.IsTrue(e.GetClassName().Contains("QuotaByStorageTypeExceededException"
					));
			}
			// check that the file exists, isn't UC, and has no dangling lease
			INodeFile inode = fsdir.GetINode(file.ToString()).AsFile();
			NUnit.Framework.Assert.IsNotNull(inode);
			NUnit.Framework.Assert.IsFalse("should not be UC", inode.IsUnderConstruction());
			NUnit.Framework.Assert.IsNull("should not have a lease", cluster.GetNamesystem().
				GetLeaseManager().GetLeaseByPath(file.ToString()));
			// make sure the quota usage is unchanged
			long newSpaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			NUnit.Framework.Assert.AreEqual(spaceUsed, newSpaceUsed);
			// make sure edits aren't corrupted
			dfs.RecoverLease(file);
			cluster.RestartNameNodes();
		}

		/// <summary>Test truncate over quota does not mark file as UC or create a lease</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTruncateOverQuota()
		{
			Path dir = new Path("/TestTruncateOverquota");
			Path file = new Path(dir, "file");
			// create partial block file
			dfs.Mkdirs(dir);
			DFSTestUtil.CreateFile(dfs, file, Blocksize / 2, Replication, seed);
			// lower quota to cause exception when appending to partial block
			dfs.SetQuota(dir, long.MaxValue - 1, 1);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			long spaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			try
			{
				dfs.Truncate(file, Blocksize / 2 - 1);
				NUnit.Framework.Assert.Fail("truncate didn't fail");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.IsTrue(e.GetClassName().Contains("DSQuotaExceededException"
					));
			}
			// check that the file exists, isn't UC, and has no dangling lease
			INodeFile inode = fsdir.GetINode(file.ToString()).AsFile();
			NUnit.Framework.Assert.IsNotNull(inode);
			NUnit.Framework.Assert.IsFalse("should not be UC", inode.IsUnderConstruction());
			NUnit.Framework.Assert.IsNull("should not have a lease", cluster.GetNamesystem().
				GetLeaseManager().GetLeaseByPath(file.ToString()));
			// make sure the quota usage is unchanged
			long newSpaceUsed = dirNode.GetDirectoryWithQuotaFeature().GetSpaceConsumed().GetStorageSpace
				();
			NUnit.Framework.Assert.AreEqual(spaceUsed, newSpaceUsed);
			// make sure edits aren't corrupted
			dfs.RecoverLease(file);
			cluster.RestartNameNodes();
		}
	}
}
