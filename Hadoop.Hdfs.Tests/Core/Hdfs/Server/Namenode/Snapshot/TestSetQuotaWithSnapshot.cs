using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	public class TestSetQuotaWithSnapshot
	{
		protected internal const long seed = 0;

		protected internal const short Replication = 3;

		protected internal const long Blocksize = 1024;

		protected internal Configuration conf;

		protected internal MiniDFSCluster cluster;

		protected internal FSNamesystem fsn;

		protected internal FSDirectory fsdir;

		protected internal DistributedFileSystem hdfs;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Format(true)
				.Build();
			cluster.WaitActive();
			fsn = cluster.GetNamesystem();
			fsdir = fsn.GetFSDirectory();
			hdfs = cluster.GetFileSystem();
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
		public virtual void TestSetQuota()
		{
			Path dir = new Path("/TestSnapshot");
			hdfs.Mkdirs(dir);
			// allow snapshot on dir and create snapshot s1
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			Path sub = new Path(dir, "sub");
			hdfs.Mkdirs(sub);
			Path fileInSub = new Path(sub, "file");
			DFSTestUtil.CreateFile(hdfs, fileInSub, Blocksize, Replication, seed);
			INodeDirectory subNode = INodeDirectory.ValueOf(fsdir.GetINode(sub.ToString()), sub
				);
			// subNode should be a INodeDirectory, but not an INodeDirectoryWithSnapshot
			NUnit.Framework.Assert.IsFalse(subNode.IsWithSnapshot());
			hdfs.SetQuota(sub, long.MaxValue - 1, long.MaxValue - 1);
			subNode = INodeDirectory.ValueOf(fsdir.GetINode(sub.ToString()), sub);
			NUnit.Framework.Assert.IsTrue(subNode.IsQuotaSet());
			NUnit.Framework.Assert.IsFalse(subNode.IsWithSnapshot());
		}

		/// <summary>Test clear quota of a snapshottable dir or a dir with snapshot.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClearQuota()
		{
			Path dir = new Path("/TestSnapshot");
			hdfs.Mkdirs(dir);
			hdfs.AllowSnapshot(dir);
			hdfs.SetQuota(dir, HdfsConstants.QuotaDontSet, HdfsConstants.QuotaDontSet);
			INodeDirectory dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(0, dirNode.GetDiffs().AsList().Count);
			hdfs.SetQuota(dir, HdfsConstants.QuotaDontSet - 1, HdfsConstants.QuotaDontSet - 1
				);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(0, dirNode.GetDiffs().AsList().Count);
			hdfs.SetQuota(dir, HdfsConstants.QuotaReset, HdfsConstants.QuotaReset);
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(0, dirNode.GetDiffs().AsList().Count);
			// allow snapshot on dir and create snapshot s1
			SnapshotTestHelper.CreateSnapshot(hdfs, dir, "s1");
			// clear quota of dir
			hdfs.SetQuota(dir, HdfsConstants.QuotaReset, HdfsConstants.QuotaReset);
			// dir should still be a snapshottable directory
			dirNode = fsdir.GetINode4Write(dir.ToString()).AsDirectory();
			NUnit.Framework.Assert.IsTrue(dirNode.IsSnapshottable());
			NUnit.Framework.Assert.AreEqual(1, dirNode.GetDiffs().AsList().Count);
			SnapshottableDirectoryStatus[] status = hdfs.GetSnapshottableDirListing();
			NUnit.Framework.Assert.AreEqual(1, status.Length);
			NUnit.Framework.Assert.AreEqual(dir, status[0].GetFullPath());
			Path subDir = new Path(dir, "sub");
			hdfs.Mkdirs(subDir);
			hdfs.CreateSnapshot(dir, "s2");
			Path file = new Path(subDir, "file");
			DFSTestUtil.CreateFile(hdfs, file, Blocksize, Replication, seed);
			hdfs.SetQuota(dir, HdfsConstants.QuotaReset, HdfsConstants.QuotaReset);
			INode subNode = fsdir.GetINode4Write(subDir.ToString());
			NUnit.Framework.Assert.IsTrue(subNode.AsDirectory().IsWithSnapshot());
			IList<DirectoryWithSnapshotFeature.DirectoryDiff> diffList = subNode.AsDirectory(
				).GetDiffs().AsList();
			NUnit.Framework.Assert.AreEqual(1, diffList.Count);
			Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot s2 = dirNode.GetSnapshot
				(DFSUtil.String2Bytes("s2"));
			NUnit.Framework.Assert.AreEqual(s2.GetId(), diffList[0].GetSnapshotId());
			IList<INode> createdList = diffList[0].GetChildrenDiff().GetList(Diff.ListType.Created
				);
			NUnit.Framework.Assert.AreEqual(1, createdList.Count);
			NUnit.Framework.Assert.AreSame(fsdir.GetINode4Write(file.ToString()), createdList
				[0]);
		}
	}
}
