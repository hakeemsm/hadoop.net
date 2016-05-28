using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFSImage
	{
		private const string Hadoop27Zer0BlockSizeTgz = "image-with-zero-block-size.tar.gz";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPersist()
		{
			Configuration conf = new Configuration();
			TestPersistHelper(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCompression()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsImageCompressKey, true);
			conf.Set(DFSConfigKeys.DfsImageCompressionCodecKey, "org.apache.hadoop.io.compress.GzipCodec"
				);
			TestPersistHelper(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestPersistHelper(Configuration conf)
		{
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNamesystem();
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path dir = new Path("/abc/def");
				Path file1 = new Path(dir, "f1");
				Path file2 = new Path(dir, "f2");
				// create an empty file f1
				fs.Create(file1).Close();
				// create an under-construction file f2
				FSDataOutputStream @out = fs.Create(file2);
				@out.WriteBytes("hello");
				((DFSOutputStream)@out.GetWrappedStream()).Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag
					.UpdateLength));
				// checkpoint
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				cluster.RestartNameNode();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(fs.IsDirectory(dir));
				NUnit.Framework.Assert.IsTrue(fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(file2));
				// check internals of file2
				INodeFile file2Node = fsn.dir.GetINode4Write(file2.ToString()).AsFile();
				NUnit.Framework.Assert.AreEqual("hello".Length, file2Node.ComputeFileSize());
				NUnit.Framework.Assert.IsTrue(file2Node.IsUnderConstruction());
				BlockInfoContiguous[] blks = file2Node.GetBlocks();
				NUnit.Framework.Assert.AreEqual(1, blks.Length);
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.BlockUCState.UnderConstruction
					, blks[0].GetBlockUCState());
				// check lease manager
				LeaseManager.Lease lease = fsn.leaseManager.GetLeaseByPath(file2.ToString());
				NUnit.Framework.Assert.IsNotNull(lease);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Ensure that the digest written by the saver equals to the digest of the
		/// file.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDigest()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				DistributedFileSystem fs = cluster.GetFileSystem();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				fs.SaveNamespace();
				fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				FilePath currentDir = FSImageTestUtil.GetNameNodeCurrentDirs(cluster, 0)[0];
				FilePath fsimage = FSImageTestUtil.FindNewestImageFile(currentDir.GetAbsolutePath
					());
				NUnit.Framework.Assert.AreEqual(MD5FileUtils.ReadStoredMd5ForFile(fsimage), MD5FileUtils
					.ComputeMd5ForFile(fsimage));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Ensure mtime and atime can be loaded from fsimage.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestLoadMtimeAtime()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				string userDir = hdfs.GetHomeDirectory().ToUri().GetPath().ToString();
				Path file = new Path(userDir, "file");
				Path dir = new Path(userDir, "/dir");
				Path link = new Path(userDir, "/link");
				hdfs.CreateNewFile(file);
				hdfs.Mkdirs(dir);
				hdfs.CreateSymlink(file, link, false);
				long mtimeFile = hdfs.GetFileStatus(file).GetModificationTime();
				long atimeFile = hdfs.GetFileStatus(file).GetAccessTime();
				long mtimeDir = hdfs.GetFileStatus(dir).GetModificationTime();
				long mtimeLink = hdfs.GetFileLinkStatus(link).GetModificationTime();
				long atimeLink = hdfs.GetFileLinkStatus(link).GetAccessTime();
				// save namespace and restart cluster
				hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				hdfs.SaveNamespace();
				hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				cluster.Shutdown();
				cluster = new MiniDFSCluster.Builder(conf).Format(false).NumDataNodes(1).Build();
				cluster.WaitActive();
				hdfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.AreEqual(mtimeFile, hdfs.GetFileStatus(file).GetModificationTime
					());
				NUnit.Framework.Assert.AreEqual(atimeFile, hdfs.GetFileStatus(file).GetAccessTime
					());
				NUnit.Framework.Assert.AreEqual(mtimeDir, hdfs.GetFileStatus(dir).GetModificationTime
					());
				NUnit.Framework.Assert.AreEqual(mtimeLink, hdfs.GetFileLinkStatus(link).GetModificationTime
					());
				NUnit.Framework.Assert.AreEqual(atimeLink, hdfs.GetFileLinkStatus(link).GetAccessTime
					());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// In this test case, I have created an image with a file having
		/// preferredblockSize = 0.
		/// </summary>
		/// <remarks>
		/// In this test case, I have created an image with a file having
		/// preferredblockSize = 0. We are trying to read this image (since file with
		/// preferredblockSize = 0 was allowed pre 2.1.0-beta version. The namenode
		/// after 2.6 version will not be able to read this particular file.
		/// See HDFS-7788 for more information.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroBlockSize()
		{
			Configuration conf = new HdfsConfiguration();
			string tarFile = Runtime.GetProperty("test.cache.data", "build/test/cache") + "/"
				 + Hadoop27Zer0BlockSizeTgz;
			string testDir = PathUtils.GetTestDirName(GetType());
			FilePath dfsDir = new FilePath(testDir, "image-with-zero-block-size");
			if (dfsDir.Exists() && !FileUtil.FullyDelete(dfsDir))
			{
				throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
			}
			FileUtil.UnTar(new FilePath(tarFile), new FilePath(testDir));
			FilePath nameDir = new FilePath(dfsDir, "name");
			GenericTestUtils.AssertExists(nameDir);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).WaitSafeMode(false).StartupOption
				(HdfsServerConstants.StartupOption.Upgrade).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				Path testPath = new Path("/tmp/zeroBlockFile");
				NUnit.Framework.Assert.IsTrue("File /tmp/zeroBlockFile doesn't exist ", fs.Exists
					(testPath));
				NUnit.Framework.Assert.IsTrue("Name node didn't come up", cluster.IsNameNodeUp(0)
					);
			}
			finally
			{
				cluster.Shutdown();
				//Clean up
				FileUtil.FullyDelete(dfsDir);
			}
		}
	}
}
