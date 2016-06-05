using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestINodeFile
	{
		static TestINodeFile()
		{
			// Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
			FileSystem.EnableSymlinks();
		}

		public static readonly Log Log = LogFactory.GetLog(typeof(TestINodeFile));

		internal const short Blockbits = 48;

		internal const long BlksizeMaxvalue = ~(unchecked((long)(0xffffL)) << Blockbits);

		private static readonly PermissionStatus perm = new PermissionStatus("userName", 
			null, FsPermission.GetDefault());

		private short replication;

		private long preferredBlockSize = 1024;

		internal virtual INodeFile CreateINodeFile(short replication, long preferredBlockSize
			)
		{
			return new INodeFile(INodeId.GrandfatherInodeId, null, perm, 0L, 0L, null, replication
				, preferredBlockSize, unchecked((byte)0));
		}

		private static INodeFile CreateINodeFile(byte storagePolicyID)
		{
			return new INodeFile(INodeId.GrandfatherInodeId, null, perm, 0L, 0L, null, (short
				)3, 1024L, storagePolicyID);
		}

		[NUnit.Framework.Test]
		public virtual void TestStoragePolicyID()
		{
			for (byte i = 0; ((sbyte)i) < 16; i++)
			{
				INodeFile f = CreateINodeFile(i);
				NUnit.Framework.Assert.AreEqual(i, f.GetStoragePolicyID());
			}
		}

		/// <exception cref="System.ArgumentException"/>
		public virtual void TestStoragePolicyIdBelowLowerBound()
		{
			CreateINodeFile(unchecked((byte)-1));
		}

		/// <exception cref="System.ArgumentException"/>
		public virtual void TestStoragePolicyIdAboveUpperBound()
		{
			CreateINodeFile(unchecked((byte)16));
		}

		/// <summary>Test for the Replication value.</summary>
		/// <remarks>
		/// Test for the Replication value. Sets a value and checks if it was set
		/// correct.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestReplication()
		{
			replication = 3;
			preferredBlockSize = 128 * 1024 * 1024;
			INodeFile inf = CreateINodeFile(replication, preferredBlockSize);
			NUnit.Framework.Assert.AreEqual("True has to be returned in this case", replication
				, inf.GetFileReplication());
		}

		/// <summary>
		/// IllegalArgumentException is expected for setting below lower bound
		/// for Replication.
		/// </summary>
		/// <exception cref="System.ArgumentException">as the result</exception>
		public virtual void TestReplicationBelowLowerBound()
		{
			replication = -1;
			preferredBlockSize = 128 * 1024 * 1024;
			CreateINodeFile(replication, preferredBlockSize);
		}

		/// <summary>Test for the PreferredBlockSize value.</summary>
		/// <remarks>
		/// Test for the PreferredBlockSize value. Sets a value and checks if it was
		/// set correct.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestPreferredBlockSize()
		{
			replication = 3;
			preferredBlockSize = 128 * 1024 * 1024;
			INodeFile inf = CreateINodeFile(replication, preferredBlockSize);
			NUnit.Framework.Assert.AreEqual("True has to be returned in this case", preferredBlockSize
				, inf.GetPreferredBlockSize());
		}

		[NUnit.Framework.Test]
		public virtual void TestPreferredBlockSizeUpperBound()
		{
			replication = 3;
			preferredBlockSize = BlksizeMaxvalue;
			INodeFile inf = CreateINodeFile(replication, preferredBlockSize);
			NUnit.Framework.Assert.AreEqual("True has to be returned in this case", BlksizeMaxvalue
				, inf.GetPreferredBlockSize());
		}

		/// <summary>
		/// IllegalArgumentException is expected for setting below lower bound
		/// for PreferredBlockSize.
		/// </summary>
		/// <exception cref="System.ArgumentException">as the result</exception>
		public virtual void TestPreferredBlockSizeBelowLowerBound()
		{
			replication = 3;
			preferredBlockSize = -1;
			CreateINodeFile(replication, preferredBlockSize);
		}

		/// <summary>
		/// IllegalArgumentException is expected for setting above upper bound
		/// for PreferredBlockSize.
		/// </summary>
		/// <exception cref="System.ArgumentException">as the result</exception>
		public virtual void TestPreferredBlockSizeAboveUpperBound()
		{
			replication = 3;
			preferredBlockSize = BlksizeMaxvalue + 1;
			CreateINodeFile(replication, preferredBlockSize);
		}

		[NUnit.Framework.Test]
		public virtual void TestGetFullPathName()
		{
			replication = 3;
			preferredBlockSize = 128 * 1024 * 1024;
			INodeFile inf = CreateINodeFile(replication, preferredBlockSize);
			inf.SetLocalName(DFSUtil.String2Bytes("f"));
			INodeDirectory root = new INodeDirectory(INodeId.GrandfatherInodeId, INodeDirectory
				.RootName, perm, 0L);
			INodeDirectory dir = new INodeDirectory(INodeId.GrandfatherInodeId, DFSUtil.String2Bytes
				("d"), perm, 0L);
			NUnit.Framework.Assert.AreEqual("f", inf.GetFullPathName());
			dir.AddChild(inf);
			NUnit.Framework.Assert.AreEqual("d" + Path.Separator + "f", inf.GetFullPathName()
				);
			root.AddChild(dir);
			NUnit.Framework.Assert.AreEqual(Path.Separator + "d" + Path.Separator + "f", inf.
				GetFullPathName());
			NUnit.Framework.Assert.AreEqual(Path.Separator + "d", dir.GetFullPathName());
			NUnit.Framework.Assert.AreEqual(Path.Separator, root.GetFullPathName());
		}

		/// <summary>
		/// FSDirectory#unprotectedSetQuota creates a new INodeDirectoryWithQuota to
		/// replace the original INodeDirectory.
		/// </summary>
		/// <remarks>
		/// FSDirectory#unprotectedSetQuota creates a new INodeDirectoryWithQuota to
		/// replace the original INodeDirectory. Before HDFS-4243, the parent field of
		/// all the children INodes of the target INodeDirectory is not changed to
		/// point to the new INodeDirectoryWithQuota. This testcase tests this
		/// scenario.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFullPathNameAfterSetQuota()
		{
			long fileLen = 1024;
			replication = 3;
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(replication).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNamesystem();
				FSDirectory fsdir = fsn.GetFSDirectory();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// Create a file for test
				Path dir = new Path("/dir");
				Path file = new Path(dir, "file");
				DFSTestUtil.CreateFile(dfs, file, fileLen, replication, 0L);
				// Check the full path name of the INode associating with the file
				INode fnode = fsdir.GetINode(file.ToString());
				NUnit.Framework.Assert.AreEqual(file.ToString(), fnode.GetFullPathName());
				// Call FSDirectory#unprotectedSetQuota which calls
				// INodeDirectory#replaceChild
				dfs.SetQuota(dir, long.MaxValue - 1, replication * fileLen * 10);
				INodeDirectory dirNode = GetDir(fsdir, dir);
				NUnit.Framework.Assert.AreEqual(dir.ToString(), dirNode.GetFullPathName());
				NUnit.Framework.Assert.IsTrue(dirNode.IsWithQuota());
				Path newDir = new Path("/newdir");
				Path newFile = new Path(newDir, "file");
				// Also rename dir
				dfs.Rename(dir, newDir, Options.Rename.Overwrite);
				// /dir/file now should be renamed to /newdir/file
				fnode = fsdir.GetINode(newFile.ToString());
				// getFullPathName can return correct result only if the parent field of
				// child node is set correctly
				NUnit.Framework.Assert.AreEqual(newFile.ToString(), fnode.GetFullPathName());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestConcatBlocks()
		{
			INodeFile origFile = CreateINodeFiles(1, "origfile")[0];
			NUnit.Framework.Assert.AreEqual("Number of blocks didn't match", origFile.NumBlocks
				(), 1L);
			INodeFile[] appendFiles = CreateINodeFiles(4, "appendfile");
			origFile.ConcatBlocks(appendFiles);
			NUnit.Framework.Assert.AreEqual("Number of blocks didn't match", origFile.NumBlocks
				(), 5L);
		}

		/// <summary>Creates the required number of files with one block each</summary>
		/// <param name="nCount">Number of INodes to create</param>
		/// <returns>Array of INode files</returns>
		private INodeFile[] CreateINodeFiles(int nCount, string fileNamePrefix)
		{
			if (nCount <= 0)
			{
				return new INodeFile[1];
			}
			replication = 3;
			preferredBlockSize = 128 * 1024 * 1024;
			INodeFile[] iNodes = new INodeFile[nCount];
			for (int i = 0; i < nCount; i++)
			{
				iNodes[i] = new INodeFile(i, null, perm, 0L, 0L, null, replication, preferredBlockSize
					, unchecked((byte)0));
				iNodes[i].SetLocalName(DFSUtil.String2Bytes(fileNamePrefix + i));
				BlockInfoContiguous newblock = new BlockInfoContiguous(replication);
				iNodes[i].AddBlock(newblock);
			}
			return iNodes;
		}

		/// <summary>
		/// Test for the static
		/// <see cref="INodeFile.ValueOf(INode, string)"/>
		/// and
		/// <see cref="INodeFileUnderConstruction#valueOf(INode,String)"/>
		/// methods.
		/// </summary>
		/// <exception cref="System.IO.IOException"></exception>
		[NUnit.Framework.Test]
		public virtual void TestValueOf()
		{
			string path = "/testValueOf";
			short replication = 3;
			{
				//cast from null
				INode from = null;
				//cast to INodeFile, should fail
				try
				{
					INodeFile.ValueOf(from, path);
					NUnit.Framework.Assert.Fail();
				}
				catch (FileNotFoundException fnfe)
				{
					NUnit.Framework.Assert.IsTrue(fnfe.Message.Contains("File does not exist"));
				}
				//cast to INodeDirectory, should fail
				try
				{
					INodeDirectory.ValueOf(from, path);
					NUnit.Framework.Assert.Fail();
				}
				catch (FileNotFoundException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("Directory does not exist"));
				}
			}
			{
				//cast from INodeFile
				INode from = CreateINodeFile(replication, preferredBlockSize);
				//cast to INodeFile, should success
				INodeFile f = INodeFile.ValueOf(from, path);
				NUnit.Framework.Assert.IsTrue(f == from);
				//cast to INodeDirectory, should fail
				try
				{
					INodeDirectory.ValueOf(from, path);
					NUnit.Framework.Assert.Fail();
				}
				catch (PathIsNotDirectoryException)
				{
				}
			}
			{
				// Expected
				//cast from INodeFileUnderConstruction
				INode from = new INodeFile(INodeId.GrandfatherInodeId, null, perm, 0L, 0L, null, 
					replication, 1024L, unchecked((byte)0));
				from.AsFile().ToUnderConstruction("client", "machine");
				//cast to INodeFile, should success
				INodeFile f = INodeFile.ValueOf(from, path);
				NUnit.Framework.Assert.IsTrue(f == from);
				//cast to INodeDirectory, should fail
				try
				{
					INodeDirectory.ValueOf(from, path);
					NUnit.Framework.Assert.Fail();
				}
				catch (PathIsNotDirectoryException)
				{
				}
			}
			{
				// expected
				//cast from INodeDirectory
				INode from = new INodeDirectory(INodeId.GrandfatherInodeId, null, perm, 0L);
				//cast to INodeFile, should fail
				try
				{
					INodeFile.ValueOf(from, path);
					NUnit.Framework.Assert.Fail();
				}
				catch (FileNotFoundException fnfe)
				{
					NUnit.Framework.Assert.IsTrue(fnfe.Message.Contains("Path is not a file"));
				}
				//cast to INodeDirectory, should success
				INodeDirectory d = INodeDirectory.ValueOf(from, path);
				NUnit.Framework.Assert.IsTrue(d == from);
			}
		}

		/// <summary>This test verifies inode ID counter and inode map functionality.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInodeId()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys.DfsBytesPerChecksumDefault
				);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNamesystem();
				long lastId = fsn.dir.GetLastInodeId();
				// Ensure root has the correct inode ID
				// Last inode ID should be root inode ID and inode map size should be 1
				int inodeCount = 1;
				long expectedLastInodeId = INodeId.RootInodeId;
				NUnit.Framework.Assert.AreEqual(fsn.dir.rootDir.GetId(), INodeId.RootInodeId);
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, lastId);
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Create a directory
				// Last inode ID and inode map size should increase by 1
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/test1");
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
				NUnit.Framework.Assert.AreEqual(++expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(++inodeCount, fsn.dir.GetInodeMapSize());
				// Create a file
				// Last inode ID and inode map size should increase by 1
				NamenodeProtocols nnrpc = cluster.GetNameNodeRpc();
				DFSTestUtil.CreateFile(fs, new Path("/test1/file"), 1024, (short)1, 0);
				NUnit.Framework.Assert.AreEqual(++expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(++inodeCount, fsn.dir.GetInodeMapSize());
				// Ensure right inode ID is returned in file status
				HdfsFileStatus fileStatus = nnrpc.GetFileInfo("/test1/file");
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fileStatus.GetFileId());
				// Rename a directory
				// Last inode ID and inode map size should not change
				Path renamedPath = new Path("/test2");
				NUnit.Framework.Assert.IsTrue(fs.Rename(path, renamedPath));
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Delete test2/file and test2 and ensure inode map size decreases
				NUnit.Framework.Assert.IsTrue(fs.Delete(renamedPath, true));
				inodeCount -= 2;
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Create and concat /test/file1 /test/file2
				// Create /test1/file1 and /test1/file2
				string file1 = "/test1/file1";
				string file2 = "/test1/file2";
				DFSTestUtil.CreateFile(fs, new Path(file1), 512, (short)1, 0);
				DFSTestUtil.CreateFile(fs, new Path(file2), 512, (short)1, 0);
				inodeCount += 3;
				// test1, file1 and file2 are created
				expectedLastInodeId += 3;
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				// Concat the /test1/file1 /test1/file2 into /test1/file2
				nnrpc.Concat(file2, new string[] { file1 });
				inodeCount--;
				// file1 and file2 are concatenated to file2
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.IsTrue(fs.Delete(new Path("/test1"), true));
				inodeCount -= 2;
				// test1 and file2 is deleted
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Make sure editlog is loaded correctly 
				cluster.RestartNameNode();
				cluster.WaitActive();
				fsn = cluster.GetNamesystem();
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Create two inodes test2 and test2/file2
				DFSTestUtil.CreateFile(fs, new Path("/test2/file2"), 1024, (short)1, 0);
				expectedLastInodeId += 2;
				inodeCount += 2;
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// create /test3, and /test3/file.
				// /test3/file is a file under construction
				FSDataOutputStream outStream = fs.Create(new Path("/test3/file"));
				NUnit.Framework.Assert.IsTrue(outStream != null);
				expectedLastInodeId += 2;
				inodeCount += 2;
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
				// Apply editlogs to fsimage, ensure inodeUnderConstruction is handled
				fsn.EnterSafeMode(false);
				fsn.SaveNamespace();
				fsn.LeaveSafeMode();
				outStream.Close();
				// The lastInodeId in fsimage should remain the same after reboot
				cluster.RestartNameNode();
				cluster.WaitActive();
				fsn = cluster.GetNamesystem();
				NUnit.Framework.Assert.AreEqual(expectedLastInodeId, fsn.dir.GetLastInodeId());
				NUnit.Framework.Assert.AreEqual(inodeCount, fsn.dir.GetInodeMapSize());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWriteToDeletedFile()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			Path path = new Path("/test1");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path));
			int size = conf.GetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 512);
			byte[] data = new byte[size];
			// Create one file
			Path filePath = new Path("/test1/file");
			FSDataOutputStream fos = fs.Create(filePath);
			// Delete the file
			fs.Delete(filePath, false);
			// Add new block should fail since /test1/file has been deleted.
			try
			{
				fos.Write(data, 0, data.Length);
				// make sure addBlock() request gets to NN immediately
				fos.Hflush();
				NUnit.Framework.Assert.Fail("Write should fail after delete");
			}
			catch (Exception)
			{
			}
			finally
			{
				/* Ignore */
				cluster.Shutdown();
			}
		}

		private Path GetInodePath(long inodeId, string remainingPath)
		{
			StringBuilder b = new StringBuilder();
			b.Append(Path.Separator).Append(FSDirectory.DotReservedString).Append(Path.Separator
				).Append(FSDirectory.DotInodesString).Append(Path.Separator).Append(inodeId).Append
				(Path.Separator).Append(remainingPath);
			Path p = new Path(b.ToString());
			Log.Info("Inode path is " + p);
			return p;
		}

		/// <summary>
		/// Tests for addressing files using /.reserved/.inodes/<inodeID> in file system
		/// operations.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInodeIdBasedPaths()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, DFSConfigKeys.DfsBytesPerChecksumDefault
				);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				NamenodeProtocols nnRpc = cluster.GetNameNodeRpc();
				// FileSystem#mkdirs "/testInodeIdBasedPaths"
				Path baseDir = GetInodePath(INodeId.RootInodeId, "testInodeIdBasedPaths");
				Path baseDirRegPath = new Path("/testInodeIdBasedPaths");
				fs.Mkdirs(baseDir);
				fs.Exists(baseDir);
				long baseDirFileId = nnRpc.GetFileInfo(baseDir.ToString()).GetFileId();
				// FileSystem#create file and FileSystem#close
				Path testFileInodePath = GetInodePath(baseDirFileId, "test1");
				Path testFileRegularPath = new Path(baseDir, "test1");
				int testFileBlockSize = 1024;
				FileSystemTestHelper.CreateFile(fs, testFileInodePath, 1, testFileBlockSize);
				NUnit.Framework.Assert.IsTrue(fs.Exists(testFileInodePath));
				// FileSystem#setPermission
				FsPermission perm = new FsPermission((short)0x1b6);
				fs.SetPermission(testFileInodePath, perm);
				// FileSystem#getFileStatus and FileSystem#getPermission
				FileStatus fileStatus = fs.GetFileStatus(testFileInodePath);
				NUnit.Framework.Assert.AreEqual(perm, fileStatus.GetPermission());
				// FileSystem#setOwner
				fs.SetOwner(testFileInodePath, fileStatus.GetOwner(), fileStatus.GetGroup());
				// FileSystem#setTimes
				fs.SetTimes(testFileInodePath, 0, 0);
				fileStatus = fs.GetFileStatus(testFileInodePath);
				NUnit.Framework.Assert.AreEqual(0, fileStatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(0, fileStatus.GetAccessTime());
				// FileSystem#setReplication
				fs.SetReplication(testFileInodePath, (short)3);
				fileStatus = fs.GetFileStatus(testFileInodePath);
				NUnit.Framework.Assert.AreEqual(3, fileStatus.GetReplication());
				fs.SetReplication(testFileInodePath, (short)1);
				// ClientProtocol#getPreferredBlockSize
				NUnit.Framework.Assert.AreEqual(testFileBlockSize, nnRpc.GetPreferredBlockSize(testFileInodePath
					.ToString()));
				{
					/*
					* HDFS-6749 added missing calls to FSDirectory.resolvePath in the
					* following four methods. The calls below ensure that
					* /.reserved/.inodes paths work properly. No need to check return
					* values as these methods are tested elsewhere.
					*/
					fs.IsFileClosed(testFileInodePath);
					fs.GetAclStatus(testFileInodePath);
					fs.GetXAttrs(testFileInodePath);
					fs.ListXAttrs(testFileInodePath);
					fs.Access(testFileInodePath, FsAction.ReadWrite);
				}
				// symbolic link related tests
				// Reserved path is not allowed as a target
				string invalidTarget = new Path(baseDir, "invalidTarget").ToString();
				string link = new Path(baseDir, "link").ToString();
				TestInvalidSymlinkTarget(nnRpc, invalidTarget, link);
				// Test creating a link using reserved inode path
				string validTarget = "/validtarget";
				TestValidSymlinkTarget(nnRpc, validTarget, link);
				// FileSystem#append
				fs.Append(testFileInodePath);
				// DistributedFileSystem#recoverLease
				fs.RecoverLease(testFileInodePath);
				// Namenode#getBlockLocations
				LocatedBlocks l1 = nnRpc.GetBlockLocations(testFileInodePath.ToString(), 0, long.MaxValue
					);
				LocatedBlocks l2 = nnRpc.GetBlockLocations(testFileRegularPath.ToString(), 0, long.MaxValue
					);
				CheckEquals(l1, l2);
				// FileSystem#rename - both the variants
				Path renameDst = GetInodePath(baseDirFileId, "test2");
				fileStatus = fs.GetFileStatus(testFileInodePath);
				// Rename variant 1: rename and rename bacck
				fs.Rename(testFileInodePath, renameDst);
				fs.Rename(renameDst, testFileInodePath);
				NUnit.Framework.Assert.AreEqual(fileStatus, fs.GetFileStatus(testFileInodePath));
				// Rename variant 2: rename and rename bacck
				fs.Rename(testFileInodePath, renameDst, Options.Rename.Overwrite);
				fs.Rename(renameDst, testFileInodePath, Options.Rename.Overwrite);
				NUnit.Framework.Assert.AreEqual(fileStatus, fs.GetFileStatus(testFileInodePath));
				// FileSystem#getContentSummary
				NUnit.Framework.Assert.AreEqual(fs.GetContentSummary(testFileRegularPath).ToString
					(), fs.GetContentSummary(testFileInodePath).ToString());
				// FileSystem#listFiles
				CheckEquals(fs.ListFiles(baseDirRegPath, false), fs.ListFiles(baseDir, false));
				// FileSystem#delete
				fs.Delete(testFileInodePath, true);
				NUnit.Framework.Assert.IsFalse(fs.Exists(testFileInodePath));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestInvalidSymlinkTarget(NamenodeProtocols nnRpc, string invalidTarget
			, string link)
		{
			try
			{
				FsPermission perm = FsPermission.CreateImmutable((short)0x1ed);
				nnRpc.CreateSymlink(invalidTarget, link, perm, false);
				NUnit.Framework.Assert.Fail("Symbolic link creation of target " + invalidTarget +
					 " should fail");
			}
			catch (InvalidPathException)
			{
			}
		}

		// Expected
		/// <exception cref="System.IO.IOException"/>
		private void TestValidSymlinkTarget(NamenodeProtocols nnRpc, string target, string
			 link)
		{
			FsPermission perm = FsPermission.CreateImmutable((short)0x1ed);
			nnRpc.CreateSymlink(target, link, perm, false);
			NUnit.Framework.Assert.AreEqual(target, nnRpc.GetLinkTarget(link));
		}

		private static void CheckEquals(LocatedBlocks l1, LocatedBlocks l2)
		{
			IList<LocatedBlock> list1 = l1.GetLocatedBlocks();
			IList<LocatedBlock> list2 = l2.GetLocatedBlocks();
			NUnit.Framework.Assert.AreEqual(list1.Count, list2.Count);
			for (int i = 0; i < list1.Count; i++)
			{
				LocatedBlock b1 = list1[i];
				LocatedBlock b2 = list2[i];
				NUnit.Framework.Assert.AreEqual(b1.GetBlock(), b2.GetBlock());
				NUnit.Framework.Assert.AreEqual(b1.GetBlockSize(), b2.GetBlockSize());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckEquals(RemoteIterator<LocatedFileStatus> i1, RemoteIterator
			<LocatedFileStatus> i2)
		{
			while (i1.HasNext())
			{
				NUnit.Framework.Assert.IsTrue(i2.HasNext());
				// Compare all the fields but the path name, which is relative
				// to the original path from listFiles.
				LocatedFileStatus l1 = i1.Next();
				LocatedFileStatus l2 = i2.Next();
				NUnit.Framework.Assert.AreEqual(l1.GetAccessTime(), l2.GetAccessTime());
				NUnit.Framework.Assert.AreEqual(l1.GetBlockSize(), l2.GetBlockSize());
				NUnit.Framework.Assert.AreEqual(l1.GetGroup(), l2.GetGroup());
				NUnit.Framework.Assert.AreEqual(l1.GetLen(), l2.GetLen());
				NUnit.Framework.Assert.AreEqual(l1.GetModificationTime(), l2.GetModificationTime(
					));
				NUnit.Framework.Assert.AreEqual(l1.GetOwner(), l2.GetOwner());
				NUnit.Framework.Assert.AreEqual(l1.GetPermission(), l2.GetPermission());
				NUnit.Framework.Assert.AreEqual(l1.GetReplication(), l2.GetReplication());
			}
			NUnit.Framework.Assert.IsFalse(i2.HasNext());
		}

		/// <summary>Check /.reserved path is reserved and cannot be created.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReservedFileNames()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				// First start a cluster with reserved file names check turned off
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// Creation of directory or file with reserved path names is disallowed
				EnsureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
				EnsureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
				Path reservedPath = new Path("/.reserved");
				// Loading of fsimage or editlog with /.reserved directory should fail
				// Mkdir "/.reserved reserved path with reserved path check turned off
				FSDirectory.CheckReservedFileNames = false;
				fs.Mkdirs(reservedPath);
				NUnit.Framework.Assert.IsTrue(fs.IsDirectory(reservedPath));
				EnsureReservedFileNamesCannotBeLoaded(cluster);
				// Loading of fsimage or editlog with /.reserved file should fail
				// Create file "/.reserved reserved path with reserved path check turned off
				FSDirectory.CheckReservedFileNames = false;
				EnsureClusterRestartSucceeds(cluster);
				fs.Delete(reservedPath, true);
				DFSTestUtil.CreateFile(fs, reservedPath, 10, (short)1, 0L);
				NUnit.Framework.Assert.IsTrue(!fs.IsDirectory(reservedPath));
				EnsureReservedFileNamesCannotBeLoaded(cluster);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private void EnsureReservedFileNamesCannotBeCreated(FileSystem fs, string name, bool
			 isDir)
		{
			// Creation of directory or file with reserved path names is disallowed
			Path reservedPath = new Path(name);
			try
			{
				if (isDir)
				{
					fs.Mkdirs(reservedPath);
				}
				else
				{
					DFSTestUtil.CreateFile(fs, reservedPath, 10, (short)1, 0L);
				}
				NUnit.Framework.Assert.Fail((isDir ? "mkdir" : "create file") + " should be disallowed"
					);
			}
			catch (Exception)
			{
			}
		}

		// ignored
		/// <exception cref="System.IO.IOException"/>
		private void EnsureReservedFileNamesCannotBeLoaded(MiniDFSCluster cluster)
		{
			// Turn on reserved file name checking. Loading of edits should fail
			FSDirectory.CheckReservedFileNames = true;
			EnsureClusterRestartFails(cluster);
			// Turn off reserved file name checking and successfully load edits
			FSDirectory.CheckReservedFileNames = false;
			EnsureClusterRestartSucceeds(cluster);
			// Turn on reserved file name checking. Loading of fsimage should fail
			FSDirectory.CheckReservedFileNames = true;
			EnsureClusterRestartFails(cluster);
		}

		private void EnsureClusterRestartFails(MiniDFSCluster cluster)
		{
			try
			{
				cluster.RestartNameNode();
				NUnit.Framework.Assert.Fail("Cluster should not have successfully started");
			}
			catch (Exception expected)
			{
				Log.Info("Expected exception thrown " + expected);
			}
			NUnit.Framework.Assert.IsFalse(cluster.IsClusterUp());
		}

		/// <exception cref="System.IO.IOException"/>
		private void EnsureClusterRestartSucceeds(MiniDFSCluster cluster)
		{
			cluster.RestartNameNode();
			cluster.WaitActive();
			NUnit.Framework.Assert.IsTrue(cluster.IsClusterUp());
		}

		/// <summary>For a given path, build a tree of INodes and return the leaf node.</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		private INode CreateTreeOfInodes(string path)
		{
			byte[][] components = INode.GetPathComponents(path);
			FsPermission perm = FsPermission.CreateImmutable((short)0x1ed);
			PermissionStatus permstatus = PermissionStatus.CreateImmutable(string.Empty, string.Empty
				, perm);
			long id = 0;
			INodeDirectory prev = new INodeDirectory(++id, new byte[0], permstatus, 0);
			INodeDirectory dir = null;
			foreach (byte[] component in components)
			{
				if (component.Length == 0)
				{
					continue;
				}
				System.Console.Out.WriteLine("Adding component " + DFSUtil.Bytes2String(component
					));
				dir = new INodeDirectory(++id, component, permstatus, 0);
				prev.AddChild(dir, false, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
				prev = dir;
			}
			return dir;
		}

		// Last Inode in the chain
		/// <summary>
		/// Test for
		/// <see cref="FSDirectory.GetPathComponents(INode)"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPathFromInode()
		{
			string path = "/a/b/c";
			INode inode = CreateTreeOfInodes(path);
			byte[][] expected = INode.GetPathComponents(path);
			byte[][] actual = FSDirectory.GetPathComponents(inode);
			DFSTestUtil.CheckComponentsEquals(expected, actual);
		}

		/// <summary>
		/// Tests for
		/// <see cref="FSDirectory.ResolvePath(string, byte[][], FSDirectory)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInodePath()
		{
			// For a non .inodes path the regular components are returned
			string path = "/a/b/c";
			INode inode = CreateTreeOfInodes(path);
			// For an any inode look up return inode corresponding to "c" from /a/b/c
			FSDirectory fsd = Org.Mockito.Mockito.Mock<FSDirectory>();
			Org.Mockito.Mockito.DoReturn(inode).When(fsd).GetInode(Org.Mockito.Mockito.AnyLong
				());
			// Null components
			NUnit.Framework.Assert.AreEqual("/test", FSDirectory.ResolvePath("/test", null, fsd
				));
			// Tests for FSDirectory#resolvePath()
			// Non inode regular path
			byte[][] components = INode.GetPathComponents(path);
			string resolvedPath = FSDirectory.ResolvePath(path, components, fsd);
			NUnit.Framework.Assert.AreEqual(path, resolvedPath);
			// Inode path with no trailing separator
			components = INode.GetPathComponents("/.reserved/.inodes/1");
			resolvedPath = FSDirectory.ResolvePath(path, components, fsd);
			NUnit.Framework.Assert.AreEqual(path, resolvedPath);
			// Inode path with trailing separator
			components = INode.GetPathComponents("/.reserved/.inodes/1/");
			NUnit.Framework.Assert.AreEqual(path, resolvedPath);
			// Inode relative path
			components = INode.GetPathComponents("/.reserved/.inodes/1/d/e/f");
			resolvedPath = FSDirectory.ResolvePath(path, components, fsd);
			NUnit.Framework.Assert.AreEqual("/a/b/c/d/e/f", resolvedPath);
			// A path with just .inodes  returns the path as is
			string testPath = "/.reserved/.inodes";
			components = INode.GetPathComponents(testPath);
			resolvedPath = FSDirectory.ResolvePath(testPath, components, fsd);
			NUnit.Framework.Assert.AreEqual(testPath, resolvedPath);
			// Root inode path
			testPath = "/.reserved/.inodes/" + INodeId.RootInodeId;
			components = INode.GetPathComponents(testPath);
			resolvedPath = FSDirectory.ResolvePath(testPath, components, fsd);
			NUnit.Framework.Assert.AreEqual("/", resolvedPath);
			// An invalid inode path should remain unresolved
			testPath = "/.invalid/.inodes/1";
			components = INode.GetPathComponents(testPath);
			resolvedPath = FSDirectory.ResolvePath(testPath, components, fsd);
			NUnit.Framework.Assert.AreEqual(testPath, resolvedPath);
			// Test path with nonexistent(deleted or wrong id) inode
			Org.Mockito.Mockito.DoReturn(null).When(fsd).GetInode(Org.Mockito.Mockito.AnyLong
				());
			testPath = "/.reserved/.inodes/1234";
			components = INode.GetPathComponents(testPath);
			try
			{
				string realPath = FSDirectory.ResolvePath(testPath, components, fsd);
				NUnit.Framework.Assert.Fail("Path should not be resolved:" + realPath);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e is FileNotFoundException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static INodeDirectory GetDir(FSDirectory fsdir, Path dir)
		{
			string dirStr = dir.ToString();
			return INodeDirectory.ValueOf(fsdir.GetINode(dirStr), dirStr);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDotdotInodePath()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			DFSClient client = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				FSDirectory fsdir = cluster.GetNamesystem().GetFSDirectory();
				Path dir = new Path("/dir");
				hdfs.Mkdirs(dir);
				long dirId = fsdir.GetINode(dir.ToString()).GetId();
				long parentId = fsdir.GetINode("/").GetId();
				string testPath = "/.reserved/.inodes/" + dirId + "/..";
				client = new DFSClient(NameNode.GetAddress(conf), conf);
				HdfsFileStatus status = client.GetFileInfo(testPath);
				NUnit.Framework.Assert.IsTrue(parentId == status.GetFileId());
				// Test root's parent is still root
				testPath = "/.reserved/.inodes/" + parentId + "/..";
				status = client.GetFileInfo(testPath);
				NUnit.Framework.Assert.IsTrue(parentId == status.GetFileId());
			}
			finally
			{
				IOUtils.Cleanup(Log, client);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocationLimitInListingOps()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsListLimit, 9);
			// 3 blocks * 3 replicas
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				AList<string> source = new AList<string>();
				// tmp1 holds files with 3 blocks, 3 replicas
				// tmp2 holds files with 3 blocks, 1 replica
				hdfs.Mkdirs(new Path("/tmp1"));
				hdfs.Mkdirs(new Path("/tmp2"));
				source.AddItem("f1");
				source.AddItem("f2");
				int numEntries = source.Count;
				for (int j = 0; j < numEntries; j++)
				{
					DFSTestUtil.CreateFile(hdfs, new Path("/tmp1/" + source[j]), 4096, 3 * 1024 - 100
						, 1024, (short)3, 0);
				}
				byte[] start = HdfsFileStatus.EmptyName;
				for (int j_1 = 0; j_1 < numEntries; j_1++)
				{
					DirectoryListing dl = cluster.GetNameNodeRpc().GetListing("/tmp1", start, true);
					NUnit.Framework.Assert.IsTrue(dl.GetPartialListing().Length == 1);
					for (int i = 0; i < dl.GetPartialListing().Length; i++)
					{
						source.Remove(dl.GetPartialListing()[i].GetLocalName());
					}
					start = dl.GetLastName();
				}
				// Verify we have listed all entries in the directory.
				NUnit.Framework.Assert.IsTrue(source.Count == 0);
				// Now create 6 files, each with 3 locations. Should take 2 iterations of 3
				source.AddItem("f1");
				source.AddItem("f2");
				source.AddItem("f3");
				source.AddItem("f4");
				source.AddItem("f5");
				source.AddItem("f6");
				numEntries = source.Count;
				for (int j_2 = 0; j_2 < numEntries; j_2++)
				{
					DFSTestUtil.CreateFile(hdfs, new Path("/tmp2/" + source[j_2]), 4096, 3 * 1024 - 100
						, 1024, (short)1, 0);
				}
				start = HdfsFileStatus.EmptyName;
				for (int j_3 = 0; j_3 < numEntries / 3; j_3++)
				{
					DirectoryListing dl = cluster.GetNameNodeRpc().GetListing("/tmp2", start, true);
					NUnit.Framework.Assert.IsTrue(dl.GetPartialListing().Length == 3);
					for (int i = 0; i < dl.GetPartialListing().Length; i++)
					{
						source.Remove(dl.GetPartialListing()[i].GetLocalName());
					}
					start = dl.GetLastName();
				}
				// Verify we have listed all entries in tmp2.
				NUnit.Framework.Assert.IsTrue(source.Count == 0);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilesInGetListingOps()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				FSDirectory fsdir = cluster.GetNamesystem().GetFSDirectory();
				hdfs.Mkdirs(new Path("/tmp"));
				DFSTestUtil.CreateFile(hdfs, new Path("/tmp/f1"), 0, (short)1, 0);
				DFSTestUtil.CreateFile(hdfs, new Path("/tmp/f2"), 0, (short)1, 0);
				DFSTestUtil.CreateFile(hdfs, new Path("/tmp/f3"), 0, (short)1, 0);
				DirectoryListing dl = cluster.GetNameNodeRpc().GetListing("/tmp", HdfsFileStatus.
					EmptyName, false);
				NUnit.Framework.Assert.IsTrue(dl.GetPartialListing().Length == 3);
				string f2 = new string("f2");
				dl = cluster.GetNameNodeRpc().GetListing("/tmp", Sharpen.Runtime.GetBytesForString
					(f2), false);
				NUnit.Framework.Assert.IsTrue(dl.GetPartialListing().Length == 1);
				INode f2INode = fsdir.GetINode("/tmp/f2");
				string f2InodePath = "/.reserved/.inodes/" + f2INode.GetId();
				dl = cluster.GetNameNodeRpc().GetListing("/tmp", Sharpen.Runtime.GetBytesForString
					(f2InodePath), false);
				NUnit.Framework.Assert.IsTrue(dl.GetPartialListing().Length == 1);
				// Test the deleted startAfter file
				hdfs.Delete(new Path("/tmp/f2"), false);
				try
				{
					dl = cluster.GetNameNodeRpc().GetListing("/tmp", Sharpen.Runtime.GetBytesForString
						(f2InodePath), false);
					NUnit.Framework.Assert.Fail("Didn't get exception for the deleted startAfter token."
						);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e is DirectoryListingStartAfterNotFoundException);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestFileUnderConstruction()
		{
			replication = 3;
			INodeFile file = new INodeFile(INodeId.GrandfatherInodeId, null, perm, 0L, 0L, null
				, replication, 1024L, unchecked((byte)0));
			NUnit.Framework.Assert.IsFalse(file.IsUnderConstruction());
			string clientName = "client";
			string clientMachine = "machine";
			file.ToUnderConstruction(clientName, clientMachine);
			NUnit.Framework.Assert.IsTrue(file.IsUnderConstruction());
			FileUnderConstructionFeature uc = file.GetFileUnderConstructionFeature();
			NUnit.Framework.Assert.AreEqual(clientName, uc.GetClientName());
			NUnit.Framework.Assert.AreEqual(clientMachine, uc.GetClientMachine());
			file.ToCompleteFile(Time.Now());
			NUnit.Framework.Assert.IsFalse(file.IsUnderConstruction());
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrFeature()
		{
			replication = 3;
			preferredBlockSize = 128 * 1024 * 1024;
			INodeFile inf = CreateINodeFile(replication, preferredBlockSize);
			ImmutableList.Builder<XAttr> builder = new ImmutableList.Builder<XAttr>();
			XAttr xAttr = new XAttr.Builder().SetNameSpace(XAttr.NameSpace.User).SetName("a1"
				).SetValue(new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
				(int)(0x33)) }).Build();
			builder.Add(xAttr);
			XAttrFeature f = new XAttrFeature(((ImmutableList<XAttr>)builder.Build()));
			inf.AddXAttrFeature(f);
			XAttrFeature f1 = inf.GetXAttrFeature();
			NUnit.Framework.Assert.AreEqual(xAttr, f1.GetXAttrs()[0]);
			inf.RemoveXAttrFeature();
			f1 = inf.GetXAttrFeature();
			NUnit.Framework.Assert.AreEqual(f1, null);
		}
	}
}
