using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestHDFSConcat
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestHDFSConcat));

		private const short ReplFactor = 2;

		private MiniDFSCluster cluster;

		private NamenodeProtocols nn;

		private DistributedFileSystem dfs;

		private const long blockSize = 512;

		private static readonly Configuration conf;

		static TestHDFSConcat()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUpCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(ReplFactor).Build();
			NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
			cluster.WaitClusterUp();
			dfs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
			nn = cluster.GetNameNodeRpc();
			NUnit.Framework.Assert.IsNotNull("Failed to get NameNode", nn);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutDownCluster()
		{
			if (dfs != null)
			{
				dfs.Close();
			}
			if (cluster != null)
			{
				cluster.ShutdownDataNodes();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Concatenates 10 files into one
		/// Verifies the final size, deletion of the file, number of blocks
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcat()
		{
			int numFiles = 10;
			long fileLen = blockSize * 3;
			HdfsFileStatus fStatus;
			FSDataInputStream stm;
			string trg = "/trg";
			Path trgPath = new Path(trg);
			DFSTestUtil.CreateFile(dfs, trgPath, fileLen, ReplFactor, 1);
			fStatus = nn.GetFileInfo(trg);
			long trgLen = fStatus.GetLen();
			long trgBlocks = nn.GetBlockLocations(trg, 0, trgLen).LocatedBlockCount();
			Path[] files = new Path[numFiles];
			byte[][] bytes = new byte[][] { new byte[(int)fileLen], new byte[(int)fileLen], new 
				byte[(int)fileLen], new byte[(int)fileLen], new byte[(int)fileLen], new byte[(int
				)fileLen], new byte[(int)fileLen], new byte[(int)fileLen], new byte[(int)fileLen
				], new byte[(int)fileLen], new byte[(int)fileLen] };
			LocatedBlocks[] lblocks = new LocatedBlocks[numFiles];
			long[] lens = new long[numFiles];
			stm = dfs.Open(trgPath);
			stm.ReadFully(0, bytes[0]);
			stm.Close();
			int i;
			for (i = 0; i < files.Length; i++)
			{
				files[i] = new Path("/file" + i);
				Path path = files[i];
				System.Console.Out.WriteLine("Creating file " + path);
				// make files with different content
				DFSTestUtil.CreateFile(dfs, path, fileLen, ReplFactor, i);
				fStatus = nn.GetFileInfo(path.ToUri().GetPath());
				lens[i] = fStatus.GetLen();
				NUnit.Framework.Assert.AreEqual(trgLen, lens[i]);
				// file of the same length.
				lblocks[i] = nn.GetBlockLocations(path.ToUri().GetPath(), 0, lens[i]);
				//read the file
				stm = dfs.Open(path);
				stm.ReadFully(0, bytes[i + 1]);
				//bytes[i][10] = 10;
				stm.Close();
			}
			// check permissions -try the operation with the "wrong" user
			UserGroupInformation user1 = UserGroupInformation.CreateUserForTesting("theDoctor"
				, new string[] { "tardis" });
			DistributedFileSystem hdfs = (DistributedFileSystem)DFSTestUtil.GetFileSystemAs(user1
				, conf);
			try
			{
				hdfs.Concat(trgPath, files);
				NUnit.Framework.Assert.Fail("Permission exception expected");
			}
			catch (IOException ie)
			{
				System.Console.Out.WriteLine("Got expected exception for permissions:" + ie.GetLocalizedMessage
					());
			}
			// expected
			// check count update
			ContentSummary cBefore = dfs.GetContentSummary(trgPath.GetParent());
			// resort file array, make INode id not sorted.
			for (int j = 0; j < files.Length / 2; j++)
			{
				Path tempPath = files[j];
				files[j] = files[files.Length - 1 - j];
				files[files.Length - 1 - j] = tempPath;
				byte[] tempBytes = bytes[1 + j];
				bytes[1 + j] = bytes[files.Length - 1 - j + 1];
				bytes[files.Length - 1 - j + 1] = tempBytes;
			}
			// now concatenate
			dfs.Concat(trgPath, files);
			// verify  count
			ContentSummary cAfter = dfs.GetContentSummary(trgPath.GetParent());
			NUnit.Framework.Assert.AreEqual(cBefore.GetFileCount(), cAfter.GetFileCount() + files
				.Length);
			// verify other stuff
			long totalLen = trgLen;
			long totalBlocks = trgBlocks;
			for (i = 0; i < files.Length; i++)
			{
				totalLen += lens[i];
				totalBlocks += lblocks[i].LocatedBlockCount();
			}
			System.Console.Out.WriteLine("total len=" + totalLen + "; totalBlocks=" + totalBlocks
				);
			fStatus = nn.GetFileInfo(trg);
			trgLen = fStatus.GetLen();
			// new length
			// read the resulting file
			stm = dfs.Open(trgPath);
			byte[] byteFileConcat = new byte[(int)trgLen];
			stm.ReadFully(0, byteFileConcat);
			stm.Close();
			trgBlocks = nn.GetBlockLocations(trg, 0, trgLen).LocatedBlockCount();
			//verifications
			// 1. number of blocks
			NUnit.Framework.Assert.AreEqual(trgBlocks, totalBlocks);
			// 2. file lengths
			NUnit.Framework.Assert.AreEqual(trgLen, totalLen);
			// 3. removal of the src file
			foreach (Path p in files)
			{
				fStatus = nn.GetFileInfo(p.ToUri().GetPath());
				NUnit.Framework.Assert.IsNull("File " + p + " still exists", fStatus);
				// file shouldn't exist
				// try to create fie with the same name
				DFSTestUtil.CreateFile(dfs, p, fileLen, ReplFactor, 1);
			}
			// 4. content
			CheckFileContent(byteFileConcat, bytes);
			// add a small file (less then a block)
			Path smallFile = new Path("/sfile");
			int sFileLen = 10;
			DFSTestUtil.CreateFile(dfs, smallFile, sFileLen, ReplFactor, 1);
			dfs.Concat(trgPath, new Path[] { smallFile });
			fStatus = nn.GetFileInfo(trg);
			trgLen = fStatus.GetLen();
			// new length
			// check number of blocks
			trgBlocks = nn.GetBlockLocations(trg, 0, trgLen).LocatedBlockCount();
			NUnit.Framework.Assert.AreEqual(trgBlocks, totalBlocks + 1);
			// and length
			NUnit.Framework.Assert.AreEqual(trgLen, totalLen + sFileLen);
		}

		/// <summary>
		/// Test that the concat operation is properly persisted in the
		/// edit log, and properly replayed on restart.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcatInEditLog()
		{
			Path TestDir = new Path("/testConcatInEditLog");
			long FileLen = blockSize;
			// 1. Concat some files
			Path[] srcFiles = new Path[3];
			for (int i = 0; i < srcFiles.Length; i++)
			{
				Path path = new Path(TestDir, "src-" + i);
				DFSTestUtil.CreateFile(dfs, path, FileLen, ReplFactor, 1);
				srcFiles[i] = path;
			}
			Path targetFile = new Path(TestDir, "target");
			DFSTestUtil.CreateFile(dfs, targetFile, FileLen, ReplFactor, 1);
			dfs.Concat(targetFile, srcFiles);
			// 2. Verify the concat operation basically worked, and record
			// file status.
			NUnit.Framework.Assert.IsTrue(dfs.Exists(targetFile));
			FileStatus origStatus = dfs.GetFileStatus(targetFile);
			// 3. Restart NN to force replay from edit log
			cluster.RestartNameNode(true);
			// 4. Verify concat operation was replayed correctly and file status
			// did not change.
			NUnit.Framework.Assert.IsTrue(dfs.Exists(targetFile));
			NUnit.Framework.Assert.IsFalse(dfs.Exists(srcFiles[0]));
			FileStatus statusAfterRestart = dfs.GetFileStatus(targetFile);
			NUnit.Framework.Assert.AreEqual(origStatus.GetModificationTime(), statusAfterRestart
				.GetModificationTime());
		}

		// compare content
		private void CheckFileContent(byte[] concat, byte[][] bytes)
		{
			int idx = 0;
			bool mismatch = false;
			foreach (byte[] bb in bytes)
			{
				foreach (byte b in bb)
				{
					if (b != concat[idx++])
					{
						mismatch = true;
						break;
					}
				}
				if (mismatch)
				{
					break;
				}
			}
			NUnit.Framework.Assert.IsFalse("File content of concatenated file is different", 
				mismatch);
		}

		// test case when final block is not of a full length
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcatNotCompleteBlock()
		{
			long trgFileLen = blockSize * 3;
			long srcFileLen = blockSize * 3 + 20;
			// block at the end - not full
			// create first file
			string name1 = "/trg";
			string name2 = "/src";
			Path filePath1 = new Path(name1);
			DFSTestUtil.CreateFile(dfs, filePath1, trgFileLen, ReplFactor, 1);
			HdfsFileStatus fStatus = nn.GetFileInfo(name1);
			long fileLen = fStatus.GetLen();
			NUnit.Framework.Assert.AreEqual(fileLen, trgFileLen);
			//read the file
			FSDataInputStream stm = dfs.Open(filePath1);
			byte[] byteFile1 = new byte[(int)trgFileLen];
			stm.ReadFully(0, byteFile1);
			stm.Close();
			LocatedBlocks lb1 = nn.GetBlockLocations(name1, 0, trgFileLen);
			Path filePath2 = new Path(name2);
			DFSTestUtil.CreateFile(dfs, filePath2, srcFileLen, ReplFactor, 1);
			fStatus = nn.GetFileInfo(name2);
			fileLen = fStatus.GetLen();
			NUnit.Framework.Assert.AreEqual(srcFileLen, fileLen);
			// read the file
			stm = dfs.Open(filePath2);
			byte[] byteFile2 = new byte[(int)srcFileLen];
			stm.ReadFully(0, byteFile2);
			stm.Close();
			LocatedBlocks lb2 = nn.GetBlockLocations(name2, 0, srcFileLen);
			System.Console.Out.WriteLine("trg len=" + trgFileLen + "; src len=" + srcFileLen);
			// move the blocks
			dfs.Concat(filePath1, new Path[] { filePath2 });
			long totalLen = trgFileLen + srcFileLen;
			fStatus = nn.GetFileInfo(name1);
			fileLen = fStatus.GetLen();
			// read the resulting file
			stm = dfs.Open(filePath1);
			byte[] byteFileConcat = new byte[(int)fileLen];
			stm.ReadFully(0, byteFileConcat);
			stm.Close();
			LocatedBlocks lbConcat = nn.GetBlockLocations(name1, 0, fileLen);
			//verifications
			// 1. number of blocks
			NUnit.Framework.Assert.AreEqual(lbConcat.LocatedBlockCount(), lb1.LocatedBlockCount
				() + lb2.LocatedBlockCount());
			// 2. file lengths
			System.Console.Out.WriteLine("file1 len=" + fileLen + "; total len=" + totalLen);
			NUnit.Framework.Assert.AreEqual(fileLen, totalLen);
			// 3. removal of the src file
			fStatus = nn.GetFileInfo(name2);
			NUnit.Framework.Assert.IsNull("File " + name2 + "still exists", fStatus);
			// file shouldn't exist
			// 4. content
			CheckFileContent(byteFileConcat, new byte[][] { byteFile1, byteFile2 });
		}

		/// <summary>test illegal args cases</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIllegalArg()
		{
			long fileLen = blockSize * 3;
			Path parentDir = new Path("/parentTrg");
			NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(parentDir));
			Path trg = new Path(parentDir, "trg");
			DFSTestUtil.CreateFile(dfs, trg, fileLen, ReplFactor, 1);
			{
				// must be in the same dir
				// create first file
				Path dir1 = new Path("/dir1");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(dir1));
				Path src = new Path(dir1, "src");
				DFSTestUtil.CreateFile(dfs, src, fileLen, ReplFactor, 1);
				try
				{
					dfs.Concat(trg, new Path[] { src });
					NUnit.Framework.Assert.Fail("didn't fail for src and trg in different directories"
						);
				}
				catch (Exception)
				{
				}
			}
			// expected
			// non existing file
			try
			{
				dfs.Concat(trg, new Path[] { new Path("test1/a") });
				// non existing file
				NUnit.Framework.Assert.Fail("didn't fail with invalid arguments");
			}
			catch (Exception)
			{
			}
			//expected
			// empty arg list
			try
			{
				dfs.Concat(trg, new Path[] {  });
				// empty array
				NUnit.Framework.Assert.Fail("didn't fail with invalid arguments");
			}
			catch (Exception)
			{
			}
			{
				// exspected
				// the source file's preferred block size cannot be greater than the target
				Path src1 = new Path(parentDir, "src1");
				DFSTestUtil.CreateFile(dfs, src1, fileLen, ReplFactor, 0L);
				Path src2 = new Path(parentDir, "src2");
				// create a file whose preferred block size is greater than the target
				DFSTestUtil.CreateFile(dfs, src2, 1024, fileLen, dfs.GetDefaultBlockSize(trg) * 2
					, ReplFactor, 0L);
				try
				{
					dfs.Concat(trg, new Path[] { src1, src2 });
					NUnit.Framework.Assert.Fail("didn't fail for src with greater preferred block size"
						);
				}
				catch (Exception e)
				{
					GenericTestUtils.AssertExceptionContains("preferred block size", e);
				}
			}
		}

		/// <summary>make sure we update the quota correctly after concat</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcatWithQuotaDecrease()
		{
			short srcRepl = 3;
			// note this is different with REPL_FACTOR
			int srcNum = 10;
			Path foo = new Path("/foo");
			Path[] srcs = new Path[srcNum];
			Path target = new Path(foo, "target");
			DFSTestUtil.CreateFile(dfs, target, blockSize, ReplFactor, 0L);
			dfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			for (int i = 0; i < srcNum; i++)
			{
				srcs[i] = new Path(foo, "src" + i);
				DFSTestUtil.CreateFile(dfs, srcs[i], blockSize * 2, srcRepl, 0L);
			}
			ContentSummary summary = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(11, summary.GetFileCount());
			NUnit.Framework.Assert.AreEqual(blockSize * ReplFactor + blockSize * 2 * srcRepl 
				* srcNum, summary.GetSpaceConsumed());
			dfs.Concat(target, srcs);
			summary = dfs.GetContentSummary(foo);
			NUnit.Framework.Assert.AreEqual(1, summary.GetFileCount());
			NUnit.Framework.Assert.AreEqual(blockSize * ReplFactor + blockSize * 2 * ReplFactor
				 * srcNum, summary.GetSpaceConsumed());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcatWithQuotaIncrease()
		{
			short repl = 3;
			int srcNum = 10;
			Path foo = new Path("/foo");
			Path bar = new Path(foo, "bar");
			Path[] srcs = new Path[srcNum];
			Path target = new Path(bar, "target");
			DFSTestUtil.CreateFile(dfs, target, blockSize, repl, 0L);
			long dsQuota = blockSize * repl + blockSize * srcNum * ReplFactor;
			dfs.SetQuota(foo, long.MaxValue - 1, dsQuota);
			for (int i = 0; i < srcNum; i++)
			{
				srcs[i] = new Path(bar, "src" + i);
				DFSTestUtil.CreateFile(dfs, srcs[i], blockSize, ReplFactor, 0L);
			}
			ContentSummary summary = dfs.GetContentSummary(bar);
			NUnit.Framework.Assert.AreEqual(11, summary.GetFileCount());
			NUnit.Framework.Assert.AreEqual(dsQuota, summary.GetSpaceConsumed());
			try
			{
				dfs.Concat(target, srcs);
				NUnit.Framework.Assert.Fail("QuotaExceededException expected");
			}
			catch (RemoteException e)
			{
				NUnit.Framework.Assert.IsTrue(e.UnwrapRemoteException() is QuotaExceededException
					);
			}
			dfs.SetQuota(foo, long.MaxValue - 1, long.MaxValue - 1);
			dfs.Concat(target, srcs);
			summary = dfs.GetContentSummary(bar);
			NUnit.Framework.Assert.AreEqual(1, summary.GetFileCount());
			NUnit.Framework.Assert.AreEqual(blockSize * repl * (srcNum + 1), summary.GetSpaceConsumed
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConcatRelativeTargetPath()
		{
			Path dir = new Path("/dir");
			Path trg = new Path("trg");
			Path src = new Path(dir, "src");
			dfs.SetWorkingDirectory(dir);
			DFSTestUtil.CreateFile(dfs, trg, blockSize, ReplFactor, 1);
			DFSTestUtil.CreateFile(dfs, src, blockSize, ReplFactor, 1);
			dfs.Concat(trg, new Path[] { src });
			NUnit.Framework.Assert.AreEqual(blockSize * 2, dfs.GetFileStatus(trg).GetLen());
			NUnit.Framework.Assert.IsFalse(dfs.Exists(src));
		}
	}
}
