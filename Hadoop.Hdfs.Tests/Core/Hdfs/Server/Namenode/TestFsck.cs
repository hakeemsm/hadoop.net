using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A JUnit test for doing fsck</summary>
	public class TestFsck
	{
		internal static readonly string auditLogFile = Runtime.GetProperty("test.build.dir"
			, "build/test") + "/TestFsck-audit.log";

		internal static readonly Sharpen.Pattern fsckPattern = Sharpen.Pattern.Compile("allowed=.*?\\s"
			 + "ugi=.*?\\s" + "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + "cmd=fsck\\ssrc=\\/\\sdst=null\\s"
			 + "perm=null\\s" + "proto=.*");

		internal static readonly Sharpen.Pattern getfileinfoPattern = Sharpen.Pattern.Compile
			("allowed=.*?\\s" + "ugi=.*?\\s" + "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s"
			 + "cmd=getfileinfo\\ssrc=\\/\\sdst=null\\s" + "perm=null\\s" + "proto=.*");

		internal static readonly Sharpen.Pattern numCorruptBlocksPattern = Sharpen.Pattern
			.Compile(".*Corrupt blocks:\t\t([0123456789]*).*");

		private static readonly string LineSeparator = Runtime.GetProperty("line.separator"
			);

		// Pattern for: 
		// allowed=true ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
		/// <exception cref="System.Exception"/>
		internal static string RunFsck(Configuration conf, int expectedErrCode, bool checkErrorCode
			, params string[] path)
		{
			ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			TextWriter @out = new TextWriter(bStream, true);
			((Log4JLogger)FSPermissionChecker.Log).GetLogger().SetLevel(Level.All);
			int errCode = ToolRunner.Run(new DFSck(conf, @out), path);
			if (checkErrorCode)
			{
				NUnit.Framework.Assert.AreEqual(expectedErrCode, errCode);
			}
			((Log4JLogger)FSPermissionChecker.Log).GetLogger().SetLevel(Level.Info);
			FSImage.Log.Error("OUTPUT = " + bStream.ToString());
			return bStream.ToString();
		}

		/// <summary>do fsck</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsck()
		{
			DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestFsck").SetNumFiles(20).
				Build();
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				long precision = 1L;
				conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, precision);
				conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				fs = cluster.GetFileSystem();
				string fileName = "/srcdat";
				util.CreateFiles(fs, fileName);
				util.WaitReplication(fs, fileName, (short)3);
				Path file = new Path(fileName);
				long aTime = fs.GetFileStatus(file).GetAccessTime();
				Sharpen.Thread.Sleep(precision);
				SetupAuditLogs();
				string outStr = RunFsck(conf, 0, true, "/");
				VerifyAuditLogs();
				NUnit.Framework.Assert.AreEqual(aTime, fs.GetFileStatus(file).GetAccessTime());
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				cluster.Shutdown();
				// restart the cluster; bring up namenode but not the data nodes
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).Build();
				outStr = RunFsck(conf, 1, true, "/");
				// expect the result is corrupt
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
				System.Console.Out.WriteLine(outStr);
				// bring up data nodes & cleanup cluster
				cluster.StartDataNodes(conf, 4, true, null, null);
				cluster.WaitActive();
				cluster.WaitClusterUp();
				fs = cluster.GetFileSystem();
				util.Cleanup(fs, "/srcdat");
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Sets up log4j logger for auditlogs</summary>
		/// <exception cref="System.IO.IOException"/>
		private void SetupAuditLogs()
		{
			FilePath file = new FilePath(auditLogFile);
			if (file.Exists())
			{
				file.Delete();
			}
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			logger.SetLevel(Level.Info);
			PatternLayout layout = new PatternLayout("%m%n");
			RollingFileAppender appender = new RollingFileAppender(layout, auditLogFile);
			logger.AddAppender(appender);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyAuditLogs()
		{
			// Turn off the logs
			Logger logger = ((Log4JLogger)FSNamesystem.auditLog).GetLogger();
			logger.SetLevel(Level.Off);
			BufferedReader reader = null;
			try
			{
				// Audit log should contain one getfileinfo and one fsck
				reader = new BufferedReader(new FileReader(auditLogFile));
				string line;
				// one extra getfileinfo stems from resolving the path
				//
				for (int i = 0; i < 2; i++)
				{
					line = reader.ReadLine();
					NUnit.Framework.Assert.IsNotNull(line);
					NUnit.Framework.Assert.IsTrue("Expected getfileinfo event not found in audit log"
						, getfileinfoPattern.Matcher(line).Matches());
				}
				line = reader.ReadLine();
				NUnit.Framework.Assert.IsNotNull(line);
				NUnit.Framework.Assert.IsTrue("Expected fsck event not found in audit log", fsckPattern
					.Matcher(line).Matches());
				NUnit.Framework.Assert.IsNull("Unexpected event in audit log", reader.ReadLine());
			}
			finally
			{
				// Close the reader and remove the appender to release the audit log file
				// handle after verifying the content of the file.
				if (reader != null)
				{
					reader.Close();
				}
				if (logger != null)
				{
					logger.RemoveAllAppenders();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckNonExistent()
		{
			DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestFsck").SetNumFiles(20).
				Build();
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				fs = cluster.GetFileSystem();
				util.CreateFiles(fs, "/srcdat");
				util.WaitReplication(fs, "/srcdat", (short)3);
				string outStr = RunFsck(conf, 0, true, "/non-existent");
				NUnit.Framework.Assert.AreEqual(-1, outStr.IndexOf(NamenodeFsck.HealthyStatus));
				System.Console.Out.WriteLine(outStr);
				util.Cleanup(fs, "/srcdat");
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test fsck with permission set on inodes</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckPermission()
		{
			DFSTestUtil util = new DFSTestUtil.Builder().SetName(GetType().Name).SetNumFiles(
				20).Build();
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
			MiniDFSCluster cluster = null;
			try
			{
				// Create a cluster with the current user, write some files
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				MiniDFSCluster c2 = cluster;
				string dir = "/dfsck";
				Path dirpath = new Path(dir);
				FileSystem fs = c2.GetFileSystem();
				util.CreateFiles(fs, dir);
				util.WaitReplication(fs, dir, (short)3);
				fs.SetPermission(dirpath, new FsPermission((short)0x1c0));
				// run DFSck as another user, should fail with permission issue
				UserGroupInformation fakeUGI = UserGroupInformation.CreateUserForTesting("ProbablyNotARealUserName"
					, new string[] { "ShangriLa" });
				fakeUGI.DoAs(new _PrivilegedExceptionAction_289(conf, dir));
				// set permission and try DFSck again as the fake user, should succeed
				fs.SetPermission(dirpath, new FsPermission((short)0x1ff));
				fakeUGI.DoAs(new _PrivilegedExceptionAction_299(conf, dir));
				util.Cleanup(fs, dir);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_289 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_289(Configuration conf, string dir)
			{
				this.conf = conf;
				this.dir = dir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				System.Console.Out.WriteLine(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestFsck.RunFsck
					(conf, -1, true, dir));
				return null;
			}

			private readonly Configuration conf;

			private readonly string dir;
		}

		private sealed class _PrivilegedExceptionAction_299 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_299(Configuration conf, string dir)
			{
				this.conf = conf;
				this.dir = dir;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				string outStr = Org.Apache.Hadoop.Hdfs.Server.Namenode.TestFsck.RunFsck(conf, 0, 
					true, dir);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				return null;
			}

			private readonly Configuration conf;

			private readonly string dir;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckMove()
		{
			Configuration conf = new HdfsConfiguration();
			int DfsBlockSize = 1024;
			int NumDatanodes = 4;
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DfsBlockSize);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
			conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
			DFSTestUtil util = new DFSTestUtil("TestFsck", 5, 3, (5 * DfsBlockSize) + (DfsBlockSize
				 - 1), 5 * DfsBlockSize);
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).Build();
				string topDir = "/srcdat";
				fs = cluster.GetFileSystem();
				cluster.WaitActive();
				util.CreateFiles(fs, topDir);
				util.WaitReplication(fs, topDir, (short)3);
				string outStr = RunFsck(conf, 0, true, "/");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				DFSClient dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
					()), conf);
				string[] fileNames = util.GetFileNames(topDir);
				TestFsck.CorruptedTestFile[] ctFiles = new TestFsck.CorruptedTestFile[] { new TestFsck.CorruptedTestFile
					(fileNames[0], Sets.NewHashSet(0), dfsClient, NumDatanodes, DfsBlockSize), new TestFsck.CorruptedTestFile
					(fileNames[1], Sets.NewHashSet(2, 3), dfsClient, NumDatanodes, DfsBlockSize), new 
					TestFsck.CorruptedTestFile(fileNames[2], Sets.NewHashSet(4), dfsClient, NumDatanodes
					, DfsBlockSize), new TestFsck.CorruptedTestFile(fileNames[3], Sets.NewHashSet(0, 
					1, 2, 3), dfsClient, NumDatanodes, DfsBlockSize), new TestFsck.CorruptedTestFile
					(fileNames[4], Sets.NewHashSet(1, 2, 3, 4), dfsClient, NumDatanodes, DfsBlockSize
					) };
				int totalMissingBlocks = 0;
				foreach (TestFsck.CorruptedTestFile ctFile in ctFiles)
				{
					totalMissingBlocks += ctFile.GetTotalMissingBlocks();
				}
				foreach (TestFsck.CorruptedTestFile ctFile_1 in ctFiles)
				{
					ctFile_1.RemoveBlocks(cluster);
				}
				// Wait for fsck to discover all the missing blocks
				while (true)
				{
					outStr = RunFsck(conf, 1, false, "/");
					string numCorrupt = null;
					foreach (string line in outStr.Split(LineSeparator))
					{
						Matcher m = numCorruptBlocksPattern.Matcher(line);
						if (m.Matches())
						{
							numCorrupt = m.Group(1);
							break;
						}
					}
					if (numCorrupt == null)
					{
						throw new IOException("failed to find number of corrupt " + "blocks in fsck output."
							);
					}
					if (numCorrupt.Equals(Sharpen.Extensions.ToString(totalMissingBlocks)))
					{
						NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
						break;
					}
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
				}
				// Copy the non-corrupt blocks of corruptFileName to lost+found.
				outStr = RunFsck(conf, 1, false, "/", "-move");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
				// Make sure that we properly copied the block files from the DataNodes
				// to lost+found
				foreach (TestFsck.CorruptedTestFile ctFile_2 in ctFiles)
				{
					ctFile_2.CheckSalvagedRemains();
				}
				// Fix the filesystem by removing corruptFileName
				outStr = RunFsck(conf, 1, true, "/", "-delete");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
				// Check to make sure we have a healthy filesystem
				outStr = RunFsck(conf, 0, true, "/");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				util.Cleanup(fs, topDir);
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private class CorruptedTestFile
		{
			private readonly string name;

			private readonly ICollection<int> blocksToCorrupt;

			private readonly DFSClient dfsClient;

			private readonly int numDataNodes;

			private readonly int blockSize;

			private readonly byte[] initialContents;

			/// <exception cref="System.IO.IOException"/>
			public CorruptedTestFile(string name, ICollection<int> blocksToCorrupt, DFSClient
				 dfsClient, int numDataNodes, int blockSize)
			{
				this.name = name;
				this.blocksToCorrupt = blocksToCorrupt;
				this.dfsClient = dfsClient;
				this.numDataNodes = numDataNodes;
				this.blockSize = blockSize;
				this.initialContents = CacheInitialContents();
			}

			public virtual int GetTotalMissingBlocks()
			{
				return blocksToCorrupt.Count;
			}

			/// <exception cref="System.IO.IOException"/>
			private byte[] CacheInitialContents()
			{
				HdfsFileStatus status = dfsClient.GetFileInfo(name);
				byte[] content = new byte[(int)status.GetLen()];
				DFSInputStream @in = null;
				try
				{
					@in = dfsClient.Open(name);
					IOUtils.ReadFully(@in, content, 0, content.Length);
				}
				finally
				{
					@in.Close();
				}
				return content;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void RemoveBlocks(MiniDFSCluster cluster)
			{
				foreach (int corruptIdx in blocksToCorrupt)
				{
					// Corrupt a block by deleting it
					ExtendedBlock block = dfsClient.GetNamenode().GetBlockLocations(name, blockSize *
						 corruptIdx, long.MaxValue).Get(0).GetBlock();
					for (int i = 0; i < numDataNodes; i++)
					{
						FilePath blockFile = cluster.GetBlockFile(i, block);
						if (blockFile != null && blockFile.Exists())
						{
							NUnit.Framework.Assert.IsTrue(blockFile.Delete());
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CheckSalvagedRemains()
			{
				int chainIdx = 0;
				HdfsFileStatus status = dfsClient.GetFileInfo(name);
				long length = status.GetLen();
				int numBlocks = (int)((length + blockSize - 1) / blockSize);
				DFSInputStream @in = null;
				byte[] blockBuffer = new byte[blockSize];
				try
				{
					for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++)
					{
						if (blocksToCorrupt.Contains(blockIdx))
						{
							if (@in != null)
							{
								@in.Close();
								@in = null;
							}
							continue;
						}
						if (@in == null)
						{
							@in = dfsClient.Open("/lost+found" + name + "/" + chainIdx);
							chainIdx++;
						}
						int len = blockBuffer.Length;
						if (blockIdx == (numBlocks - 1))
						{
							// The last block might not be full-length
							len = (int)(@in.GetFileLength() % blockSize);
							if (len == 0)
							{
								len = blockBuffer.Length;
							}
						}
						IOUtils.ReadFully(@in, blockBuffer, 0, len);
						int startIdx = blockIdx * blockSize;
						for (int i = 0; i < len; i++)
						{
							if (initialContents[startIdx + i] != blockBuffer[i])
							{
								throw new IOException("salvaged file " + name + " differed " + "from what we expected on block "
									 + blockIdx);
							}
						}
					}
				}
				finally
				{
					IOUtils.Cleanup(null, @in);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckMoveAndDelete()
		{
			int MaxMoveTries = 5;
			DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestFsckMoveAndDelete").SetNumFiles
				(5).Build();
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
				conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				string topDir = "/srcdat";
				fs = cluster.GetFileSystem();
				cluster.WaitActive();
				util.CreateFiles(fs, topDir);
				util.WaitReplication(fs, topDir, (short)3);
				string outStr = RunFsck(conf, 0, true, "/");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				// Corrupt a block by deleting it
				string[] fileNames = util.GetFileNames(topDir);
				DFSClient dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort
					()), conf);
				string corruptFileName = fileNames[0];
				ExtendedBlock block = dfsClient.GetNamenode().GetBlockLocations(corruptFileName, 
					0, long.MaxValue).Get(0).GetBlock();
				for (int i = 0; i < 4; i++)
				{
					FilePath blockFile = cluster.GetBlockFile(i, block);
					if (blockFile != null && blockFile.Exists())
					{
						NUnit.Framework.Assert.IsTrue(blockFile.Delete());
					}
				}
				// We excpect the filesystem to be corrupted
				outStr = RunFsck(conf, 1, false, "/");
				while (!outStr.Contains(NamenodeFsck.CorruptStatus))
				{
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
					outStr = RunFsck(conf, 1, false, "/");
				}
				// After a fsck -move, the corrupted file should still exist.
				for (int i_1 = 0; i_1 < MaxMoveTries; i_1++)
				{
					outStr = RunFsck(conf, 1, true, "/", "-move");
					NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
					string[] newFileNames = util.GetFileNames(topDir);
					bool found = false;
					foreach (string f in newFileNames)
					{
						if (f.Equals(corruptFileName))
						{
							found = true;
							break;
						}
					}
					NUnit.Framework.Assert.IsTrue(found);
				}
				// Fix the filesystem by moving corrupted files to lost+found
				outStr = RunFsck(conf, 1, true, "/", "-move", "-delete");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
				// Check to make sure we have healthy filesystem
				outStr = RunFsck(conf, 0, true, "/");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				util.Cleanup(fs, topDir);
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				cluster.Shutdown();
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckOpenFiles()
		{
			DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestFsck").SetNumFiles(4).Build
				();
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				string topDir = "/srcdat";
				string randomString = "HADOOP  ";
				fs = cluster.GetFileSystem();
				cluster.WaitActive();
				util.CreateFiles(fs, topDir);
				util.WaitReplication(fs, topDir, (short)3);
				string outStr = RunFsck(conf, 0, true, "/");
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				// Open a file for writing and do not close for now
				Path openFile = new Path(topDir + "/openFile");
				FSDataOutputStream @out = fs.Create(openFile);
				int writeCount = 0;
				while (writeCount != 100)
				{
					@out.Write(Sharpen.Runtime.GetBytesForString(randomString));
					writeCount++;
				}
				// We expect the filesystem to be HEALTHY and show one open file
				outStr = RunFsck(conf, 0, true, topDir);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				NUnit.Framework.Assert.IsFalse(outStr.Contains("OPENFORWRITE"));
				// Use -openforwrite option to list open files
				outStr = RunFsck(conf, 0, true, topDir, "-openforwrite");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains("OPENFORWRITE"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("openFile"));
				// Close the file
				@out.Close();
				// Now, fsck should show HEALTHY fs and should not show any open files
				outStr = RunFsck(conf, 0, true, topDir);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				NUnit.Framework.Assert.IsFalse(outStr.Contains("OPENFORWRITE"));
				util.Cleanup(fs, topDir);
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				cluster.Shutdown();
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptBlock()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			FileSystem fs = null;
			DFSClient dfsClient = null;
			LocatedBlocks blocks = null;
			int replicaCount = 0;
			Random random = new Random();
			string outStr = null;
			short factor = 1;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path file1 = new Path("/testCorruptBlock");
				DFSTestUtil.CreateFile(fs, file1, 1024, factor, 0);
				// Wait until file replication has completed
				DFSTestUtil.WaitReplication(fs, file1, factor);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, file1);
				// Make sure filesystem is in healthy state
				outStr = RunFsck(conf, 0, true, "/");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				// corrupt replicas
				FilePath blockFile = cluster.GetBlockFile(0, block);
				if (blockFile != null && blockFile.Exists())
				{
					RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
					FileChannel channel = raFile.GetChannel();
					string badString = "BADBAD";
					int rand = random.Next((int)channel.Size() / 2);
					raFile.Seek(rand);
					raFile.Write(Sharpen.Runtime.GetBytesForString(badString));
					raFile.Close();
				}
				// Read the file to trigger reportBadBlocks
				try
				{
					IOUtils.CopyBytes(fs.Open(file1), new IOUtils.NullOutputStream(), conf, true);
				}
				catch (IOException)
				{
				}
				// Ignore exception
				dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), 
					conf);
				blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
					);
				replicaCount = blocks.Get(0).GetLocations().Length;
				while (replicaCount != factor)
				{
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception)
					{
					}
					blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
						);
					replicaCount = blocks.Get(0).GetLocations().Length;
				}
				NUnit.Framework.Assert.IsTrue(blocks.Get(0).IsCorrupt());
				// Check if fsck reports the same
				outStr = RunFsck(conf, 1, true, "/");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("testCorruptBlock"));
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
		public virtual void TestUnderMinReplicatedBlock()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			// Set minReplication to 2
			short minReplication = 2;
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey, minReplication);
			FileSystem fs = null;
			DFSClient dfsClient = null;
			LocatedBlocks blocks = null;
			int replicaCount = 0;
			Random random = new Random();
			string outStr = null;
			short factor = 1;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path file1 = new Path("/testUnderMinReplicatedBlock");
				DFSTestUtil.CreateFile(fs, file1, 1024, minReplication, 0);
				// Wait until file replication has completed
				DFSTestUtil.WaitReplication(fs, file1, minReplication);
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, file1);
				// Make sure filesystem is in healthy state
				outStr = RunFsck(conf, 0, true, "/");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				// corrupt the first replica
				FilePath blockFile = cluster.GetBlockFile(0, block);
				if (blockFile != null && blockFile.Exists())
				{
					RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
					FileChannel channel = raFile.GetChannel();
					string badString = "BADBAD";
					int rand = random.Next((int)channel.Size() / 2);
					raFile.Seek(rand);
					raFile.Write(Sharpen.Runtime.GetBytesForString(badString));
					raFile.Close();
				}
				dfsClient = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), 
					conf);
				blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
					);
				replicaCount = blocks.Get(0).GetLocations().Length;
				while (replicaCount != factor)
				{
					try
					{
						Sharpen.Thread.Sleep(100);
						// Read the file to trigger reportBadBlocks
						try
						{
							IOUtils.CopyBytes(fs.Open(file1), new IOUtils.NullOutputStream(), conf, true);
						}
						catch (IOException)
						{
						}
						// Ignore exception
						System.Console.Out.WriteLine("sleep in try: replicaCount=" + replicaCount + "  factor="
							 + factor);
					}
					catch (Exception)
					{
					}
					blocks = dfsClient.GetNamenode().GetBlockLocations(file1.ToString(), 0, long.MaxValue
						);
					replicaCount = blocks.Get(0).GetLocations().Length;
				}
				// Check if fsck reports the same
				outStr = RunFsck(conf, 0, true, "/");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("UNDER MIN REPL'D BLOCKS:\t1 (100.0 %)"
					));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("dfs.namenode.replication.min:\t2")
					);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test if fsck can return -1 in case of failure</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckError()
		{
			MiniDFSCluster cluster = null;
			try
			{
				// bring up a one-node cluster
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				string fileName = "/test.txt";
				Path filePath = new Path(fileName);
				FileSystem fs = cluster.GetFileSystem();
				// create a one-block file
				DFSTestUtil.CreateFile(fs, filePath, 1L, (short)1, 1L);
				DFSTestUtil.WaitReplication(fs, filePath, (short)1);
				// intentionally corrupt NN data structure
				INodeFile node = (INodeFile)cluster.GetNamesystem().dir.GetINode(fileName, true);
				BlockInfoContiguous[] blocks = node.GetBlocks();
				NUnit.Framework.Assert.AreEqual(blocks.Length, 1);
				blocks[0].SetNumBytes(-1L);
				// set the block length to be negative
				// run fsck and expect a failure with -1 as the error code
				string outStr = RunFsck(conf, -1, true, fileName);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.FailureStatus));
				// clean up file system
				fs.Delete(filePath, true);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>check if option -list-corruptfiles of fsck command works properly</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckListCorruptFilesBlocks()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, 1);
			FileSystem fs = null;
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("testGetCorruptFiles").SetNumFiles
					(3).SetMaxLevels(1).SetMaxSize(1024).Build();
				util.CreateFiles(fs, "/corruptData", (short)1);
				util.WaitReplication(fs, "/corruptData", (short)1);
				// String outStr = runFsck(conf, 0, true, "/corruptData", "-list-corruptfileblocks");
				string outStr = RunFsck(conf, 0, false, "/corruptData", "-list-corruptfileblocks"
					);
				System.Console.Out.WriteLine("1. good fsck out: " + outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains("has 0 CORRUPT files"));
				// delete the blocks
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				for (int i = 0; i < 4; i++)
				{
					for (int j = 0; j <= 1; j++)
					{
						FilePath storageDir = cluster.GetInstanceStorageDir(i, j);
						FilePath data_dir = MiniDFSCluster.GetFinalizedDir(storageDir, bpid);
						IList<FilePath> metadataFiles = MiniDFSCluster.GetAllBlockMetadataFiles(data_dir);
						if (metadataFiles == null)
						{
							continue;
						}
						foreach (FilePath metadataFile in metadataFiles)
						{
							FilePath blockFile = Block.MetaToBlockFile(metadataFile);
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", blockFile.Delete());
							NUnit.Framework.Assert.IsTrue("Cannot remove file.", metadataFile.Delete());
						}
					}
				}
				// wait for the namenode to see the corruption
				NamenodeProtocols namenode = cluster.GetNameNodeRpc();
				CorruptFileBlocks corruptFileBlocks = namenode.ListCorruptFileBlocks("/corruptData"
					, null);
				int numCorrupt = corruptFileBlocks.GetFiles().Length;
				while (numCorrupt == 0)
				{
					Sharpen.Thread.Sleep(1000);
					corruptFileBlocks = namenode.ListCorruptFileBlocks("/corruptData", null);
					numCorrupt = corruptFileBlocks.GetFiles().Length;
				}
				outStr = RunFsck(conf, -1, true, "/corruptData", "-list-corruptfileblocks");
				System.Console.Out.WriteLine("2. bad fsck out: " + outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains("has 3 CORRUPT files"));
				// Do a listing on a dir which doesn't have any corrupt blocks and validate
				util.CreateFiles(fs, "/goodData");
				outStr = RunFsck(conf, 0, true, "/goodData", "-list-corruptfileblocks");
				System.Console.Out.WriteLine("3. good fsck out: " + outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains("has 0 CORRUPT files"));
				util.Cleanup(fs, "/corruptData");
				util.Cleanup(fs, "/goodData");
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
		/// Test for checking fsck command on illegal arguments should print the proper
		/// usage.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToCheckTheFsckCommandOnIllegalArguments()
		{
			MiniDFSCluster cluster = null;
			try
			{
				// bring up a one-node cluster
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				string fileName = "/test.txt";
				Path filePath = new Path(fileName);
				FileSystem fs = cluster.GetFileSystem();
				// create a one-block file
				DFSTestUtil.CreateFile(fs, filePath, 1L, (short)1, 1L);
				DFSTestUtil.WaitReplication(fs, filePath, (short)1);
				// passing illegal option
				string outStr = RunFsck(conf, -1, true, fileName, "-thisIsNotAValidFlag");
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(!outStr.Contains(NamenodeFsck.HealthyStatus));
				// passing multiple paths are arguments
				outStr = RunFsck(conf, -1, true, "/", fileName);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(!outStr.Contains(NamenodeFsck.HealthyStatus));
				// clean up file system
				fs.Delete(filePath, true);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Tests that the # of missing block replicas and expected replicas is correct
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckMissingReplicas()
		{
			// Desired replication factor
			// Set this higher than NUM_REPLICAS so it's under-replicated
			short ReplFactor = 2;
			// Number of replicas to actually start
			short NumReplicas = 1;
			// Number of blocks to write
			short NumBlocks = 3;
			// Set a small-ish blocksize
			long blockSize = 512;
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			MiniDFSCluster cluster = null;
			DistributedFileSystem dfs = null;
			try
			{
				// Startup a minicluster
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumReplicas).Build();
				NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
				cluster.WaitClusterUp();
				dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
				// Create a file that will be intentionally under-replicated
				string pathString = new string("/testfile");
				Path path = new Path(pathString);
				long fileLen = blockSize * NumBlocks;
				DFSTestUtil.CreateFile(dfs, path, fileLen, ReplFactor, 1);
				// Create an under-replicated file
				NameNode namenode = cluster.GetNameNode();
				NetworkTopology nettop = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetNetworkTopology();
				IDictionary<string, string[]> pmap = new Dictionary<string, string[]>();
				TextWriter result = new StringWriter();
				PrintWriter @out = new PrintWriter(result, true);
				IPAddress remoteAddress = Sharpen.Runtime.GetLocalHost();
				NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, @out, NumReplicas
					, remoteAddress);
				// Run the fsck and check the Result
				HdfsFileStatus file = namenode.GetRpcServer().GetFileInfo(pathString);
				NUnit.Framework.Assert.IsNotNull(file);
				NamenodeFsck.Result res = new NamenodeFsck.Result(conf);
				fsck.Check(pathString, file, res);
				// Also print the output from the fsck, for ex post facto sanity checks
				System.Console.Out.WriteLine(result.ToString());
				NUnit.Framework.Assert.AreEqual(res.missingReplicas, (NumBlocks * ReplFactor) - (
					NumBlocks * NumReplicas));
				NUnit.Framework.Assert.AreEqual(res.numExpectedReplicas, NumBlocks * ReplFactor);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Tests that the # of misreplaced replicas is correct</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckMisPlacedReplicas()
		{
			// Desired replication factor
			short ReplFactor = 2;
			// Number of replicas to actually start
			short NumDn = 2;
			// Number of blocks to write
			short NumBlocks = 3;
			// Set a small-ish blocksize
			long blockSize = 512;
			string[] racks = new string[] { "/rack1", "/rack1" };
			string[] hosts = new string[] { "host1", "host2" };
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			MiniDFSCluster cluster = null;
			DistributedFileSystem dfs = null;
			try
			{
				// Startup a minicluster
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDn).Hosts(hosts).Racks
					(racks).Build();
				NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
				cluster.WaitClusterUp();
				dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
				// Create a file that will be intentionally under-replicated
				string pathString = new string("/testfile");
				Path path = new Path(pathString);
				long fileLen = blockSize * NumBlocks;
				DFSTestUtil.CreateFile(dfs, path, fileLen, ReplFactor, 1);
				// Create an under-replicated file
				NameNode namenode = cluster.GetNameNode();
				NetworkTopology nettop = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetNetworkTopology();
				// Add a new node on different rack, so previous blocks' replicas 
				// are considered to be misplaced
				nettop.Add(DFSTestUtil.GetDatanodeDescriptor("/rack2", "/host3"));
				NumDn++;
				IDictionary<string, string[]> pmap = new Dictionary<string, string[]>();
				TextWriter result = new StringWriter();
				PrintWriter @out = new PrintWriter(result, true);
				IPAddress remoteAddress = Sharpen.Runtime.GetLocalHost();
				NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, @out, NumDn, remoteAddress
					);
				// Run the fsck and check the Result
				HdfsFileStatus file = namenode.GetRpcServer().GetFileInfo(pathString);
				NUnit.Framework.Assert.IsNotNull(file);
				NamenodeFsck.Result res = new NamenodeFsck.Result(conf);
				fsck.Check(pathString, file, res);
				// check misReplicatedBlock number.
				NUnit.Framework.Assert.AreEqual(res.numMisReplicatedBlocks, NumBlocks);
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test fsck with FileNotFound</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckFileNotFound()
		{
			// Number of replicas to actually start
			short NumReplicas = 1;
			Configuration conf = new Configuration();
			NameNode namenode = Org.Mockito.Mockito.Mock<NameNode>();
			NetworkTopology nettop = Org.Mockito.Mockito.Mock<NetworkTopology>();
			IDictionary<string, string[]> pmap = new Dictionary<string, string[]>();
			TextWriter result = new StringWriter();
			PrintWriter @out = new PrintWriter(result, true);
			IPAddress remoteAddress = Sharpen.Runtime.GetLocalHost();
			FSNamesystem fsName = Org.Mockito.Mockito.Mock<FSNamesystem>();
			BlockManager blockManager = Org.Mockito.Mockito.Mock<BlockManager>();
			DatanodeManager dnManager = Org.Mockito.Mockito.Mock<DatanodeManager>();
			Org.Mockito.Mockito.When(namenode.GetNamesystem()).ThenReturn(fsName);
			Org.Mockito.Mockito.When(fsName.GetBlockLocations(Matchers.Any<FSPermissionChecker
				>(), Matchers.AnyString(), Matchers.AnyLong(), Matchers.AnyLong(), Matchers.AnyBoolean
				(), Matchers.AnyBoolean())).ThenThrow(new FileNotFoundException());
			Org.Mockito.Mockito.When(fsName.GetBlockManager()).ThenReturn(blockManager);
			Org.Mockito.Mockito.When(blockManager.GetDatanodeManager()).ThenReturn(dnManager);
			NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, @out, NumReplicas
				, remoteAddress);
			string pathString = "/tmp/testFile";
			long length = 123L;
			bool isDir = false;
			int blockReplication = 1;
			long blockSize = 128 * 1024L;
			long modTime = 123123123L;
			long accessTime = 123123120L;
			FsPermission perms = FsPermission.GetDefault();
			string owner = "foo";
			string group = "bar";
			byte[] symlink = null;
			byte[] path = new byte[128];
			path = DFSUtil.String2Bytes(pathString);
			long fileId = 312321L;
			int numChildren = 1;
			byte storagePolicy = 0;
			HdfsFileStatus file = new HdfsFileStatus(length, isDir, blockReplication, blockSize
				, modTime, accessTime, perms, owner, group, symlink, path, fileId, numChildren, 
				null, storagePolicy);
			NamenodeFsck.Result res = new NamenodeFsck.Result(conf);
			try
			{
				fsck.Check(pathString, file, res);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Unexpected exception " + e.Message);
			}
			NUnit.Framework.Assert.IsTrue(res.ToString().Contains("HEALTHY"));
		}

		/// <summary>Test fsck with symlinks in the filesystem</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckSymlink()
		{
			DFSTestUtil util = new DFSTestUtil.Builder().SetName(GetType().Name).SetNumFiles(
				1).Build();
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				long precision = 1L;
				conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, precision);
				conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				fs = cluster.GetFileSystem();
				string fileName = "/srcdat";
				util.CreateFiles(fs, fileName);
				FileContext fc = FileContext.GetFileContext(cluster.GetConfiguration(0));
				Path file = new Path(fileName);
				Path symlink = new Path("/srcdat-symlink");
				fc.CreateSymlink(file, symlink, false);
				util.WaitReplication(fs, fileName, (short)3);
				long aTime = fc.GetFileStatus(symlink).GetAccessTime();
				Sharpen.Thread.Sleep(precision);
				SetupAuditLogs();
				string outStr = RunFsck(conf, 0, true, "/");
				VerifyAuditLogs();
				NUnit.Framework.Assert.AreEqual(aTime, fc.GetFileStatus(symlink).GetAccessTime());
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("Total symlinks:\t\t1"));
				util.Cleanup(fs, fileName);
			}
			finally
			{
				if (fs != null)
				{
					try
					{
						fs.Close();
					}
					catch (Exception)
					{
					}
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test for including the snapshot files in fsck report</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsckForSnapshotFiles()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			try
			{
				string runFsck = RunFsck(conf, 0, true, "/", "-includeSnapshots", "-files");
				NUnit.Framework.Assert.IsTrue(runFsck.Contains("HEALTHY"));
				string fileName = "/srcdat";
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				Path file1 = new Path(fileName);
				DFSTestUtil.CreateFile(hdfs, file1, 1024, (short)1, 1000L);
				hdfs.AllowSnapshot(new Path("/"));
				hdfs.CreateSnapshot(new Path("/"), "mySnapShot");
				runFsck = RunFsck(conf, 0, true, "/", "-includeSnapshots", "-files");
				NUnit.Framework.Assert.IsTrue(runFsck.Contains("/.snapshot/mySnapShot/srcdat"));
				runFsck = RunFsck(conf, 0, true, "/", "-files");
				NUnit.Framework.Assert.IsFalse(runFsck.Contains("mySnapShot"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test for blockIdCK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockIdCK()
		{
			short ReplFactor = 2;
			short NumDn = 2;
			long blockSize = 512;
			string[] racks = new string[] { "/rack1", "/rack2" };
			string[] hosts = new string[] { "host1", "host2" };
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 2);
			MiniDFSCluster cluster = null;
			DistributedFileSystem dfs = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDn).Hosts(hosts).Racks
				(racks).Build();
			NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
			cluster.WaitClusterUp();
			dfs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
			DFSTestUtil util = new DFSTestUtil.Builder().SetName(GetType().Name).SetNumFiles(
				1).Build();
			//create files
			string pathString = new string("/testfile");
			Path path = new Path(pathString);
			DFSTestUtil.CreateFile(dfs, path, 1024, ReplFactor, 1000L);
			DFSTestUtil.WaitReplication(dfs, path, ReplFactor);
			StringBuilder sb = new StringBuilder();
			foreach (LocatedBlock lb in DFSTestUtil.GetAllBlocks(dfs, path))
			{
				sb.Append(lb.GetBlock().GetLocalBlock().GetBlockName() + " ");
			}
			string[] bIds = sb.ToString().Split(" ");
			//run fsck
			try
			{
				//illegal input test
				string runFsckResult = RunFsck(conf, 0, true, "/", "-blockId", "not_a_block_id");
				NUnit.Framework.Assert.IsTrue(runFsckResult.Contains("Incorrect blockId format:")
					);
				//general test
				runFsckResult = RunFsck(conf, 0, true, "/", "-blockId", sb.ToString());
				NUnit.Framework.Assert.IsTrue(runFsckResult.Contains(bIds[0]));
				NUnit.Framework.Assert.IsTrue(runFsckResult.Contains(bIds[1]));
				NUnit.Framework.Assert.IsTrue(runFsckResult.Contains("Block replica on datanode/rack: host1/rack1 is HEALTHY"
					));
				NUnit.Framework.Assert.IsTrue(runFsckResult.Contains("Block replica on datanode/rack: host2/rack2 is HEALTHY"
					));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test for blockIdCK with datanode decommission</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockIdCKDecommission()
		{
			short ReplFactor = 1;
			short NumDn = 2;
			long blockSize = 512;
			bool checkDecommissionInProgress = false;
			string[] racks = new string[] { "/rack1", "/rack2" };
			string[] hosts = new string[] { "host1", "host2" };
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 2);
			MiniDFSCluster cluster;
			DistributedFileSystem dfs;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDn).Hosts(hosts).Racks
				(racks).Build();
			NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
			cluster.WaitClusterUp();
			dfs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
			DFSTestUtil util = new DFSTestUtil.Builder().SetName(GetType().Name).SetNumFiles(
				1).Build();
			//create files
			string pathString = new string("/testfile");
			Path path = new Path(pathString);
			DFSTestUtil.CreateFile(dfs, path, 1024, ReplFactor, 1000L);
			DFSTestUtil.WaitReplication(dfs, path, ReplFactor);
			StringBuilder sb = new StringBuilder();
			foreach (LocatedBlock lb in DFSTestUtil.GetAllBlocks(dfs, path))
			{
				sb.Append(lb.GetBlock().GetLocalBlock().GetBlockName() + " ");
			}
			string[] bIds = sb.ToString().Split(" ");
			try
			{
				//make sure datanode that has replica is fine before decommission
				string outStr = RunFsck(conf, 0, true, "/", "-blockId", bIds[0]);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				//decommission datanode
				ExtendedBlock eb = DFSTestUtil.GetFirstBlock(dfs, path);
				DatanodeDescriptor dn = cluster.GetNameNode().GetNamesystem().GetBlockManager().GetBlockCollection
					(eb.GetLocalBlock()).GetBlocks()[0].GetDatanode(0);
				cluster.GetNameNode().GetNamesystem().GetBlockManager().GetDatanodeManager().GetDecomManager
					().StartDecommission(dn);
				string dnName = dn.GetXferAddr();
				//wait for decommission start
				DatanodeInfo datanodeInfo = null;
				int count = 0;
				do
				{
					Sharpen.Thread.Sleep(2000);
					foreach (DatanodeInfo info in dfs.GetDataNodeStats())
					{
						if (dnName.Equals(info.GetXferAddr()))
						{
							datanodeInfo = info;
						}
					}
					//check decommissioning only once
					if (!checkDecommissionInProgress && datanodeInfo != null && datanodeInfo.IsDecommissionInProgress
						())
					{
						string fsckOut = RunFsck(conf, 3, true, "/", "-blockId", bIds[0]);
						NUnit.Framework.Assert.IsTrue(fsckOut.Contains(NamenodeFsck.DecommissioningStatus
							));
						checkDecommissionInProgress = true;
					}
				}
				while (datanodeInfo != null && !datanodeInfo.IsDecommissioned());
				//check decommissioned
				string fsckOut_1 = RunFsck(conf, 2, true, "/", "-blockId", bIds[0]);
				NUnit.Framework.Assert.IsTrue(fsckOut_1.Contains(NamenodeFsck.DecommissionedStatus
					));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test for blockIdCK with block corruption</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockIdCKCorruption()
		{
			short NumDn = 1;
			long blockSize = 512;
			Random random = new Random();
			DFSClient dfsClient;
			LocatedBlocks blocks;
			ExtendedBlock block;
			short repFactor = 1;
			string[] racks = new string[] { "/rack1" };
			string[] hosts = new string[] { "host1" };
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			MiniDFSCluster cluster = null;
			DistributedFileSystem dfs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDn).Hosts(hosts).Racks
					(racks).Build();
				NUnit.Framework.Assert.IsNotNull("Failed Cluster Creation", cluster);
				cluster.WaitClusterUp();
				dfs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsNotNull("Failed to get FileSystem", dfs);
				DFSTestUtil util = new DFSTestUtil.Builder().SetName(GetType().Name).SetNumFiles(
					1).Build();
				//create files
				string pathString = new string("/testfile");
				Path path = new Path(pathString);
				DFSTestUtil.CreateFile(dfs, path, 1024, repFactor, 1000L);
				DFSTestUtil.WaitReplication(dfs, path, repFactor);
				StringBuilder sb = new StringBuilder();
				foreach (LocatedBlock lb in DFSTestUtil.GetAllBlocks(dfs, path))
				{
					sb.Append(lb.GetBlock().GetLocalBlock().GetBlockName() + " ");
				}
				string[] bIds = sb.ToString().Split(" ");
				//make sure block is healthy before we corrupt it
				string outStr = RunFsck(conf, 0, true, "/", "-blockId", bIds[0]);
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.HealthyStatus));
				// corrupt replicas
				block = DFSTestUtil.GetFirstBlock(dfs, path);
				FilePath blockFile = cluster.GetBlockFile(0, block);
				if (blockFile != null && blockFile.Exists())
				{
					RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
					FileChannel channel = raFile.GetChannel();
					string badString = "BADBAD";
					int rand = random.Next((int)channel.Size() / 2);
					raFile.Seek(rand);
					raFile.Write(Sharpen.Runtime.GetBytesForString(badString));
					raFile.Close();
				}
				DFSTestUtil.WaitCorruptReplicas(dfs, cluster.GetNamesystem(), path, block, 1);
				outStr = RunFsck(conf, 1, false, "/", "-blockId", block.GetBlockName());
				System.Console.Out.WriteLine(outStr);
				NUnit.Framework.Assert.IsTrue(outStr.Contains(NamenodeFsck.CorruptStatus));
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
		private void WriteFile(DistributedFileSystem dfs, Path dir, string fileName)
		{
			Path filePath = new Path(dir.ToString() + Path.Separator + fileName);
			FSDataOutputStream @out = dfs.Create(filePath);
			@out.WriteChars("teststring");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(DistributedFileSystem dfs, string dirName, string fileName
			, string StoragePolicy)
		{
			Path dirPath = new Path(dirName);
			dfs.Mkdirs(dirPath);
			dfs.SetStoragePolicy(dirPath, StoragePolicy);
			WriteFile(dfs, dirPath, fileName);
		}

		/// <summary>Test storage policy display</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStoragePoliciesCK()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).StorageTypes
				(new StorageType[] { StorageType.Disk, StorageType.Archive }).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				WriteFile(dfs, "/testhot", "file", "HOT");
				WriteFile(dfs, "/testwarm", "file", "WARM");
				WriteFile(dfs, "/testcold", "file", "COLD");
				string outStr = RunFsck(conf, 0, true, "/", "-storagepolicies");
				NUnit.Framework.Assert.IsTrue(outStr.Contains("DISK:3(HOT)"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("DISK:1,ARCHIVE:2(WARM)"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("ARCHIVE:3(COLD)"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("All blocks satisfy specified storage policy."
					));
				dfs.SetStoragePolicy(new Path("/testhot"), "COLD");
				dfs.SetStoragePolicy(new Path("/testwarm"), "COLD");
				outStr = RunFsck(conf, 0, true, "/", "-storagepolicies");
				NUnit.Framework.Assert.IsTrue(outStr.Contains("DISK:3(HOT)"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("DISK:1,ARCHIVE:2(WARM)"));
				NUnit.Framework.Assert.IsTrue(outStr.Contains("ARCHIVE:3(COLD)"));
				NUnit.Framework.Assert.IsFalse(outStr.Contains("All blocks satisfy specified storage policy."
					));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
