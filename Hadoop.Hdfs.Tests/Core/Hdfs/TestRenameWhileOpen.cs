using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestRenameWhileOpen
	{
		//TODO: un-comment checkFullFile once the lease recovery is done
		/// <exception cref="System.IO.IOException"/>
		private static void CheckFullFile(FileSystem fs, Path p)
		{
		}

		//TestFileCreation.checkFullFile(fs, p);
		/// <summary>
		/// open /user/dir1/file1 /user/dir2/file2
		/// mkdir /user/dir3
		/// move /user/dir1 /user/dir3
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWhileOpenRenameParent()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 1);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, TestFileCreation.blockSize);
			// create cluster
			System.Console.Out.WriteLine("Test 1*****************************");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				// Normally, the in-progress edit log would be finalized by
				// FSEditLog#endCurrentLogSegment.  For testing purposes, we
				// disable that here.
				FSEditLog spyLog = Org.Mockito.Mockito.Spy(cluster.GetNameNode().GetFSImage().GetEditLog
					());
				Org.Mockito.Mockito.DoNothing().When(spyLog).EndCurrentLogSegment(Org.Mockito.Mockito
					.AnyBoolean());
				DFSTestUtil.SetEditLogForTesting(cluster.GetNamesystem(), spyLog);
				int nnport = cluster.GetNameNodePort();
				// create file1.
				Path dir1 = new Path("/user/a+b/dir1");
				Path file1 = new Path(dir1, "file1");
				FSDataOutputStream stm1 = TestFileCreation.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file1);
				TestFileCreation.WriteFile(stm1);
				stm1.Hflush();
				// create file2.
				Path dir2 = new Path("/user/dir2");
				Path file2 = new Path(dir2, "file2");
				FSDataOutputStream stm2 = TestFileCreation.CreateFile(fs, file2, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file2);
				TestFileCreation.WriteFile(stm2);
				stm2.Hflush();
				// move dir1 while file1 is open
				Path dir3 = new Path("/user/dir3");
				fs.Mkdirs(dir3);
				fs.Rename(dir1, dir3);
				// create file3
				Path file3 = new Path(dir3, "file3");
				FSDataOutputStream stm3 = fs.Create(file3);
				fs.Rename(file3, new Path(dir3, "bozo"));
				// Get a new block for the file.
				TestFileCreation.WriteFile(stm3, TestFileCreation.blockSize + 1);
				stm3.Hflush();
				// Stop the NameNode before closing the files.
				// This will ensure that the write leases are still active and present
				// in the edit log.  Simiarly, there should be a pending ADD_BLOCK_OP
				// for file3, since we just added a block to that file.
				cluster.GetNameNode().Stop();
				// Restart cluster with the same namenode port as before.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(2 * MaxIdleTime);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				// restart cluster yet again. This triggers the code to read in
				// persistent leases from the edit log.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path newfile = new Path("/user/dir3/dir1", "file1");
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(file2));
				NUnit.Framework.Assert.IsTrue(fs.Exists(newfile));
				CheckFullFile(fs, newfile);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// open /user/dir1/file1 /user/dir2/file2
		/// move /user/dir1 /user/dir3
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWhileOpenRenameParentToNonexistentDir()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 1);
			System.Console.Out.WriteLine("Test 2************************************");
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				int nnport = cluster.GetNameNodePort();
				// create file1.
				Path dir1 = new Path("/user/dir1");
				Path file1 = new Path(dir1, "file1");
				FSDataOutputStream stm1 = TestFileCreation.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file1);
				TestFileCreation.WriteFile(stm1);
				stm1.Hflush();
				// create file2.
				Path dir2 = new Path("/user/dir2");
				Path file2 = new Path(dir2, "file2");
				FSDataOutputStream stm2 = TestFileCreation.CreateFile(fs, file2, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file2);
				TestFileCreation.WriteFile(stm2);
				stm2.Hflush();
				// move dir1 while file1 is open
				Path dir3 = new Path("/user/dir3");
				fs.Rename(dir1, dir3);
				// restart cluster with the same namenode port as before.
				// This ensures that leases are persisted in fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(2 * MaxIdleTime);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				// restart cluster yet again. This triggers the code to read in
				// persistent leases from fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path newfile = new Path("/user/dir3", "file1");
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(file2));
				NUnit.Framework.Assert.IsTrue(fs.Exists(newfile));
				CheckFullFile(fs, newfile);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// open /user/dir1/file1
		/// mkdir /user/dir2
		/// move /user/dir1/file1 /user/dir2/
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWhileOpenRenameToExistentDirectory()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 1);
			System.Console.Out.WriteLine("Test 3************************************");
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				int nnport = cluster.GetNameNodePort();
				// create file1.
				Path dir1 = new Path("/user/dir1");
				Path file1 = new Path(dir1, "file1");
				FSDataOutputStream stm1 = TestFileCreation.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file1);
				TestFileCreation.WriteFile(stm1);
				stm1.Hflush();
				Path dir2 = new Path("/user/dir2");
				fs.Mkdirs(dir2);
				fs.Rename(file1, dir2);
				// restart cluster with the same namenode port as before.
				// This ensures that leases are persisted in fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(2 * MaxIdleTime);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				// restart cluster yet again. This triggers the code to read in
				// persistent leases from fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path newfile = new Path("/user/dir2", "file1");
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(newfile));
				CheckFullFile(fs, newfile);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// open /user/dir1/file1
		/// move /user/dir1/file1 /user/dir2/
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWhileOpenRenameToNonExistentDirectory()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 1);
			System.Console.Out.WriteLine("Test 4************************************");
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				int nnport = cluster.GetNameNodePort();
				// create file1.
				Path dir1 = new Path("/user/dir1");
				Path file1 = new Path(dir1, "file1");
				FSDataOutputStream stm1 = TestFileCreation.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file1);
				TestFileCreation.WriteFile(stm1);
				stm1.Hflush();
				Path dir2 = new Path("/user/dir2");
				fs.Rename(file1, dir2);
				// restart cluster with the same namenode port as before.
				// This ensures that leases are persisted in fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(2 * MaxIdleTime);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				// restart cluster yet again. This triggers the code to read in
				// persistent leases from fsimage.
				cluster.Shutdown();
				try
				{
					Sharpen.Thread.Sleep(5000);
				}
				catch (Exception)
				{
				}
				cluster = new MiniDFSCluster.Builder(conf).NameNodePort(nnport).Format(false).Build
					();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				Path newfile = new Path("/user", "dir2");
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(newfile));
				CheckFullFile(fs, newfile);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		public TestRenameWhileOpen()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
			}
		}
	}
}
