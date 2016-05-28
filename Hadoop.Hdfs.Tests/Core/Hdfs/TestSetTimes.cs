using System;
using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the access time on files.</summary>
	public class TestSetTimes
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		internal const int numDatanodes = 1;

		internal static readonly SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm"
			);

		internal Random myrand = new Random();

		internal Path hostsFile;

		internal Path excludeFile;

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream WriteFile(FileSystem fileSys, Path name, int repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			return stm;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		private void PrintDatanodeReport(DatanodeInfo[] info)
		{
			System.Console.Out.WriteLine("-------------------------------------------------");
			for (int i = 0; i < info.Length; i++)
			{
				System.Console.Out.WriteLine(info[i].GetDatanodeReport());
				System.Console.Out.WriteLine();
			}
		}

		/// <summary>Tests mod & access time in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTimes()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			cluster.WaitActive();
			int nnport = cluster.GetNameNodePort();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", numDatanodes, info.Length
				);
			FileSystem fileSys = cluster.GetFileSystem();
			int replicas = 1;
			NUnit.Framework.Assert.IsTrue(fileSys is DistributedFileSystem);
			try
			{
				//
				// create file and record atime/mtime
				//
				System.Console.Out.WriteLine("Creating testdir1 and testdir1/test1.dat.");
				Path dir1 = new Path("testdir1");
				Path file1 = new Path(dir1, "test1.dat");
				FSDataOutputStream stm = WriteFile(fileSys, file1, replicas);
				FileStatus stat = fileSys.GetFileStatus(file1);
				long atimeBeforeClose = stat.GetAccessTime();
				string adate = dateForm.Format(Sharpen.Extensions.CreateDate(atimeBeforeClose));
				System.Console.Out.WriteLine("atime on " + file1 + " before close is " + adate + 
					" (" + atimeBeforeClose + ")");
				NUnit.Framework.Assert.IsTrue(atimeBeforeClose != 0);
				stm.Close();
				stat = fileSys.GetFileStatus(file1);
				long atime1 = stat.GetAccessTime();
				long mtime1 = stat.GetModificationTime();
				adate = dateForm.Format(Sharpen.Extensions.CreateDate(atime1));
				string mdate = dateForm.Format(Sharpen.Extensions.CreateDate(mtime1));
				System.Console.Out.WriteLine("atime on " + file1 + " is " + adate + " (" + atime1
					 + ")");
				System.Console.Out.WriteLine("mtime on " + file1 + " is " + mdate + " (" + mtime1
					 + ")");
				NUnit.Framework.Assert.IsTrue(atime1 != 0);
				//
				// record dir times
				//
				stat = fileSys.GetFileStatus(dir1);
				long mdir1 = stat.GetAccessTime();
				NUnit.Framework.Assert.IsTrue(mdir1 == 0);
				// set the access time to be one day in the past
				long atime2 = atime1 - (24L * 3600L * 1000L);
				fileSys.SetTimes(file1, -1, atime2);
				// check new access time on file
				stat = fileSys.GetFileStatus(file1);
				long atime3 = stat.GetAccessTime();
				string adate3 = dateForm.Format(Sharpen.Extensions.CreateDate(atime3));
				System.Console.Out.WriteLine("new atime on " + file1 + " is " + adate3 + " (" + atime3
					 + ")");
				NUnit.Framework.Assert.IsTrue(atime2 == atime3);
				NUnit.Framework.Assert.IsTrue(mtime1 == stat.GetModificationTime());
				// set the modification time to be 1 hour in the past
				long mtime2 = mtime1 - (3600L * 1000L);
				fileSys.SetTimes(file1, mtime2, -1);
				// check new modification time on file
				stat = fileSys.GetFileStatus(file1);
				long mtime3 = stat.GetModificationTime();
				string mdate3 = dateForm.Format(Sharpen.Extensions.CreateDate(mtime3));
				System.Console.Out.WriteLine("new mtime on " + file1 + " is " + mdate3 + " (" + mtime3
					 + ")");
				NUnit.Framework.Assert.IsTrue(atime2 == stat.GetAccessTime());
				NUnit.Framework.Assert.IsTrue(mtime2 == mtime3);
				long mtime4 = Time.Now() - (3600L * 1000L);
				long atime4 = Time.Now();
				fileSys.SetTimes(dir1, mtime4, atime4);
				// check new modification time on file
				stat = fileSys.GetFileStatus(dir1);
				NUnit.Framework.Assert.IsTrue("Not matching the modification times", mtime4 == stat
					.GetModificationTime());
				NUnit.Framework.Assert.IsTrue("Not matching the access times", atime4 == stat.GetAccessTime
					());
				Path nonExistingDir = new Path(dir1, "/nonExistingDir/");
				try
				{
					fileSys.SetTimes(nonExistingDir, mtime4, atime4);
					NUnit.Framework.Assert.Fail("Expecting FileNotFoundException");
				}
				catch (FileNotFoundException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("File/Directory " + nonExistingDir
						.ToString() + " does not exist."));
				}
				// shutdown cluster and restart
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
				fileSys = cluster.GetFileSystem();
				// verify that access times and modification times persist after a
				// cluster restart.
				System.Console.Out.WriteLine("Verifying times after cluster restart");
				stat = fileSys.GetFileStatus(file1);
				NUnit.Framework.Assert.IsTrue(atime2 == stat.GetAccessTime());
				NUnit.Framework.Assert.IsTrue(mtime3 == stat.GetModificationTime());
				CleanupFile(fileSys, file1);
				CleanupFile(fileSys, dir1);
			}
			catch (IOException e)
			{
				info = client.DatanodeReport(HdfsConstants.DatanodeReportType.All);
				PrintDatanodeReport(info);
				throw;
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests mod time change at close in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTimesAtClose()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			int replicas = 1;
			// parameter initialization
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 50);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			cluster.WaitActive();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", numDatanodes, info.Length
				);
			FileSystem fileSys = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue(fileSys is DistributedFileSystem);
			try
			{
				// create a new file and write to it
				Path file1 = new Path("/simple.dat");
				FSDataOutputStream stm = WriteFile(fileSys, file1, replicas);
				System.Console.Out.WriteLine("Created and wrote file simple.dat");
				FileStatus statBeforeClose = fileSys.GetFileStatus(file1);
				long mtimeBeforeClose = statBeforeClose.GetModificationTime();
				string mdateBeforeClose = dateForm.Format(Sharpen.Extensions.CreateDate(mtimeBeforeClose
					));
				System.Console.Out.WriteLine("mtime on " + file1 + " before close is " + mdateBeforeClose
					 + " (" + mtimeBeforeClose + ")");
				NUnit.Framework.Assert.IsTrue(mtimeBeforeClose != 0);
				//close file after writing
				stm.Close();
				System.Console.Out.WriteLine("Closed file.");
				FileStatus statAfterClose = fileSys.GetFileStatus(file1);
				long mtimeAfterClose = statAfterClose.GetModificationTime();
				string mdateAfterClose = dateForm.Format(Sharpen.Extensions.CreateDate(mtimeAfterClose
					));
				System.Console.Out.WriteLine("mtime on " + file1 + " after close is " + mdateAfterClose
					 + " (" + mtimeAfterClose + ")");
				NUnit.Framework.Assert.IsTrue(mtimeAfterClose != 0);
				NUnit.Framework.Assert.IsTrue(mtimeBeforeClose != mtimeAfterClose);
				CleanupFile(fileSys, file1);
			}
			catch (IOException e)
			{
				info = client.DatanodeReport(HdfsConstants.DatanodeReportType.All);
				PrintDatanodeReport(info);
				throw;
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that when access time updates are not needed, the FSNamesystem
		/// write lock is not taken by getBlockLocations.
		/// </summary>
		/// <remarks>
		/// Test that when access time updates are not needed, the FSNamesystem
		/// write lock is not taken by getBlockLocations.
		/// Regression test for HDFS-3981.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetBlockLocationsOnlyUsesReadLock()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, 100 * 1000);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			ReentrantReadWriteLock spyLock = NameNodeAdapter.SpyOnFsLock(cluster.GetNamesystem
				());
			try
			{
				// Create empty file in the FSN.
				Path p = new Path("/empty-file");
				DFSTestUtil.CreateFile(cluster.GetFileSystem(), p, 0, (short)1, 0L);
				// getBlockLocations() should not need the write lock, since we just created
				// the file (and thus its access time is already within the 100-second
				// accesstime precision configured above). 
				MockitoUtil.DoThrowWhenCallStackMatches(new Exception("Should not need write lock"
					), ".*getBlockLocations.*").When(spyLock).WriteLock();
				cluster.GetFileSystem().GetFileBlockLocations(p, 0, 100);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new TestSetTimes().TestTimes();
		}
	}
}
