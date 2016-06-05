using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestFileCreationDelete
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileCreationDeleteParent()
		{
			Configuration conf = new HdfsConfiguration();
			int MaxIdleTime = 2000;
			// 2s
			conf.SetInt("ipc.client.connection.maxidletime", MaxIdleTime);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = null;
			try
			{
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				int nnport = cluster.GetNameNodePort();
				// create file1.
				Path dir = new Path("/foo");
				Path file1 = new Path(dir, "file1");
				FSDataOutputStream stm1 = TestFileCreation.CreateFile(fs, file1, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file1);
				TestFileCreation.WriteFile(stm1, 1000);
				stm1.Hflush();
				// create file2.
				Path file2 = new Path("/file2");
				FSDataOutputStream stm2 = TestFileCreation.CreateFile(fs, file2, 1);
				System.Console.Out.WriteLine("testFileCreationDeleteParent: " + "Created file " +
					 file2);
				TestFileCreation.WriteFile(stm2, 1000);
				stm2.Hflush();
				// rm dir
				fs.Delete(dir, true);
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
				NUnit.Framework.Assert.IsTrue(!fs.Exists(file1));
				NUnit.Framework.Assert.IsTrue(fs.Exists(file2));
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		public TestFileCreationDelete()
		{
			{
				DFSTestUtil.SetNameNodeLogLevel(Level.All);
			}
		}
	}
}
