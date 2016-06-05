using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestSetrepIncreasing
	{
		/// <exception cref="System.IO.IOException"/>
		internal static void Setrep(int fromREP, int toREP, bool simulatedStorage)
		{
			Configuration conf = new HdfsConfiguration();
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			conf.Set(DFSConfigKeys.DfsReplicationKey, string.Empty + fromREP);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.Set(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, Sharpen.Extensions.ToString
				(2));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(10).Build(
				);
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			try
			{
				Path root = TestDFSShell.Mkdir(fs, new Path("/test/setrep" + fromREP + "-" + toREP
					));
				Path f = TestDFSShell.WriteFile(fs, new Path(root, "foo"));
				{
					// Verify setrep for changing replication
					string[] args = new string[] { "-setrep", "-w", string.Empty + toREP, string.Empty
						 + f };
					FsShell shell = new FsShell();
					shell.SetConf(conf);
					try
					{
						NUnit.Framework.Assert.AreEqual(0, shell.Run(args));
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.IsTrue("-setrep " + e, false);
					}
				}
				//get fs again since the old one may be closed
				fs = cluster.GetFileSystem();
				FileStatus file = fs.GetFileStatus(f);
				long len = file.GetLen();
				foreach (BlockLocation locations in fs.GetFileBlockLocations(file, 0, len))
				{
					NUnit.Framework.Assert.IsTrue(locations.GetHosts().Length == toREP);
				}
				TestDFSShell.Show("done setrep waiting: " + root);
			}
			finally
			{
				try
				{
					fs.Close();
				}
				catch (Exception)
				{
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetrepIncreasing()
		{
			Setrep(3, 7, false);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetrepIncreasingSimulatedStorage()
		{
			Setrep(3, 7, true);
		}
	}
}
