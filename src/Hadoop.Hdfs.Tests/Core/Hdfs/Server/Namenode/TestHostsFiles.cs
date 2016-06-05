using System.Text;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>DFS_HOSTS and DFS_HOSTS_EXCLUDE tests</summary>
	public class TestHostsFiles
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestHostsFiles).FullName
			);

		/*
		* Return a configuration object with low timeouts for testing and
		* a topology script set (which enables rack awareness).
		*/
		private Configuration GetConf()
		{
			Configuration conf = new HdfsConfiguration();
			// Lower the heart beat interval so the NN quickly learns of dead
			// or decommissioned DNs and the NN issues replication and invalidation
			// commands quickly (as replies to heartbeats)
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			// Have the NN ReplicationMonitor compute the replication and
			// invalidation commands to send DNs every second.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			// Have the NN check for pending replications every second so it
			// quickly schedules additional replicas as they are identified.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationPendingTimeoutSecKey, 1);
			// The DNs report blocks every second.
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			// Indicates we have multiple racks
			conf.Set(DFSConfigKeys.NetTopologyScriptFileNameKey, "xyz");
			return conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHostsExcludeInUI()
		{
			Configuration conf = GetConf();
			short ReplicationFactor = 2;
			Path filePath = new Path("/testFile");
			// Configure an excludes file
			FileSystem localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, "build/test/data/temp/decommission");
			Path excludeFile = new Path(dir, "exclude");
			Path includeFile = new Path(dir, "include");
			NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
			DFSTestUtil.WriteFile(localFileSys, excludeFile, string.Empty);
			DFSTestUtil.WriteFile(localFileSys, includeFile, string.Empty);
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
			// Two blocks and four racks
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2", "/rack2" };
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(racks.Length
				).Racks(racks).Build();
			FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
			try
			{
				// Create a file with one block
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, filePath, 1L, ReplicationFactor, 1L);
				ExtendedBlock b = DFSTestUtil.GetFirstBlock(fs, filePath);
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				// Decommission one of the hosts with the block, this should cause 
				// the block to get replicated to another host on the same rack,
				// otherwise the rack policy is violated.
				BlockLocation[] locs = fs.GetFileBlockLocations(fs.GetFileStatus(filePath), 0, long.MaxValue
					);
				string name = locs[0].GetNames()[0];
				string names = name + "\n" + "localhost:42\n";
				Log.Info("adding '" + names + "' to exclude file " + excludeFile.ToUri().GetPath(
					));
				DFSTestUtil.WriteFile(localFileSys, excludeFile, name);
				ns.GetBlockManager().GetDatanodeManager().RefreshNodes(conf);
				DFSTestUtil.WaitForDecommission(fs, name);
				// Check the block still has sufficient # replicas across racks
				DFSTestUtil.WaitForReplication(cluster, b, 2, ReplicationFactor, 0);
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo"
					);
				string nodes = (string)mbs.GetAttribute(mxbeanName, "LiveNodes");
				NUnit.Framework.Assert.IsTrue("Live nodes should contain the decommissioned node"
					, nodes.Contains("Decommissioned"));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHostsIncludeForDeadCount()
		{
			Configuration conf = GetConf();
			// Configure an excludes file
			FileSystem localFileSys = FileSystem.GetLocal(conf);
			Path workingDir = localFileSys.GetWorkingDirectory();
			Path dir = new Path(workingDir, "build/test/data/temp/decommission");
			Path excludeFile = new Path(dir, "exclude");
			Path includeFile = new Path(dir, "include");
			NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
			StringBuilder includeHosts = new StringBuilder();
			includeHosts.Append("localhost:52").Append("\n").Append("127.0.0.1:7777").Append(
				"\n");
			DFSTestUtil.WriteFile(localFileSys, excludeFile, string.Empty);
			DFSTestUtil.WriteFile(localFileSys, includeFile, includeHosts.ToString());
			conf.Set(DFSConfigKeys.DfsHostsExclude, excludeFile.ToUri().GetPath());
			conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				FSNamesystem ns = cluster.GetNameNode().GetNamesystem();
				NUnit.Framework.Assert.IsTrue(ns.GetNumDeadDataNodes() == 2);
				NUnit.Framework.Assert.IsTrue(ns.GetNumLiveDataNodes() == 0);
				// Testing using MBeans
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				string nodes = mbs.GetAttribute(mxbeanName, "NumDeadDataNodes") + string.Empty;
				NUnit.Framework.Assert.IsTrue((int)mbs.GetAttribute(mxbeanName, "NumDeadDataNodes"
					) == 2);
				NUnit.Framework.Assert.IsTrue((int)mbs.GetAttribute(mxbeanName, "NumLiveDataNodes"
					) == 0);
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
