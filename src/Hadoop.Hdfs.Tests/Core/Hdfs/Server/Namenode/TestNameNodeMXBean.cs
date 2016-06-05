using System.Collections;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Util.Concurrent;
using Javax.Management;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Org.Codehaus.Jackson.Map;
using Org.Mortbay.Util.Ajax;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Class for testing
	/// <see cref="NameNodeMXBean"/>
	/// implementation
	/// </summary>
	public class TestNameNodeMXBean
	{
		/// <summary>Used to assert equality between doubles</summary>
		private const double Delta = 0.000001;

		static TestNameNodeMXBean()
		{
			NativeIO.POSIX.SetCacheManipulator(new NativeIO.POSIX.NoMlockCacheManipulator());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNameNodeMXBeanInfo()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, NativeIO.POSIX.GetCacheManipulator
				().GetMemlockLimit());
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNameNode().namesystem;
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo"
					);
				// get attribute "ClusterId"
				string clusterId = (string)mbs.GetAttribute(mxbeanName, "ClusterId");
				NUnit.Framework.Assert.AreEqual(fsn.GetClusterId(), clusterId);
				// get attribute "BlockPoolId"
				string blockpoolId = (string)mbs.GetAttribute(mxbeanName, "BlockPoolId");
				NUnit.Framework.Assert.AreEqual(fsn.GetBlockPoolId(), blockpoolId);
				// get attribute "Version"
				string version = (string)mbs.GetAttribute(mxbeanName, "Version");
				NUnit.Framework.Assert.AreEqual(fsn.GetVersion(), version);
				NUnit.Framework.Assert.IsTrue(version.Equals(VersionInfo.GetVersion() + ", r" + VersionInfo
					.GetRevision()));
				// get attribute "Used"
				long used = (long)mbs.GetAttribute(mxbeanName, "Used");
				NUnit.Framework.Assert.AreEqual(fsn.GetUsed(), used);
				// get attribute "Total"
				long total = (long)mbs.GetAttribute(mxbeanName, "Total");
				NUnit.Framework.Assert.AreEqual(fsn.GetTotal(), total);
				// get attribute "safemode"
				string safemode = (string)mbs.GetAttribute(mxbeanName, "Safemode");
				NUnit.Framework.Assert.AreEqual(fsn.GetSafemode(), safemode);
				// get attribute nondfs
				long nondfs = (long)(mbs.GetAttribute(mxbeanName, "NonDfsUsedSpace"));
				NUnit.Framework.Assert.AreEqual(fsn.GetNonDfsUsedSpace(), nondfs);
				// get attribute percentremaining
				float percentremaining = (float)(mbs.GetAttribute(mxbeanName, "PercentRemaining")
					);
				NUnit.Framework.Assert.AreEqual(fsn.GetPercentRemaining(), percentremaining, Delta
					);
				// get attribute Totalblocks
				long totalblocks = (long)(mbs.GetAttribute(mxbeanName, "TotalBlocks"));
				NUnit.Framework.Assert.AreEqual(fsn.GetTotalBlocks(), totalblocks);
				// get attribute alivenodeinfo
				string alivenodeinfo = (string)(mbs.GetAttribute(mxbeanName, "LiveNodes"));
				IDictionary<string, IDictionary<string, object>> liveNodes = (IDictionary<string, 
					IDictionary<string, object>>)JSON.Parse(alivenodeinfo);
				NUnit.Framework.Assert.IsTrue(liveNodes.Count == 2);
				foreach (IDictionary<string, object> liveNode in liveNodes.Values)
				{
					NUnit.Framework.Assert.IsTrue(liveNode.Contains("nonDfsUsedSpace"));
					NUnit.Framework.Assert.IsTrue(((long)liveNode["nonDfsUsedSpace"]) > 0);
					NUnit.Framework.Assert.IsTrue(liveNode.Contains("capacity"));
					NUnit.Framework.Assert.IsTrue(((long)liveNode["capacity"]) > 0);
					NUnit.Framework.Assert.IsTrue(liveNode.Contains("numBlocks"));
					NUnit.Framework.Assert.IsTrue(((long)liveNode["numBlocks"]) == 0);
				}
				NUnit.Framework.Assert.AreEqual(fsn.GetLiveNodes(), alivenodeinfo);
				// get attribute deadnodeinfo
				string deadnodeinfo = (string)(mbs.GetAttribute(mxbeanName, "DeadNodes"));
				NUnit.Framework.Assert.AreEqual(fsn.GetDeadNodes(), deadnodeinfo);
				// get attribute NodeUsage
				string nodeUsage = (string)(mbs.GetAttribute(mxbeanName, "NodeUsage"));
				NUnit.Framework.Assert.AreEqual("Bad value for NodeUsage", fsn.GetNodeUsage(), nodeUsage
					);
				// get attribute NameJournalStatus
				string nameJournalStatus = (string)(mbs.GetAttribute(mxbeanName, "NameJournalStatus"
					));
				NUnit.Framework.Assert.AreEqual("Bad value for NameJournalStatus", fsn.GetNameJournalStatus
					(), nameJournalStatus);
				// get attribute JournalTransactionInfo
				string journalTxnInfo = (string)mbs.GetAttribute(mxbeanName, "JournalTransactionInfo"
					);
				NUnit.Framework.Assert.AreEqual("Bad value for NameTxnIds", fsn.GetJournalTransactionInfo
					(), journalTxnInfo);
				// get attribute "NNStarted"
				string nnStarted = (string)mbs.GetAttribute(mxbeanName, "NNStarted");
				NUnit.Framework.Assert.AreEqual("Bad value for NNStarted", fsn.GetNNStarted(), nnStarted
					);
				// get attribute "CompileInfo"
				string compileInfo = (string)mbs.GetAttribute(mxbeanName, "CompileInfo");
				NUnit.Framework.Assert.AreEqual("Bad value for CompileInfo", fsn.GetCompileInfo()
					, compileInfo);
				// get attribute CorruptFiles
				string corruptFiles = (string)(mbs.GetAttribute(mxbeanName, "CorruptFiles"));
				NUnit.Framework.Assert.AreEqual("Bad value for CorruptFiles", fsn.GetCorruptFiles
					(), corruptFiles);
				// get attribute NameDirStatuses
				string nameDirStatuses = (string)(mbs.GetAttribute(mxbeanName, "NameDirStatuses")
					);
				NUnit.Framework.Assert.AreEqual(fsn.GetNameDirStatuses(), nameDirStatuses);
				IDictionary<string, IDictionary<string, string>> statusMap = (IDictionary<string, 
					IDictionary<string, string>>)JSON.Parse(nameDirStatuses);
				ICollection<URI> nameDirUris = cluster.GetNameDirs(0);
				foreach (URI nameDirUri in nameDirUris)
				{
					FilePath nameDir = new FilePath(nameDirUri);
					System.Console.Out.WriteLine("Checking for the presence of " + nameDir + " in active name dirs."
						);
					NUnit.Framework.Assert.IsTrue(statusMap["active"].Contains(nameDir.GetAbsolutePath
						()));
				}
				NUnit.Framework.Assert.AreEqual(2, statusMap["active"].Count);
				NUnit.Framework.Assert.AreEqual(0, statusMap["failed"].Count);
				// This will cause the first dir to fail.
				FilePath failedNameDir = new FilePath(nameDirUris.GetEnumerator().Next());
				NUnit.Framework.Assert.AreEqual(0, FileUtil.Chmod(new FilePath(failedNameDir, "current"
					).GetAbsolutePath(), "000"));
				cluster.GetNameNodeRpc().RollEditLog();
				nameDirStatuses = (string)(mbs.GetAttribute(mxbeanName, "NameDirStatuses"));
				statusMap = (IDictionary<string, IDictionary<string, string>>)JSON.Parse(nameDirStatuses
					);
				foreach (URI nameDirUri_1 in nameDirUris)
				{
					FilePath nameDir = new FilePath(nameDirUri_1);
					string expectedStatus = nameDir.Equals(failedNameDir) ? "failed" : "active";
					System.Console.Out.WriteLine("Checking for the presence of " + nameDir + " in " +
						 expectedStatus + " name dirs.");
					NUnit.Framework.Assert.IsTrue(statusMap[expectedStatus].Contains(nameDir.GetAbsolutePath
						()));
				}
				NUnit.Framework.Assert.AreEqual(1, statusMap["active"].Count);
				NUnit.Framework.Assert.AreEqual(1, statusMap["failed"].Count);
				NUnit.Framework.Assert.AreEqual(0L, mbs.GetAttribute(mxbeanName, "CacheUsed"));
				NUnit.Framework.Assert.AreEqual(NativeIO.POSIX.GetCacheManipulator().GetMemlockLimit
					() * cluster.GetDataNodes().Count, mbs.GetAttribute(mxbeanName, "CacheCapacity")
					);
				NUnit.Framework.Assert.IsNull("RollingUpgradeInfo should be null when there is no rolling"
					 + " upgrade", mbs.GetAttribute(mxbeanName, "RollingUpgradeStatus"));
			}
			finally
			{
				if (cluster != null)
				{
					foreach (URI dir in cluster.GetNameDirs(0))
					{
						FileUtil.Chmod(new FilePath(new FilePath(dir), "current").GetAbsolutePath(), "755"
							);
					}
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLastContactTime()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				cluster.WaitActive();
				FSNamesystem fsn = cluster.GetNameNode().namesystem;
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo"
					);
				// Define include file to generate deadNodes metrics
				FileSystem localFileSys = FileSystem.GetLocal(conf);
				Path workingDir = localFileSys.GetWorkingDirectory();
				Path dir = new Path(workingDir, "build/test/data/temp/TestNameNodeMXBean");
				Path includeFile = new Path(dir, "include");
				NUnit.Framework.Assert.IsTrue(localFileSys.Mkdirs(dir));
				StringBuilder includeHosts = new StringBuilder();
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					includeHosts.Append(dn.GetDisplayName()).Append("\n");
				}
				DFSTestUtil.WriteFile(localFileSys, includeFile, includeHosts.ToString());
				conf.Set(DFSConfigKeys.DfsHosts, includeFile.ToUri().GetPath());
				fsn.GetBlockManager().GetDatanodeManager().RefreshNodes(conf);
				cluster.StopDataNode(0);
				while (fsn.GetBlockManager().GetDatanodeManager().GetNumLiveDataNodes() != 2)
				{
					Uninterruptibles.SleepUninterruptibly(1, TimeUnit.Seconds);
				}
				// get attribute deadnodeinfo
				string deadnodeinfo = (string)(mbs.GetAttribute(mxbeanName, "DeadNodes"));
				NUnit.Framework.Assert.AreEqual(fsn.GetDeadNodes(), deadnodeinfo);
				IDictionary<string, IDictionary<string, object>> deadNodes = (IDictionary<string, 
					IDictionary<string, object>>)JSON.Parse(deadnodeinfo);
				NUnit.Framework.Assert.IsTrue(deadNodes.Count > 0);
				foreach (IDictionary<string, object> deadNode in deadNodes.Values)
				{
					NUnit.Framework.Assert.IsTrue(deadNode.Contains("lastContact"));
					NUnit.Framework.Assert.IsTrue(deadNode.Contains("decommissioned"));
					NUnit.Framework.Assert.IsTrue(deadNode.Contains("xferaddr"));
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

		/// <exception cref="System.Exception"/>
		public virtual void TestTopUsers()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanNameFsns = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/");
				int NumOps = 10;
				for (int i = 0; i < NumOps; i++)
				{
					fs.ListStatus(path);
					fs.SetTimes(path, 0, 1);
				}
				string topUsers = (string)(mbs.GetAttribute(mxbeanNameFsns, "TopUserOpCounts"));
				ObjectMapper mapper = new ObjectMapper();
				IDictionary<string, object> map = mapper.ReadValue<IDictionary>(topUsers);
				NUnit.Framework.Assert.IsTrue("Could not find map key timestamp", map.Contains("timestamp"
					));
				NUnit.Framework.Assert.IsTrue("Could not find map key windows", map.Contains("windows"
					));
				IList<IDictionary<string, IList<IDictionary<string, object>>>> windows = (IList<IDictionary
					<string, IList<IDictionary<string, object>>>>)map["windows"];
				NUnit.Framework.Assert.AreEqual("Unexpected num windows", 3, windows.Count);
				foreach (IDictionary<string, IList<IDictionary<string, object>>> window in windows)
				{
					IList<IDictionary<string, object>> ops = window["ops"];
					NUnit.Framework.Assert.AreEqual("Unexpected num ops", 3, ops.Count);
					foreach (IDictionary<string, object> op in ops)
					{
						long count = long.Parse(op["totalCount"].ToString());
						string opType = op["opType"].ToString();
						int expected;
						if (opType.Equals(TopConf.AllCmds))
						{
							expected = 2 * NumOps;
						}
						else
						{
							expected = NumOps;
						}
						NUnit.Framework.Assert.AreEqual("Unexpected total count", expected, count);
					}
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

		/// <exception cref="System.Exception"/>
		public virtual void TestTopUsersDisabled()
		{
			Configuration conf = new Configuration();
			// Disable nntop
			conf.SetBoolean(DFSConfigKeys.NntopEnabledKey, false);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanNameFsns = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/");
				int NumOps = 10;
				for (int i = 0; i < NumOps; i++)
				{
					fs.ListStatus(path);
					fs.SetTimes(path, 0, 1);
				}
				string topUsers = (string)(mbs.GetAttribute(mxbeanNameFsns, "TopUserOpCounts"));
				NUnit.Framework.Assert.IsNull("Did not expect to find TopUserOpCounts bean!", topUsers
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

		/// <exception cref="System.Exception"/>
		public virtual void TestTopUsersNoPeriods()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.NntopEnabledKey, true);
			conf.Set(DFSConfigKeys.NntopWindowsMinutesKey, string.Empty);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanNameFsns = new ObjectName("Hadoop:service=NameNode,name=FSNamesystemState"
					);
				FileSystem fs = cluster.GetFileSystem();
				Path path = new Path("/");
				int NumOps = 10;
				for (int i = 0; i < NumOps; i++)
				{
					fs.ListStatus(path);
					fs.SetTimes(path, 0, 1);
				}
				string topUsers = (string)(mbs.GetAttribute(mxbeanNameFsns, "TopUserOpCounts"));
				NUnit.Framework.Assert.IsNotNull("Expected TopUserOpCounts bean!", topUsers);
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
