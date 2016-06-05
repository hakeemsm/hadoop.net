using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class TestHDFSCLI : CLITestHelperDFS
	{
		protected internal MiniDFSCluster dfsCluster = null;

		protected internal FileSystem fs = null;

		protected internal string namenode = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
			conf.SetClass(PolicyProvider.PolicyProviderConfig, typeof(HDFSPolicyProvider), typeof(
				PolicyProvider));
			// Many of the tests expect a replication value of 1 in the output
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			// Build racks and hosts configuration to test dfsAdmin -printTopology
			string[] racks = new string[] { "/rack1", "/rack1", "/rack2", "/rack2", "/rack2", 
				"/rack3", "/rack4", "/rack4" };
			string[] hosts = new string[] { "host1", "host2", "host3", "host4", "host5", "host6"
				, "host7", "host8" };
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(8).Racks(racks).Hosts(
				hosts).Build();
			dfsCluster.WaitClusterUp();
			namenode = conf.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
			username = Runtime.GetProperty("user.name");
			fs = dfsCluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
		}

		protected override string GetTestFile()
		{
			return "testHDFSConf.xml";
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
			Sharpen.Thread.Sleep(2000);
			base.TearDown();
		}

		protected override string ExpandCommand(string cmd)
		{
			string expCmd = cmd;
			expCmd = expCmd.ReplaceAll("NAMENODE", namenode);
			expCmd = base.ExpandCommand(expCmd);
			return expCmd;
		}

		/// <exception cref="System.Exception"/>
		protected override CommandExecutor.Result Execute(CLICommand cmd)
		{
			return cmd.GetExecutor(namenode).ExecuteCommand(cmd.GetCmd());
		}

		[NUnit.Framework.Test]
		public override void TestAll()
		{
			base.TestAll();
		}
	}
}
