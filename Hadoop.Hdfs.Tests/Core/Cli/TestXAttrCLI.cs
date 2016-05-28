using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class TestXAttrCLI : CLITestHelperDFS
	{
		protected internal MiniDFSCluster dfsCluster = null;

		protected internal FileSystem fs = null;

		protected internal string namenode = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			conf.SetClass(PolicyProvider.PolicyProviderConfig, typeof(HDFSPolicyProvider), typeof(
				PolicyProvider));
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			dfsCluster.WaitClusterUp();
			namenode = conf.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
			username = Runtime.GetProperty("user.name");
			fs = dfsCluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
		}

		protected override string GetTestFile()
		{
			return "testXAttrConf.xml";
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
			expCmd = expCmd.ReplaceAll("#LF#", Runtime.GetProperty("line.separator"));
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
