using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class TestAclCLI : CLITestHelperDFS
	{
		private MiniDFSCluster cluster = null;

		private FileSystem fs = null;

		private string namenode = null;

		private string username = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			fs = cluster.GetFileSystem();
			namenode = conf.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
			username = Runtime.GetProperty("user.name");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		protected override string GetTestFile()
		{
			return "testAclCLI.xml";
		}

		protected override string ExpandCommand(string cmd)
		{
			string expCmd = cmd;
			expCmd = expCmd.ReplaceAll("NAMENODE", namenode);
			expCmd = expCmd.ReplaceAll("USERNAME", username);
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
