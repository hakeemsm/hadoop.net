using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class TestCacheAdminCLI : CLITestHelper
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestCacheAdminCLI));

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
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			dfsCluster.WaitClusterUp();
			namenode = conf.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
			username = Runtime.GetProperty("user.name");
			fs = dfsCluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
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

		protected override string GetTestFile()
		{
			return "testCacheAdminConf.xml";
		}

		protected override CLITestHelper.TestConfigFileParser GetConfigParser()
		{
			return new TestCacheAdminCLI.TestConfigFileParserCacheAdmin(this);
		}

		private class TestConfigFileParserCacheAdmin : CLITestHelper.TestConfigFileParser
		{
			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndElement(string uri, string localName, string qName)
			{
				if (qName.Equals("cache-admin-command"))
				{
					if (this.testCommands != null)
					{
						this.testCommands.AddItem(new TestCacheAdminCLI.CLITestCmdCacheAdmin(this, this.charString
							, new CLICommandCacheAdmin()));
					}
					else
					{
						if (this.cleanupCommands != null)
						{
							this.cleanupCommands.AddItem(new TestCacheAdminCLI.CLITestCmdCacheAdmin(this, this
								.charString, new CLICommandCacheAdmin()));
						}
					}
				}
				else
				{
					base.EndElement(uri, localName, qName);
				}
			}

			internal TestConfigFileParserCacheAdmin(TestCacheAdminCLI _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCacheAdminCLI _enclosing;
		}

		private class CLITestCmdCacheAdmin : CLITestCmd
		{
			public CLITestCmdCacheAdmin(TestCacheAdminCLI _enclosing, string str, CLICommandTypes
				 type)
				: base(str, type)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.ArgumentException"/>
			public override CommandExecutor GetExecutor(string tag)
			{
				if (this.GetType() is CLICommandCacheAdmin)
				{
					return new CacheAdminCmdExecutor(tag, new CacheAdmin(this._enclosing.conf));
				}
				return base.GetExecutor(tag);
			}

			private readonly TestCacheAdminCLI _enclosing;
		}

		/// <exception cref="System.Exception"/>
		protected override CommandExecutor.Result Execute(CLICommand cmd)
		{
			return cmd.GetExecutor(string.Empty).ExecuteCommand(cmd.GetCmd());
		}

		[NUnit.Framework.Test]
		public override void TestAll()
		{
			base.TestAll();
		}
	}
}
