using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class TestCryptoAdminCLI : CLITestHelperDFS
	{
		protected internal MiniDFSCluster dfsCluster = null;

		protected internal FileSystem fs = null;

		protected internal string namenode = null;

		private static FilePath tmpDir;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
			conf.SetClass(PolicyProvider.PolicyProviderConfig, typeof(HDFSPolicyProvider), typeof(
				PolicyProvider));
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, 1);
			tmpDir = new FilePath(Runtime.GetProperty("test.build.data", "target"), UUID.RandomUUID
				().ToString()).GetAbsoluteFile();
			Path jksPath = new Path(tmpDir.ToString(), "test.jks");
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, JavaKeyStoreProvider.SchemeName
				 + "://file" + jksPath.ToUri());
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			dfsCluster.WaitClusterUp();
			CreateAKey("mykey", conf);
			namenode = conf.Get(DFSConfigKeys.FsDefaultNameKey, "file:///");
			username = Runtime.GetProperty("user.name");
			fs = dfsCluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not an HDFS: " + fs.GetUri(), fs is DistributedFileSystem
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

		/* Helper function to create a key in the Key Provider. */
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		private void CreateAKey(string keyName, Configuration conf)
		{
			KeyProvider provider = dfsCluster.GetNameNode().GetNamesystem().GetProvider();
			KeyProvider.Options options = KeyProvider.Options(conf);
			provider.CreateKey(keyName, options);
			provider.Flush();
		}

		protected override string GetTestFile()
		{
			return "testCryptoConf.xml";
		}

		protected override string ExpandCommand(string cmd)
		{
			string expCmd = cmd;
			expCmd = expCmd.ReplaceAll("NAMENODE", namenode);
			expCmd = expCmd.ReplaceAll("#LF#", Runtime.GetProperty("line.separator"));
			expCmd = base.ExpandCommand(expCmd);
			return expCmd;
		}

		protected override CLITestHelper.TestConfigFileParser GetConfigParser()
		{
			return new TestCryptoAdminCLI.TestConfigFileParserCryptoAdmin(this);
		}

		private class TestConfigFileParserCryptoAdmin : CLITestHelper.TestConfigFileParser
		{
			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndElement(string uri, string localName, string qName)
			{
				if (qName.Equals("crypto-admin-command"))
				{
					if (this.testCommands != null)
					{
						this.testCommands.AddItem(new TestCryptoAdminCLI.CLITestCmdCryptoAdmin(this, this
							.charString, new CLICommandCryptoAdmin()));
					}
					else
					{
						if (this.cleanupCommands != null)
						{
							this.cleanupCommands.AddItem(new TestCryptoAdminCLI.CLITestCmdCryptoAdmin(this, this
								.charString, new CLICommandCryptoAdmin()));
						}
					}
				}
				else
				{
					base.EndElement(uri, localName, qName);
				}
			}

			internal TestConfigFileParserCryptoAdmin(TestCryptoAdminCLI _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestCryptoAdminCLI _enclosing;
		}

		private class CLITestCmdCryptoAdmin : CLITestCmd
		{
			public CLITestCmdCryptoAdmin(TestCryptoAdminCLI _enclosing, string str, CLICommandTypes
				 type)
				: base(str, type)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.ArgumentException"/>
			public override CommandExecutor GetExecutor(string tag)
			{
				if (this.GetType() is CLICommandCryptoAdmin)
				{
					return new CryptoAdminCmdExecutor(tag, new CryptoAdmin(this._enclosing.conf));
				}
				return base.GetExecutor(tag);
			}

			private readonly TestCryptoAdminCLI _enclosing;
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
