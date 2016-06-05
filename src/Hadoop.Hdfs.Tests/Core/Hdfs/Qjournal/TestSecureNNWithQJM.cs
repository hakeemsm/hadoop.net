using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public class TestSecureNNWithQJM
	{
		private static readonly Path TestPath = new Path("/test-dir");

		private static readonly Path TestPath2 = new Path("/test-dir-2");

		private static HdfsConfiguration baseConf;

		private static FilePath baseDir;

		private static MiniKdc kdc;

		private MiniDFSCluster cluster;

		private HdfsConfiguration conf;

		private FileSystem fs;

		private MiniJournalCluster mjc;

		[Rule]
		public Timeout timeout = new Timeout(30000);

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			baseDir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"), 
				typeof(TestSecureNNWithQJM).Name);
			FileUtil.FullyDelete(baseDir);
			NUnit.Framework.Assert.IsTrue(baseDir.Mkdirs());
			Properties kdcConf = MiniKdc.CreateConf();
			kdc = new MiniKdc(kdcConf, baseDir);
			kdc.Start();
			baseConf = new HdfsConfiguration();
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, baseConf);
			UserGroupInformation.SetConfiguration(baseConf);
			NUnit.Framework.Assert.IsTrue("Expected configuration to enable security", UserGroupInformation
				.IsSecurityEnabled());
			string userName = UserGroupInformation.GetLoginUser().GetShortUserName();
			FilePath keytabFile = new FilePath(baseDir, userName + ".keytab");
			string keytab = keytabFile.GetAbsolutePath();
			// Windows will not reverse name lookup "127.0.0.1" to "localhost".
			string krbInstance = Path.Windows ? "127.0.0.1" : "localhost";
			kdc.CreatePrincipal(keytabFile, userName + "/" + krbInstance, "HTTP/" + krbInstance
				);
			string hdfsPrincipal = userName + "/" + krbInstance + "@" + kdc.GetRealm();
			string spnegoPrincipal = "HTTP/" + krbInstance + "@" + kdc.GetRealm();
			baseConf.Set(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, hdfsPrincipal);
			baseConf.Set(DFSConfigKeys.DfsNamenodeKeytabFileKey, keytab);
			baseConf.Set(DFSConfigKeys.DfsDatanodeKerberosPrincipalKey, hdfsPrincipal);
			baseConf.Set(DFSConfigKeys.DfsDatanodeKeytabFileKey, keytab);
			baseConf.Set(DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey, spnegoPrincipal
				);
			baseConf.Set(DFSConfigKeys.DfsJournalnodeKeytabFileKey, keytab);
			baseConf.Set(DFSConfigKeys.DfsJournalnodeKerberosPrincipalKey, hdfsPrincipal);
			baseConf.Set(DFSConfigKeys.DfsJournalnodeKerberosInternalSpnegoPrincipalKey, spnegoPrincipal
				);
			baseConf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			baseConf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, "authentication");
			baseConf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString
				());
			baseConf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
			baseConf.Set(DFSConfigKeys.DfsDatanodeHttpsAddressKey, "localhost:0");
			baseConf.Set(DFSConfigKeys.DfsJournalnodeHttpsAddressKey, "localhost:0");
			baseConf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslKey, 10);
			string keystoresDir = baseDir.GetAbsolutePath();
			string sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestSecureNNWithQJM));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, baseConf, false);
		}

		[AfterClass]
		public static void Destroy()
		{
			if (kdc != null)
			{
				kdc.Stop();
			}
			FileUtil.FullyDelete(baseDir);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new HdfsConfiguration(baseConf);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Shutdown()
		{
			IOUtils.Cleanup(null, fs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			if (mjc != null)
			{
				mjc.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecureMode()
		{
			DoNNWithQJMTest();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecondaryNameNodeHttpAddressNotNeeded()
		{
			conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "null");
			DoNNWithQJMTest();
		}

		/// <summary>Tests use of QJM with the defined cluster.</summary>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private void DoNNWithQJMTest()
		{
			StartCluster();
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(TestPath));
			// Restart the NN and make sure the edit was persisted
			// and loaded again
			RestartNameNode();
			NUnit.Framework.Assert.IsTrue(fs.Exists(TestPath));
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(TestPath2));
			// Restart the NN again and make sure both edits are persisted.
			RestartNameNode();
			NUnit.Framework.Assert.IsTrue(fs.Exists(TestPath));
			NUnit.Framework.Assert.IsTrue(fs.Exists(TestPath2));
		}

		/// <summary>Restarts the NameNode and obtains a new FileSystem.</summary>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private void RestartNameNode()
		{
			IOUtils.Cleanup(null, fs);
			cluster.RestartNameNode();
			fs = cluster.GetFileSystem();
		}

		/// <summary>Starts a cluster using QJM with the defined configuration.</summary>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		private void StartCluster()
		{
			mjc = new MiniJournalCluster.Builder(conf).Build();
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, mjc.GetQuorumJournalURI("myjournal"
				).ToString());
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}
	}
}
