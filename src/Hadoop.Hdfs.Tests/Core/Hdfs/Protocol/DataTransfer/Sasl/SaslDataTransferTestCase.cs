using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Ssl;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	public abstract class SaslDataTransferTestCase
	{
		private static FilePath baseDir;

		private static string hdfsPrincipal;

		private static MiniKdc kdc;

		private static string keytab;

		private static string spnegoPrincipal;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void InitKdc()
		{
			baseDir = new FilePath(Runtime.GetProperty("test.build.dir", "target/test-dir"), 
				typeof(SaslDataTransferTestCase).Name);
			FileUtil.FullyDelete(baseDir);
			NUnit.Framework.Assert.IsTrue(baseDir.Mkdirs());
			Properties kdcConf = MiniKdc.CreateConf();
			kdc = new MiniKdc(kdcConf, baseDir);
			kdc.Start();
			string userName = UserGroupInformation.GetLoginUser().GetShortUserName();
			FilePath keytabFile = new FilePath(baseDir, userName + ".keytab");
			keytab = keytabFile.GetAbsolutePath();
			kdc.CreatePrincipal(keytabFile, userName + "/localhost", "HTTP/localhost");
			hdfsPrincipal = userName + "/localhost@" + kdc.GetRealm();
			spnegoPrincipal = "HTTP/localhost@" + kdc.GetRealm();
		}

		[AfterClass]
		public static void ShutdownKdc()
		{
			if (kdc != null)
			{
				kdc.Stop();
			}
			FileUtil.FullyDelete(baseDir);
		}

		/// <summary>Creates configuration for starting a secure cluster.</summary>
		/// <param name="dataTransferProtection">supported QOPs</param>
		/// <returns>configuration for starting a secure cluster</returns>
		/// <exception cref="System.Exception">if there is any failure</exception>
		protected internal virtual HdfsConfiguration CreateSecureConfig(string dataTransferProtection
			)
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, conf);
			conf.Set(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, hdfsPrincipal);
			conf.Set(DFSConfigKeys.DfsNamenodeKeytabFileKey, keytab);
			conf.Set(DFSConfigKeys.DfsDatanodeKerberosPrincipalKey, hdfsPrincipal);
			conf.Set(DFSConfigKeys.DfsDatanodeKeytabFileKey, keytab);
			conf.Set(DFSConfigKeys.DfsWebAuthenticationKerberosPrincipalKey, spnegoPrincipal);
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			conf.Set(DFSConfigKeys.DfsDataTransferProtectionKey, dataTransferProtection);
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpsAddressKey, "localhost:0");
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslKey, 10);
			string keystoresDir = baseDir.GetAbsolutePath();
			string sslConfDir = KeyStoreTestUtil.GetClasspathDir(this.GetType());
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
			return conf;
		}
	}
}
