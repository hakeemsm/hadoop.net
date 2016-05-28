using System;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security
{
	public class TestDelegationTokenForProxyUser
	{
		private static MiniDFSCluster cluster;

		private static Configuration config;

		private const string Group1Name = "group1";

		private const string Group2Name = "group2";

		private static readonly string[] GroupNames = new string[] { Group1Name, Group2Name
			 };

		private const string RealUser = "RealUser";

		private const string ProxyUser = "ProxyUser";

		private static UserGroupInformation ugi;

		private static UserGroupInformation proxyUgi;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDoAsEffectiveUser)
			);

		/// <exception cref="System.IO.IOException"/>
		private static void ConfigureSuperUserIPAddresses(Configuration conf, string superUserShortName
			)
		{
			AList<string> ipList = new AList<string>();
			Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.GetNetworkInterfaces
				();
			while (netInterfaceList.MoveNext())
			{
				NetworkInterface inf = netInterfaceList.Current;
				Enumeration<IPAddress> addrList = inf.GetInetAddresses();
				while (addrList.MoveNext())
				{
					IPAddress addr = addrList.Current;
					ipList.AddItem(addr.GetHostAddress());
				}
			}
			StringBuilder builder = new StringBuilder();
			foreach (string ip in ipList)
			{
				builder.Append(ip);
				builder.Append(',');
			}
			builder.Append("127.0.1.1,");
			builder.Append(Sharpen.Runtime.GetLocalHost().ToString());
			Log.Info("Local Ip addresses: " + builder.ToString());
			conf.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(superUserShortName), builder.ToString());
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			config = new HdfsConfiguration();
			config.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			config.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeKey, 10000);
			config.SetLong(DFSConfigKeys.DfsNamenodeDelegationTokenRenewIntervalKey, 5000);
			config.SetStrings(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(RealUser), "group1");
			config.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			ConfigureSuperUserIPAddresses(config, RealUser);
			FileSystem.SetDefaultUri(config, "hdfs://localhost:" + "0");
			cluster = new MiniDFSCluster.Builder(config).Build();
			cluster.WaitActive();
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			ugi = UserGroupInformation.CreateRemoteUser(RealUser);
			proxyUgi = UserGroupInformation.CreateProxyUserForTesting(ProxyUser, ugi, GroupNames
				);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDelegationTokenWithRealUser()
		{
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = proxyUgi.DoAs(new _PrivilegedExceptionAction_131
					());
				DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
				byte[] tokenId = tokens[0].GetIdentifier();
				identifier.ReadFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
				NUnit.Framework.Assert.AreEqual(identifier.GetUser().GetUserName(), ProxyUser);
				NUnit.Framework.Assert.AreEqual(identifier.GetUser().GetRealUser().GetUserName(), 
					RealUser);
			}
			catch (Exception)
			{
			}
		}

		private sealed class _PrivilegedExceptionAction_131 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<object>[]>
		{
			public _PrivilegedExceptionAction_131()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Org.Apache.Hadoop.Security.Token.Token<object>[] Run()
			{
				return TestDelegationTokenForProxyUser.cluster.GetFileSystem().AddDelegationTokens
					("RenewerUser", null);
			}
		}

		//Do Nothing
		/// <exception cref="System.Exception"/>
		public virtual void TestWebHdfsDoAs()
		{
			WebHdfsTestUtil.Log.Info("START: testWebHdfsDoAs()");
			WebHdfsTestUtil.Log.Info("ugi.getShortUserName()=" + ugi.GetShortUserName());
			WebHdfsFileSystem webhdfs = WebHdfsTestUtil.GetWebHdfsFileSystemAs(ugi, config, WebHdfsFileSystem
				.Scheme);
			Path root = new Path("/");
			cluster.GetFileSystem().SetPermission(root, new FsPermission((short)0x1ff));
			Whitebox.SetInternalState(webhdfs, "ugi", proxyUgi);
			{
				Path responsePath = webhdfs.GetHomeDirectory();
				WebHdfsTestUtil.Log.Info("responsePath=" + responsePath);
				NUnit.Framework.Assert.AreEqual(webhdfs.GetUri() + "/user/" + ProxyUser, responsePath
					.ToString());
			}
			Path f = new Path("/testWebHdfsDoAs/a.txt");
			{
				FSDataOutputStream @out = webhdfs.Create(f);
				@out.Write(Sharpen.Runtime.GetBytesForString("Hello, webhdfs user!"));
				@out.Close();
				FileStatus status = webhdfs.GetFileStatus(f);
				WebHdfsTestUtil.Log.Info("status.getOwner()=" + status.GetOwner());
				NUnit.Framework.Assert.AreEqual(ProxyUser, status.GetOwner());
			}
			{
				FSDataOutputStream @out = webhdfs.Append(f);
				@out.Write(Sharpen.Runtime.GetBytesForString("\nHello again!"));
				@out.Close();
				FileStatus status = webhdfs.GetFileStatus(f);
				WebHdfsTestUtil.Log.Info("status.getOwner()=" + status.GetOwner());
				WebHdfsTestUtil.Log.Info("status.getLen()  =" + status.GetLen());
				NUnit.Framework.Assert.AreEqual(ProxyUser, status.GetOwner());
			}
		}
	}
}
