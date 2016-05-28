using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This test checks that the NameNode respects the following keys:
	/// - DFS_NAMENODE_RPC_BIND_HOST_KEY
	/// - DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY
	/// - DFS_NAMENODE_HTTP_BIND_HOST_KEY
	/// - DFS_NAMENODE_HTTPS_BIND_HOST_KEY
	/// </summary>
	public class TestNameNodeRespectsBindHostKeys
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestNameNodeRespectsBindHostKeys
			));

		private const string WildcardAddress = "0.0.0.0";

		private const string LocalhostServerAddress = "127.0.0.1:0";

		private static string GetRpcServerAddress(MiniDFSCluster cluster)
		{
			NameNodeRpcServer rpcServer = (NameNodeRpcServer)cluster.GetNameNodeRpc();
			return rpcServer.GetClientRpcServer().GetListenerAddress().Address.ToString();
		}

		private static string GetServiceRpcServerAddress(MiniDFSCluster cluster)
		{
			NameNodeRpcServer rpcServer = (NameNodeRpcServer)cluster.GetNameNodeRpc();
			return rpcServer.GetServiceRpcServer().GetListenerAddress().Address.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRpcBindHostKey()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Log.Info("Testing without " + DfsNamenodeRpcBindHostKey);
			// NN should not bind the wildcard address by default.
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = GetRpcServerAddress(cluster);
				Assert.AssertThat("Bind address not expected to be wildcard by default.", address
					, IsNot.Not("/" + WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
			Log.Info("Testing with " + DfsNamenodeRpcBindHostKey);
			// Tell NN to bind the wildcard address.
			conf.Set(DfsNamenodeRpcBindHostKey, WildcardAddress);
			// Verify that NN binds wildcard address now.
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = GetRpcServerAddress(cluster);
				Assert.AssertThat("Bind address " + address + " is not wildcard.", address, IS.Is
					("/" + WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestServiceRpcBindHostKey()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Log.Info("Testing without " + DfsNamenodeServiceRpcBindHostKey);
			conf.Set(DfsNamenodeServiceRpcAddressKey, LocalhostServerAddress);
			// NN should not bind the wildcard address by default.
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = GetServiceRpcServerAddress(cluster);
				Assert.AssertThat("Bind address not expected to be wildcard by default.", address
					, IsNot.Not("/" + WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
			Log.Info("Testing with " + DfsNamenodeServiceRpcBindHostKey);
			// Tell NN to bind the wildcard address.
			conf.Set(DfsNamenodeServiceRpcBindHostKey, WildcardAddress);
			// Verify that NN binds wildcard address now.
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = GetServiceRpcServerAddress(cluster);
				Assert.AssertThat("Bind address " + address + " is not wildcard.", address, IS.Is
					("/" + WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHttpBindHostKey()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Log.Info("Testing without " + DfsNamenodeHttpBindHostKey);
			// NN should not bind the wildcard address by default.
			try
			{
				conf.Set(DfsNamenodeHttpAddressKey, LocalhostServerAddress);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = cluster.GetNameNode().GetHttpAddress().ToString();
				NUnit.Framework.Assert.IsFalse("HTTP Bind address not expected to be wildcard by default."
					, address.StartsWith(WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
			Log.Info("Testing with " + DfsNamenodeHttpBindHostKey);
			// Tell NN to bind the wildcard address.
			conf.Set(DfsNamenodeHttpBindHostKey, WildcardAddress);
			// Verify that NN binds wildcard address now.
			try
			{
				conf.Set(DfsNamenodeHttpAddressKey, LocalhostServerAddress);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = cluster.GetNameNode().GetHttpAddress().ToString();
				NUnit.Framework.Assert.IsTrue("HTTP Bind address " + address + " is not wildcard."
					, address.StartsWith(WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestNameNodeRespectsBindHostKeys).Name;

		/// <exception cref="System.Exception"/>
		private static void SetupSsl()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			conf.Set(DFSConfigKeys.DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString());
			conf.Set(DFSConfigKeys.DfsNamenodeHttpsAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpsAddressKey, "localhost:0");
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			NUnit.Framework.Assert.IsTrue(@base.Mkdirs());
			string keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			string sslConfDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestNameNodeRespectsBindHostKeys
				));
			KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfDir, conf, false);
		}

		/// <summary>HTTPS test is different since we need to setup SSL configuration.</summary>
		/// <remarks>
		/// HTTPS test is different since we need to setup SSL configuration.
		/// NN also binds the wildcard address for HTTPS port by default so we must
		/// pick a different host/port combination.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestHttpsBindHostKey()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			Log.Info("Testing behavior without " + DfsNamenodeHttpsBindHostKey);
			SetupSsl();
			conf.Set(DfsHttpPolicyKey, HttpConfig.Policy.HttpsOnly.ToString());
			// NN should not bind the wildcard address by default.
			try
			{
				conf.Set(DfsNamenodeHttpsAddressKey, LocalhostServerAddress);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = cluster.GetNameNode().GetHttpsAddress().ToString();
				NUnit.Framework.Assert.IsFalse("HTTP Bind address not expected to be wildcard by default."
					, address.StartsWith(WildcardAddress));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
			Log.Info("Testing behavior with " + DfsNamenodeHttpsBindHostKey);
			// Tell NN to bind the wildcard address.
			conf.Set(DfsNamenodeHttpsBindHostKey, WildcardAddress);
			// Verify that NN binds wildcard address now.
			try
			{
				conf.Set(DfsNamenodeHttpsAddressKey, LocalhostServerAddress);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				string address = cluster.GetNameNode().GetHttpsAddress().ToString();
				NUnit.Framework.Assert.IsTrue("HTTP Bind address " + address + " is not wildcard."
					, address.StartsWith(WildcardAddress));
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
