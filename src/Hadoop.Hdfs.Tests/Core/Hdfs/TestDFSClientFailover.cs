using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Javax.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;
using Sun.Net.Spi.Nameservice;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSClientFailover
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSClientFailover)
			);

		private static readonly Path TestFile = new Path("/tmp/failover-test-file");

		private const int FileLengthToVerify = 100;

		private readonly Configuration conf = new Configuration();

		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUpCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).Build();
			cluster.TransitionToActive(0);
			cluster.WaitActive();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void TearDownCluster()
		{
			cluster.Shutdown();
		}

		[TearDown]
		public virtual void ClearConfig()
		{
			SecurityUtil.SetTokenServiceUseIp(true);
		}

		/// <summary>
		/// Make sure that client failover works when an active NN dies and the standby
		/// takes over.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestDfsClientFailover()
		{
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			DFSTestUtil.CreateFile(fs, TestFile, FileLengthToVerify, (short)1, 1L);
			NUnit.Framework.Assert.AreEqual(fs.GetFileStatus(TestFile).GetLen(), FileLengthToVerify
				);
			cluster.ShutdownNameNode(0);
			cluster.TransitionToActive(1);
			NUnit.Framework.Assert.AreEqual(fs.GetFileStatus(TestFile).GetLen(), FileLengthToVerify
				);
			// Check that it functions even if the URL becomes canonicalized
			// to include a port number.
			Path withPort = new Path("hdfs://" + HATestUtil.GetLogicalHostname(cluster) + ":"
				 + NameNode.DefaultPort + "/" + TestFile.ToUri().GetPath());
			FileSystem fs2 = withPort.GetFileSystem(fs.GetConf());
			NUnit.Framework.Assert.IsTrue(fs2.Exists(withPort));
			fs.Close();
		}

		/// <summary>
		/// Test that even a non-idempotent method will properly fail-over if the
		/// first IPC attempt times out trying to connect.
		/// </summary>
		/// <remarks>
		/// Test that even a non-idempotent method will properly fail-over if the
		/// first IPC attempt times out trying to connect. Regression test for
		/// HDFS-4404.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverOnConnectTimeout()
		{
			conf.SetClass(CommonConfigurationKeysPublic.HadoopRpcSocketFactoryClassDefaultKey
				, typeof(TestDFSClientFailover.InjectingSocketFactory), typeof(SocketFactory));
			// Set up the InjectingSocketFactory to throw a ConnectTimeoutException
			// when connecting to the first NN.
			TestDFSClientFailover.InjectingSocketFactory.portToInjectOn = cluster.GetNameNodePort
				(0);
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			// Make the second NN the active one.
			cluster.ShutdownNameNode(0);
			cluster.TransitionToActive(1);
			// Call a non-idempotent method, and ensure the failover of the call proceeds
			// successfully.
			IOUtils.CloseStream(fs.Create(TestFile));
		}

		private class InjectingSocketFactory : StandardSocketFactory
		{
			internal static readonly SocketFactory defaultFactory = SocketFactory.GetDefault(
				);

			internal static int portToInjectOn;

			/// <exception cref="System.IO.IOException"/>
			public override Socket CreateSocket()
			{
				Socket spy = Org.Mockito.Mockito.Spy(defaultFactory.CreateSocket());
				// Simplify our spying job by not having to also spy on the channel
				Org.Mockito.Mockito.DoReturn(null).When(spy).GetChannel();
				// Throw a ConnectTimeoutException when connecting to our target "bad"
				// host.
				Org.Mockito.Mockito.DoThrow(new ConnectTimeoutException("injected")).When(spy).Connect
					(Org.Mockito.Mockito.ArgThat(new TestDFSClientFailover.InjectingSocketFactory.MatchesPort
					(this)), Org.Mockito.Mockito.AnyInt());
				return spy;
			}

			private class MatchesPort : BaseMatcher<EndPoint>
			{
				public override bool Matches(object arg0)
				{
					return ((IPEndPoint)arg0).Port == TestDFSClientFailover.InjectingSocketFactory.portToInjectOn;
				}

				public override void DescribeTo(Description desc)
				{
					desc.AppendText("matches port " + TestDFSClientFailover.InjectingSocketFactory.portToInjectOn
						);
				}

				internal MatchesPort(InjectingSocketFactory _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly InjectingSocketFactory _enclosing;
			}
		}

		/// <summary>Regression test for HDFS-2683.</summary>
		[NUnit.Framework.Test]
		public virtual void TestLogicalUriShouldNotHavePorts()
		{
			Configuration config = new HdfsConfiguration(conf);
			string logicalName = HATestUtil.GetLogicalHostname(cluster);
			HATestUtil.SetFailoverConfigurations(cluster, config, logicalName);
			Path p = new Path("hdfs://" + logicalName + ":12345/");
			try
			{
				p.GetFileSystem(config).Exists(p);
				NUnit.Framework.Assert.Fail("Did not fail with fake FS");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("does not use port information", ioe);
			}
		}

		/// <summary>
		/// Make sure that a helpful error message is shown if a proxy provider is
		/// configured for a given URI, but no actual addresses are configured for that
		/// URI.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailureWithMisconfiguredHaNNs()
		{
			string logicalHost = "misconfigured-ha-uri";
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + logicalHost
				, typeof(ConfiguredFailoverProxyProvider).FullName);
			URI uri = new URI("hdfs://" + logicalHost + "/test");
			try
			{
				FileSystem.Get(uri, conf).Exists(new Path("/test"));
				NUnit.Framework.Assert.Fail("Successfully got proxy provider for misconfigured FS"
					);
			}
			catch (IOException ioe)
			{
				Log.Info("got expected exception", ioe);
				NUnit.Framework.Assert.IsTrue("expected exception did not contain helpful message"
					, StringUtils.StringifyException(ioe).Contains("Could not find any configured addresses for URI "
					 + uri));
			}
		}

		/// <summary>Spy on the Java DNS infrastructure.</summary>
		/// <remarks>
		/// Spy on the Java DNS infrastructure.
		/// This likely only works on Sun-derived JDKs, but uses JUnit's
		/// Assume functionality so that any tests using it are skipped on
		/// incompatible JDKs.
		/// </remarks>
		private NameService SpyOnNameService()
		{
			try
			{
				FieldInfo f = Sharpen.Runtime.GetDeclaredField(typeof(IPAddress), "nameServices");
				Assume.AssumeNotNull(f);
				IList<NameService> nsList = (IList<NameService>)f.GetValue(null);
				NameService ns = nsList[0];
				Log log = LogFactory.GetLog("NameServiceSpy");
				ns = Org.Mockito.Mockito.Mock<NameService>(new GenericTestUtils.DelegateAnswer(log
					, ns));
				nsList.Set(0, ns);
				return ns;
			}
			catch (Exception t)
			{
				Log.Info("Unable to spy on DNS. Skipping test.", t);
				// In case the JDK we're testing on doesn't work like Sun's, just
				// skip the test.
				Assume.AssumeNoException(t);
				throw new RuntimeException(t);
			}
		}

		/// <summary>Test that the client doesn't ever try to DNS-resolve the logical URI.</summary>
		/// <remarks>
		/// Test that the client doesn't ever try to DNS-resolve the logical URI.
		/// Regression test for HADOOP-9150.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoesntDnsResolveLogicalURI()
		{
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			NameService spyNS = SpyOnNameService();
			string logicalHost = fs.GetUri().GetHost();
			Path qualifiedRoot = fs.MakeQualified(new Path("/"));
			// Make a few calls against the filesystem.
			fs.GetCanonicalServiceName();
			fs.ListStatus(qualifiedRoot);
			// Ensure that the logical hostname was never resolved.
			Org.Mockito.Mockito.Verify(spyNS, Org.Mockito.Mockito.Never()).LookupAllHostAddr(
				Org.Mockito.Mockito.Eq(logicalHost));
		}

		/// <summary>Same test as above, but for FileContext.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileContextDoesntDnsResolveLogicalURI()
		{
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			NameService spyNS = SpyOnNameService();
			string logicalHost = fs.GetUri().GetHost();
			Configuration haClientConf = fs.GetConf();
			FileContext fc = FileContext.GetFileContext(haClientConf);
			Path root = new Path("/");
			fc.ListStatus(root);
			fc.ListStatus(fc.MakeQualified(root));
			fc.GetDefaultFileSystem().GetCanonicalServiceName();
			// Ensure that the logical hostname was never resolved.
			Org.Mockito.Mockito.Verify(spyNS, Org.Mockito.Mockito.Never()).LookupAllHostAddr(
				Org.Mockito.Mockito.Eq(logicalHost));
		}

		/// <summary>Dummy implementation of plain FailoverProxyProvider</summary>
		public class DummyLegacyFailoverProxyProvider<T> : FailoverProxyProvider<T>
		{
			private Type xface;

			private T proxy;

			public DummyLegacyFailoverProxyProvider(Configuration conf, URI uri, Type xface)
			{
				try
				{
					this.proxy = NameNodeProxies.CreateNonHAProxy(conf, NameNode.GetAddress(uri), xface
						, UserGroupInformation.GetCurrentUser(), false).GetProxy();
					this.xface = xface;
				}
				catch (IOException)
				{
				}
			}

			public override Type GetInterface()
			{
				return xface;
			}

			public override FailoverProxyProvider.ProxyInfo<T> GetProxy()
			{
				return new FailoverProxyProvider.ProxyInfo<T>(proxy, "dummy");
			}

			public override void PerformFailover(T currentProxy)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		/// <summary>Test to verify legacy proxy providers are correctly wrapped.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrappedFailoverProxyProvider()
		{
			// setup the config with the dummy provider class
			Configuration config = new HdfsConfiguration(conf);
			string logicalName = HATestUtil.GetLogicalHostname(cluster);
			HATestUtil.SetFailoverConfigurations(cluster, config, logicalName);
			config.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + logicalName
				, typeof(TestDFSClientFailover.DummyLegacyFailoverProxyProvider).FullName);
			Path p = new Path("hdfs://" + logicalName + "/");
			// not to use IP address for token service
			SecurityUtil.SetTokenServiceUseIp(false);
			// Logical URI should be used.
			NUnit.Framework.Assert.IsTrue("Legacy proxy providers should use logical URI.", HAUtil
				.UseLogicalUri(config, p.ToUri()));
		}

		/// <summary>Test to verify IPFailoverProxyProvider is not requiring logical URI.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIPFailoverProxyProviderLogicalUri()
		{
			// setup the config with the IP failover proxy provider class
			Configuration config = new HdfsConfiguration(conf);
			URI nnUri = cluster.GetURI(0);
			config.Set(DFSConfigKeys.DfsClientFailoverProxyProviderKeyPrefix + "." + nnUri.GetHost
				(), typeof(IPFailoverProxyProvider).FullName);
			NUnit.Framework.Assert.IsFalse("IPFailoverProxyProvider should not use logical URI."
				, HAUtil.UseLogicalUri(config, nnUri));
		}
	}
}
