using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestSshFenceByTcpPort
	{
		static TestSshFenceByTcpPort()
		{
			((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.ha.SshFenceByTcpPort
				.LOG).getLogger().setLevel(org.apache.log4j.Level.ALL);
		}

		private static string TEST_FENCING_HOST = Sharpen.Runtime.getProperty("test.TestSshFenceByTcpPort.host"
			, "localhost");

		private static readonly string TEST_FENCING_PORT = Sharpen.Runtime.getProperty("test.TestSshFenceByTcpPort.port"
			, "8020");

		private static readonly string TEST_KEYFILE = Sharpen.Runtime.getProperty("test.TestSshFenceByTcpPort.key"
			);

		private static readonly java.net.InetSocketAddress TEST_ADDR = new java.net.InetSocketAddress
			(TEST_FENCING_HOST, int.Parse(TEST_FENCING_PORT));

		private static readonly org.apache.hadoop.ha.HAServiceTarget TEST_TARGET = new org.apache.hadoop.ha.DummyHAService
			(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, TEST_ADDR);

		/// <summary>Connect to Google's DNS server - not running ssh!</summary>
		private static readonly org.apache.hadoop.ha.HAServiceTarget UNFENCEABLE_TARGET = 
			new org.apache.hadoop.ha.DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
			.ACTIVE, new java.net.InetSocketAddress("8.8.8.8", 1234));

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public virtual void testFence()
		{
			NUnit.Framework.Assume.assumeTrue(isConfigured());
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.ha.SshFenceByTcpPort.CONF_IDENTITIES_KEY, TEST_KEYFILE
				);
			org.apache.hadoop.ha.SshFenceByTcpPort fence = new org.apache.hadoop.ha.SshFenceByTcpPort
				();
			fence.setConf(conf);
			NUnit.Framework.Assert.IsTrue(fence.tryFence(TEST_TARGET, null));
		}

		/// <summary>Test connecting to a host which definitely won't respond.</summary>
		/// <remarks>
		/// Test connecting to a host which definitely won't respond.
		/// Make sure that it times out and returns false, but doesn't throw
		/// any exception
		/// </remarks>
		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public virtual void testConnectTimeout()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt(org.apache.hadoop.ha.SshFenceByTcpPort.CONF_CONNECT_TIMEOUT_KEY, 3000
				);
			org.apache.hadoop.ha.SshFenceByTcpPort fence = new org.apache.hadoop.ha.SshFenceByTcpPort
				();
			fence.setConf(conf);
			NUnit.Framework.Assert.IsFalse(fence.tryFence(UNFENCEABLE_TARGET, string.Empty));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testArgsParsing()
		{
			org.apache.hadoop.ha.SshFenceByTcpPort.Args args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args
				(null);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getProperty("user.name"), args.user
				);
			NUnit.Framework.Assert.AreEqual(22, args.sshPort);
			args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args(string.Empty);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getProperty("user.name"), args.user
				);
			NUnit.Framework.Assert.AreEqual(22, args.sshPort);
			args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args("12345");
			NUnit.Framework.Assert.AreEqual("12345", args.user);
			NUnit.Framework.Assert.AreEqual(22, args.sshPort);
			args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args(":12345");
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getProperty("user.name"), args.user
				);
			NUnit.Framework.Assert.AreEqual(12345, args.sshPort);
			args = new org.apache.hadoop.ha.SshFenceByTcpPort.Args("foo:2222");
			NUnit.Framework.Assert.AreEqual("foo", args.user);
			NUnit.Framework.Assert.AreEqual(2222, args.sshPort);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testBadArgsParsing()
		{
			assertBadArgs(":");
			// No port specified
			assertBadArgs("bar.com:");
			// "
			assertBadArgs(":xx");
			// Port does not parse
			assertBadArgs("bar.com:xx");
		}

		// "
		private void assertBadArgs(string argStr)
		{
			try
			{
				new org.apache.hadoop.ha.SshFenceByTcpPort.Args(argStr);
				NUnit.Framework.Assert.Fail("Did not fail on bad args: " + argStr);
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException)
			{
			}
		}

		// Expected
		private bool isConfigured()
		{
			return (TEST_FENCING_HOST != null && !TEST_FENCING_HOST.isEmpty()) && (TEST_FENCING_PORT
				 != null && !TEST_FENCING_PORT.isEmpty()) && (TEST_KEYFILE != null && !TEST_KEYFILE
				.isEmpty());
		}
	}
}
