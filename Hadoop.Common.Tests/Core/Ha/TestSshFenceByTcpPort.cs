using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestSshFenceByTcpPort
	{
		static TestSshFenceByTcpPort()
		{
			((Log4JLogger)SshFenceByTcpPort.Log).GetLogger().SetLevel(Level.All);
		}

		private static string TestFencingHost = Runtime.GetProperty("test.TestSshFenceByTcpPort.host"
			, "localhost");

		private static readonly string TestFencingPort = Runtime.GetProperty("test.TestSshFenceByTcpPort.port"
			, "8020");

		private static readonly string TestKeyfile = Runtime.GetProperty("test.TestSshFenceByTcpPort.key"
			);

		private static readonly IPEndPoint TestAddr = new IPEndPoint(TestFencingHost, Sharpen.Extensions.ValueOf
			(TestFencingPort));

		private static readonly HAServiceTarget TestTarget = new DummyHAService(HAServiceProtocol.HAServiceState
			.Active, TestAddr);

		/// <summary>Connect to Google's DNS server - not running ssh!</summary>
		private static readonly HAServiceTarget UnfenceableTarget = new DummyHAService(HAServiceProtocol.HAServiceState
			.Active, new IPEndPoint("8.8.8.8", 1234));

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public virtual void TestFence()
		{
			Assume.AssumeTrue(IsConfigured());
			Configuration conf = new Configuration();
			conf.Set(SshFenceByTcpPort.ConfIdentitiesKey, TestKeyfile);
			SshFenceByTcpPort fence = new SshFenceByTcpPort();
			fence.SetConf(conf);
			Assert.True(fence.TryFence(TestTarget, null));
		}

		/// <summary>Test connecting to a host which definitely won't respond.</summary>
		/// <remarks>
		/// Test connecting to a host which definitely won't respond.
		/// Make sure that it times out and returns false, but doesn't throw
		/// any exception
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public virtual void TestConnectTimeout()
		{
			Configuration conf = new Configuration();
			conf.SetInt(SshFenceByTcpPort.ConfConnectTimeoutKey, 3000);
			SshFenceByTcpPort fence = new SshFenceByTcpPort();
			fence.SetConf(conf);
			NUnit.Framework.Assert.IsFalse(fence.TryFence(UnfenceableTarget, string.Empty));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestArgsParsing()
		{
			SshFenceByTcpPort.Args args = new SshFenceByTcpPort.Args(null);
			Assert.Equal(Runtime.GetProperty("user.name"), args.user);
			Assert.Equal(22, args.sshPort);
			args = new SshFenceByTcpPort.Args(string.Empty);
			Assert.Equal(Runtime.GetProperty("user.name"), args.user);
			Assert.Equal(22, args.sshPort);
			args = new SshFenceByTcpPort.Args("12345");
			Assert.Equal("12345", args.user);
			Assert.Equal(22, args.sshPort);
			args = new SshFenceByTcpPort.Args(":12345");
			Assert.Equal(Runtime.GetProperty("user.name"), args.user);
			Assert.Equal(12345, args.sshPort);
			args = new SshFenceByTcpPort.Args("foo:2222");
			Assert.Equal("foo", args.user);
			Assert.Equal(2222, args.sshPort);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestBadArgsParsing()
		{
			AssertBadArgs(":");
			// No port specified
			AssertBadArgs("bar.com:");
			// "
			AssertBadArgs(":xx");
			// Port does not parse
			AssertBadArgs("bar.com:xx");
		}

		// "
		private void AssertBadArgs(string argStr)
		{
			try
			{
				new SshFenceByTcpPort.Args(argStr);
				NUnit.Framework.Assert.Fail("Did not fail on bad args: " + argStr);
			}
			catch (BadFencingConfigurationException)
			{
			}
		}

		// Expected
		private bool IsConfigured()
		{
			return (TestFencingHost != null && !TestFencingHost.IsEmpty()) && (TestFencingPort
				 != null && !TestFencingPort.IsEmpty()) && (TestKeyfile != null && !TestKeyfile.
				IsEmpty());
		}
	}
}
