using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestShellCommandFencer
	{
		private org.apache.hadoop.ha.ShellCommandFencer fencer = createFencer();

		private static readonly org.apache.hadoop.ha.HAServiceTarget TEST_TARGET = new org.apache.hadoop.ha.DummyHAService
			(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState.ACTIVE, new java.net.InetSocketAddress
			("dummyhost", 1234));

		[NUnit.Framework.BeforeClass]
		public static void setupLogSpy()
		{
			org.apache.hadoop.ha.ShellCommandFencer.LOG = org.mockito.Mockito.spy(org.apache.hadoop.ha.ShellCommandFencer
				.LOG);
		}

		[NUnit.Framework.SetUp]
		public virtual void resetLogSpy()
		{
			org.mockito.Mockito.reset(org.apache.hadoop.ha.ShellCommandFencer.LOG);
		}

		private static org.apache.hadoop.ha.ShellCommandFencer createFencer()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("in.fencing.tests", "yessir");
			org.apache.hadoop.ha.ShellCommandFencer fencer = new org.apache.hadoop.ha.ShellCommandFencer
				();
			fencer.setConf(conf);
			return fencer;
		}

		/// <summary>
		/// Test that the exit code of the script determines
		/// whether the fencer succeeded or failed
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testBasicSuccessFailure()
		{
			NUnit.Framework.Assert.IsTrue(fencer.tryFence(TEST_TARGET, "echo"));
			NUnit.Framework.Assert.IsFalse(fencer.tryFence(TEST_TARGET, "exit 1"));
			// bad path should also fail
			NUnit.Framework.Assert.IsFalse(fencer.tryFence(TEST_TARGET, "xxxxxxxxxxxx"));
		}

		[NUnit.Framework.Test]
		public virtual void testCheckNoArgs()
		{
			try
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				new org.apache.hadoop.ha.NodeFencer(conf, "shell");
				NUnit.Framework.Assert.Fail("Didn't throw when passing no args to shell");
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException confe)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected exception:" + org.apache.hadoop.util.StringUtils
					.stringifyException(confe), confe.Message.contains("No argument passed"));
			}
		}

		[NUnit.Framework.Test]
		public virtual void testCheckParensNoArgs()
		{
			try
			{
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
					();
				new org.apache.hadoop.ha.NodeFencer(conf, "shell()");
				NUnit.Framework.Assert.Fail("Didn't throw when passing no args to shell");
			}
			catch (org.apache.hadoop.ha.BadFencingConfigurationException confe)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected exception:" + org.apache.hadoop.util.StringUtils
					.stringifyException(confe), confe.Message.contains("Unable to parse line: 'shell()'"
					));
			}
		}

		/// <summary>
		/// Test that lines on stdout get passed as INFO
		/// level messages
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testStdoutLogging()
		{
			NUnit.Framework.Assert.IsTrue(fencer.tryFence(TEST_TARGET, "echo hello"));
			org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).info(org.mockito.Mockito
				.endsWith("echo hello: hello"));
		}

		/// <summary>
		/// Test that lines on stderr get passed as
		/// WARN level log messages
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testStderrLogging()
		{
			NUnit.Framework.Assert.IsTrue(fencer.tryFence(TEST_TARGET, "echo hello>&2"));
			org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).warn(org.mockito.Mockito
				.endsWith("echo hello>&2: hello"));
		}

		/// <summary>
		/// Verify that the Configuration gets passed as
		/// environment variables to the fencer.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testConfAsEnvironment()
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				fencer.tryFence(TEST_TARGET, "echo $in_fencing_tests");
				org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).info(org.mockito.Mockito
					.endsWith("echo $in...ing_tests: yessir"));
			}
			else
			{
				fencer.tryFence(TEST_TARGET, "echo %in_fencing_tests%");
				org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).info(org.mockito.Mockito
					.endsWith("echo %in...ng_tests%: yessir"));
			}
		}

		/// <summary>
		/// Verify that information about the fencing target gets passed as
		/// environment variables to the fencer.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void testTargetAsEnvironment()
		{
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				fencer.tryFence(TEST_TARGET, "echo $target_host $target_port");
				org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).info(org.mockito.Mockito
					.endsWith("echo $ta...rget_port: dummyhost 1234"));
			}
			else
			{
				fencer.tryFence(TEST_TARGET, "echo %target_host% %target_port%");
				org.mockito.Mockito.verify(org.apache.hadoop.ha.ShellCommandFencer.LOG).info(org.mockito.Mockito
					.endsWith("echo %ta...get_port%: dummyhost 1234"));
			}
		}

		/// <summary>
		/// Test that we properly close off our input to the subprocess
		/// such that it knows there's no tty connected.
		/// </summary>
		/// <remarks>
		/// Test that we properly close off our input to the subprocess
		/// such that it knows there's no tty connected. This is important
		/// so that, if we use 'ssh', it won't try to prompt for a password
		/// and block forever, for example.
		/// </remarks>
		public virtual void testSubprocessInputIsClosed()
		{
			NUnit.Framework.Assert.IsFalse(fencer.tryFence(TEST_TARGET, "read"));
		}

		[NUnit.Framework.Test]
		public virtual void testCommandAbbreviation()
		{
			NUnit.Framework.Assert.AreEqual("a...f", org.apache.hadoop.ha.ShellCommandFencer.
				abbreviate("abcdef", 5));
			NUnit.Framework.Assert.AreEqual("abcdef", org.apache.hadoop.ha.ShellCommandFencer
				.abbreviate("abcdef", 6));
			NUnit.Framework.Assert.AreEqual("abcdef", org.apache.hadoop.ha.ShellCommandFencer
				.abbreviate("abcdef", 7));
			NUnit.Framework.Assert.AreEqual("a...g", org.apache.hadoop.ha.ShellCommandFencer.
				abbreviate("abcdefg", 5));
			NUnit.Framework.Assert.AreEqual("a...h", org.apache.hadoop.ha.ShellCommandFencer.
				abbreviate("abcdefgh", 5));
			NUnit.Framework.Assert.AreEqual("a...gh", org.apache.hadoop.ha.ShellCommandFencer
				.abbreviate("abcdefgh", 6));
			NUnit.Framework.Assert.AreEqual("ab...gh", org.apache.hadoop.ha.ShellCommandFencer
				.abbreviate("abcdefgh", 7));
		}
	}
}
