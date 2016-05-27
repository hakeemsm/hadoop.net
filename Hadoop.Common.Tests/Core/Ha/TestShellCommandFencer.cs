using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestShellCommandFencer
	{
		private ShellCommandFencer fencer = CreateFencer();

		private static readonly HAServiceTarget TestTarget = new DummyHAService(HAServiceProtocol.HAServiceState
			.Active, new IPEndPoint("dummyhost", 1234));

		[BeforeClass]
		public static void SetupLogSpy()
		{
			ShellCommandFencer.Log = Org.Mockito.Mockito.Spy(ShellCommandFencer.Log);
		}

		[SetUp]
		public virtual void ResetLogSpy()
		{
			Org.Mockito.Mockito.Reset(ShellCommandFencer.Log);
		}

		private static ShellCommandFencer CreateFencer()
		{
			Configuration conf = new Configuration();
			conf.Set("in.fencing.tests", "yessir");
			ShellCommandFencer fencer = new ShellCommandFencer();
			fencer.SetConf(conf);
			return fencer;
		}

		/// <summary>
		/// Test that the exit code of the script determines
		/// whether the fencer succeeded or failed
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestBasicSuccessFailure()
		{
			NUnit.Framework.Assert.IsTrue(fencer.TryFence(TestTarget, "echo"));
			NUnit.Framework.Assert.IsFalse(fencer.TryFence(TestTarget, "exit 1"));
			// bad path should also fail
			NUnit.Framework.Assert.IsFalse(fencer.TryFence(TestTarget, "xxxxxxxxxxxx"));
		}

		[NUnit.Framework.Test]
		public virtual void TestCheckNoArgs()
		{
			try
			{
				Configuration conf = new Configuration();
				new NodeFencer(conf, "shell");
				NUnit.Framework.Assert.Fail("Didn't throw when passing no args to shell");
			}
			catch (BadFencingConfigurationException confe)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected exception:" + StringUtils.StringifyException
					(confe), confe.Message.Contains("No argument passed"));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCheckParensNoArgs()
		{
			try
			{
				Configuration conf = new Configuration();
				new NodeFencer(conf, "shell()");
				NUnit.Framework.Assert.Fail("Didn't throw when passing no args to shell");
			}
			catch (BadFencingConfigurationException confe)
			{
				NUnit.Framework.Assert.IsTrue("Unexpected exception:" + StringUtils.StringifyException
					(confe), confe.Message.Contains("Unable to parse line: 'shell()'"));
			}
		}

		/// <summary>
		/// Test that lines on stdout get passed as INFO
		/// level messages
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestStdoutLogging()
		{
			NUnit.Framework.Assert.IsTrue(fencer.TryFence(TestTarget, "echo hello"));
			Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Info(Org.Mockito.Mockito.EndsWith
				("echo hello: hello"));
		}

		/// <summary>
		/// Test that lines on stderr get passed as
		/// WARN level log messages
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestStderrLogging()
		{
			NUnit.Framework.Assert.IsTrue(fencer.TryFence(TestTarget, "echo hello>&2"));
			Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Warn(Org.Mockito.Mockito.EndsWith
				("echo hello>&2: hello"));
		}

		/// <summary>
		/// Verify that the Configuration gets passed as
		/// environment variables to the fencer.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestConfAsEnvironment()
		{
			if (!Shell.Windows)
			{
				fencer.TryFence(TestTarget, "echo $in_fencing_tests");
				Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Info(Org.Mockito.Mockito.EndsWith
					("echo $in...ing_tests: yessir"));
			}
			else
			{
				fencer.TryFence(TestTarget, "echo %in_fencing_tests%");
				Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Info(Org.Mockito.Mockito.EndsWith
					("echo %in...ng_tests%: yessir"));
			}
		}

		/// <summary>
		/// Verify that information about the fencing target gets passed as
		/// environment variables to the fencer.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestTargetAsEnvironment()
		{
			if (!Shell.Windows)
			{
				fencer.TryFence(TestTarget, "echo $target_host $target_port");
				Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Info(Org.Mockito.Mockito.EndsWith
					("echo $ta...rget_port: dummyhost 1234"));
			}
			else
			{
				fencer.TryFence(TestTarget, "echo %target_host% %target_port%");
				Org.Mockito.Mockito.Verify(ShellCommandFencer.Log).Info(Org.Mockito.Mockito.EndsWith
					("echo %ta...get_port%: dummyhost 1234"));
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
		public virtual void TestSubprocessInputIsClosed()
		{
			NUnit.Framework.Assert.IsFalse(fencer.TryFence(TestTarget, "read"));
		}

		[NUnit.Framework.Test]
		public virtual void TestCommandAbbreviation()
		{
			NUnit.Framework.Assert.AreEqual("a...f", ShellCommandFencer.Abbreviate("abcdef", 
				5));
			NUnit.Framework.Assert.AreEqual("abcdef", ShellCommandFencer.Abbreviate("abcdef", 
				6));
			NUnit.Framework.Assert.AreEqual("abcdef", ShellCommandFencer.Abbreviate("abcdef", 
				7));
			NUnit.Framework.Assert.AreEqual("a...g", ShellCommandFencer.Abbreviate("abcdefg", 
				5));
			NUnit.Framework.Assert.AreEqual("a...h", ShellCommandFencer.Abbreviate("abcdefgh"
				, 5));
			NUnit.Framework.Assert.AreEqual("a...gh", ShellCommandFencer.Abbreviate("abcdefgh"
				, 6));
			NUnit.Framework.Assert.AreEqual("ab...gh", ShellCommandFencer.Abbreviate("abcdefgh"
				, 7));
		}
	}
}
