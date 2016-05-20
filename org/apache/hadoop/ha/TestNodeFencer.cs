using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestNodeFencer
	{
		private org.apache.hadoop.ha.HAServiceTarget MOCK_TARGET;

		private static string FENCER_TRUE_COMMAND_UNIX = "shell(true)";

		private static string FENCER_TRUE_COMMAND_WINDOWS = "shell(rem)";

		// Fencer shell commands that always return true on Unix and Windows
		// respectively. Lacking the POSIX 'true' command on Windows, we use
		// the batch command 'rem'.
		[NUnit.Framework.SetUp]
		public virtual void clearMockState()
		{
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer.callArgs.clear();
			org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer.callArgs.clear();
			MOCK_TARGET = org.mockito.Mockito.mock<org.apache.hadoop.ha.HAServiceTarget>();
			org.mockito.Mockito.doReturn("my mock").when(MOCK_TARGET).ToString();
			org.mockito.Mockito.doReturn(new java.net.InetSocketAddress("host", 1234)).when(MOCK_TARGET
				).getAddress();
		}

		private static string getFencerTrueCommand()
		{
			return org.apache.hadoop.util.Shell.WINDOWS ? FENCER_TRUE_COMMAND_WINDOWS : FENCER_TRUE_COMMAND_UNIX;
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testSingleFencer()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName() + "(foo)"
				);
			NUnit.Framework.Assert.IsTrue(fencer.fence(MOCK_TARGET));
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(MOCK_TARGET, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual("foo", org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.callArgs[0]);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleFencers()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName() + "(foo)\n"
				 + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				)).getName() + "(bar)\n");
			NUnit.Framework.Assert.IsTrue(fencer.fence(MOCK_TARGET));
			// Only one call, since the first fencer succeeds
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreEqual("foo", org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.callArgs[0]);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testWhitespaceAndCommentsInConfig()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer("\n" + " # the next one will always fail\n"
				 + " " + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				)).getName() + "(foo) # <- fails\n" + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				)).getName() + "(bar) \n");
			NUnit.Framework.Assert.IsTrue(fencer.fence(MOCK_TARGET));
			// One call to each, since top fencer fails
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(MOCK_TARGET, org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(MOCK_TARGET, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual("foo", org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer
				.callArgs[0]);
			NUnit.Framework.Assert.AreEqual("bar", org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.callArgs[0]);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testArglessFencer()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer)).getName());
			NUnit.Framework.Assert.IsTrue(fencer.fence(MOCK_TARGET));
			// One call to each, since top fencer fails
			NUnit.Framework.Assert.AreEqual(1, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fenceCalled);
			NUnit.Framework.Assert.AreSame(MOCK_TARGET, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.fencedSvc);
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer
				.callArgs[0]);
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testShortNameShell()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer(getFencerTrueCommand());
			NUnit.Framework.Assert.IsTrue(fencer.fence(MOCK_TARGET));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testShortNameSsh()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer("sshfence");
			NUnit.Framework.Assert.IsFalse(fencer.fence(MOCK_TARGET));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testShortNameSshWithUser()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer("sshfence(user)");
			NUnit.Framework.Assert.IsFalse(fencer.fence(MOCK_TARGET));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testShortNameSshWithPort()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer("sshfence(:123)");
			NUnit.Framework.Assert.IsFalse(fencer.fence(MOCK_TARGET));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		[NUnit.Framework.Test]
		public virtual void testShortNameSshWithUserPort()
		{
			org.apache.hadoop.ha.NodeFencer fencer = setupFencer("sshfence(user:123)");
			NUnit.Framework.Assert.IsFalse(fencer.fence(MOCK_TARGET));
		}

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public static org.apache.hadoop.ha.NodeFencer setupFencer(string confStr)
		{
			System.Console.Error.WriteLine("Testing configuration:\n" + confStr);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			return new org.apache.hadoop.ha.NodeFencer(conf, confStr);
		}

		/// <summary>Mock fencing method that always returns true</summary>
		public class AlwaysSucceedFencer : org.apache.hadoop.conf.Configured, org.apache.hadoop.ha.FenceMethod
		{
			internal static int fenceCalled = 0;

			internal static org.apache.hadoop.ha.HAServiceTarget fencedSvc;

			internal static System.Collections.Generic.IList<string> callArgs = com.google.common.collect.Lists
				.newArrayList();

			public virtual bool tryFence(org.apache.hadoop.ha.HAServiceTarget target, string 
				args)
			{
				fencedSvc = target;
				callArgs.add(args);
				fenceCalled++;
				return true;
			}

			public virtual void checkArgs(string args)
			{
			}

			public static org.apache.hadoop.ha.HAServiceTarget getLastFencedService()
			{
				return fencedSvc;
			}
		}

		/// <summary>Identical mock to above, except always returns false</summary>
		public class AlwaysFailFencer : org.apache.hadoop.conf.Configured, org.apache.hadoop.ha.FenceMethod
		{
			internal static int fenceCalled = 0;

			internal static org.apache.hadoop.ha.HAServiceTarget fencedSvc;

			internal static System.Collections.Generic.IList<string> callArgs = com.google.common.collect.Lists
				.newArrayList();

			public virtual bool tryFence(org.apache.hadoop.ha.HAServiceTarget target, string 
				args)
			{
				fencedSvc = target;
				callArgs.add(args);
				fenceCalled++;
				return false;
			}

			public virtual void checkArgs(string args)
			{
			}
		}
	}
}
