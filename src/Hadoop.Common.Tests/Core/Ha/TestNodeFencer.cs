using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.HA
{
	public class TestNodeFencer
	{
		private HAServiceTarget MockTarget;

		private static string FencerTrueCommandUnix = "shell(true)";

		private static string FencerTrueCommandWindows = "shell(rem)";

		// Fencer shell commands that always return true on Unix and Windows
		// respectively. Lacking the POSIX 'true' command on Windows, we use
		// the batch command 'rem'.
		[SetUp]
		public virtual void ClearMockState()
		{
			TestNodeFencer.AlwaysSucceedFencer.fenceCalled = 0;
			TestNodeFencer.AlwaysSucceedFencer.callArgs.Clear();
			TestNodeFencer.AlwaysFailFencer.fenceCalled = 0;
			TestNodeFencer.AlwaysFailFencer.callArgs.Clear();
			MockTarget = Org.Mockito.Mockito.Mock<HAServiceTarget>();
			Org.Mockito.Mockito.DoReturn("my mock").When(MockTarget).ToString();
			Org.Mockito.Mockito.DoReturn(new IPEndPoint("host", 1234)).When(MockTarget).GetAddress
				();
		}

		private static string GetFencerTrueCommand()
		{
			return Shell.Windows ? FencerTrueCommandWindows : FencerTrueCommandUnix;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestSingleFencer()
		{
			NodeFencer fencer = SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer).FullName
				 + "(foo)");
			Assert.True(fencer.Fence(MockTarget));
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			NUnit.Framework.Assert.AreSame(MockTarget, TestNodeFencer.AlwaysSucceedFencer.fencedSvc
				);
			Assert.Equal("foo", TestNodeFencer.AlwaysSucceedFencer.callArgs
				[0]);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestMultipleFencers()
		{
			NodeFencer fencer = SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer).FullName
				 + "(foo)\n" + typeof(TestNodeFencer.AlwaysSucceedFencer).FullName + "(bar)\n");
			Assert.True(fencer.Fence(MockTarget));
			// Only one call, since the first fencer succeeds
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			Assert.Equal("foo", TestNodeFencer.AlwaysSucceedFencer.callArgs
				[0]);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestWhitespaceAndCommentsInConfig()
		{
			NodeFencer fencer = SetupFencer("\n" + " # the next one will always fail\n" + " "
				 + typeof(TestNodeFencer.AlwaysFailFencer).FullName + "(foo) # <- fails\n" + typeof(
				TestNodeFencer.AlwaysSucceedFencer).FullName + "(bar) \n");
			Assert.True(fencer.Fence(MockTarget));
			// One call to each, since top fencer fails
			Assert.Equal(1, TestNodeFencer.AlwaysFailFencer.fenceCalled);
			NUnit.Framework.Assert.AreSame(MockTarget, TestNodeFencer.AlwaysFailFencer.fencedSvc
				);
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			NUnit.Framework.Assert.AreSame(MockTarget, TestNodeFencer.AlwaysSucceedFencer.fencedSvc
				);
			Assert.Equal("foo", TestNodeFencer.AlwaysFailFencer.callArgs[0
				]);
			Assert.Equal("bar", TestNodeFencer.AlwaysSucceedFencer.callArgs
				[0]);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestArglessFencer()
		{
			NodeFencer fencer = SetupFencer(typeof(TestNodeFencer.AlwaysSucceedFencer).FullName
				);
			Assert.True(fencer.Fence(MockTarget));
			// One call to each, since top fencer fails
			Assert.Equal(1, TestNodeFencer.AlwaysSucceedFencer.fenceCalled
				);
			NUnit.Framework.Assert.AreSame(MockTarget, TestNodeFencer.AlwaysSucceedFencer.fencedSvc
				);
			Assert.Equal(null, TestNodeFencer.AlwaysSucceedFencer.callArgs
				[0]);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestShortNameShell()
		{
			NodeFencer fencer = SetupFencer(GetFencerTrueCommand());
			Assert.True(fencer.Fence(MockTarget));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestShortNameSsh()
		{
			NodeFencer fencer = SetupFencer("sshfence");
			NUnit.Framework.Assert.IsFalse(fencer.Fence(MockTarget));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestShortNameSshWithUser()
		{
			NodeFencer fencer = SetupFencer("sshfence(user)");
			NUnit.Framework.Assert.IsFalse(fencer.Fence(MockTarget));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestShortNameSshWithPort()
		{
			NodeFencer fencer = SetupFencer("sshfence(:123)");
			NUnit.Framework.Assert.IsFalse(fencer.Fence(MockTarget));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		[Fact]
		public virtual void TestShortNameSshWithUserPort()
		{
			NodeFencer fencer = SetupFencer("sshfence(user:123)");
			NUnit.Framework.Assert.IsFalse(fencer.Fence(MockTarget));
		}

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public static NodeFencer SetupFencer(string confStr)
		{
			System.Console.Error.WriteLine("Testing configuration:\n" + confStr);
			Configuration conf = new Configuration();
			return new NodeFencer(conf, confStr);
		}

		/// <summary>Mock fencing method that always returns true</summary>
		public class AlwaysSucceedFencer : Configured, FenceMethod
		{
			internal static int fenceCalled = 0;

			internal static HAServiceTarget fencedSvc;

			internal static IList<string> callArgs = Lists.NewArrayList();

			public virtual bool TryFence(HAServiceTarget target, string args)
			{
				fencedSvc = target;
				callArgs.AddItem(args);
				fenceCalled++;
				return true;
			}

			public virtual void CheckArgs(string args)
			{
			}

			public static HAServiceTarget GetLastFencedService()
			{
				return fencedSvc;
			}
		}

		/// <summary>Identical mock to above, except always returns false</summary>
		public class AlwaysFailFencer : Configured, FenceMethod
		{
			internal static int fenceCalled = 0;

			internal static HAServiceTarget fencedSvc;

			internal static IList<string> callArgs = Lists.NewArrayList();

			public virtual bool TryFence(HAServiceTarget target, string args)
			{
				fencedSvc = target;
				callArgs.AddItem(args);
				fenceCalled++;
				return false;
			}

			public virtual void CheckArgs(string args)
			{
			}
		}
	}
}
