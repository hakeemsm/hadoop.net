using Hadoop.Common.Core.Fs.Shell;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestCommandFactory
	{
		internal static CommandFactory factory;

		internal static Configuration conf = new Configuration();

		internal static void RegisterCommands(CommandFactory factory)
		{
		}

		[SetUp]
		public virtual void TestSetup()
		{
			factory = new CommandFactory(conf);
			NUnit.Framework.Assert.IsNotNull(factory);
		}

		[Fact]
		public virtual void TestRegistration()
		{
			Assert.AssertArrayEquals(new string[] {  }, factory.GetNames());
			factory.RegisterCommands(typeof(TestCommandFactory.TestRegistrar));
			string[] names = factory.GetNames();
			Assert.AssertArrayEquals(new string[] { "tc1", "tc2", "tc2.1" }, names);
			factory.AddClass(typeof(TestCommandFactory.TestCommand3), "tc3");
			names = factory.GetNames();
			Assert.AssertArrayEquals(new string[] { "tc1", "tc2", "tc2.1", "tc3" }, names);
		}

		[Fact]
		public virtual void TestGetInstances()
		{
			factory.RegisterCommands(typeof(TestCommandFactory.TestRegistrar));
			Command instance;
			instance = factory.GetInstance("blarg");
			NUnit.Framework.Assert.IsNull(instance);
			instance = factory.GetInstance("tc1");
			NUnit.Framework.Assert.IsNotNull(instance);
			Assert.Equal(typeof(TestCommandFactory.TestCommand1), instance
				.GetType());
			Assert.Equal("tc1", instance.GetCommandName());
			instance = factory.GetInstance("tc2");
			NUnit.Framework.Assert.IsNotNull(instance);
			Assert.Equal(typeof(TestCommandFactory.TestCommand2), instance
				.GetType());
			Assert.Equal("tc2", instance.GetCommandName());
			instance = factory.GetInstance("tc2.1");
			NUnit.Framework.Assert.IsNotNull(instance);
			Assert.Equal(typeof(TestCommandFactory.TestCommand2), instance
				.GetType());
			Assert.Equal("tc2.1", instance.GetCommandName());
		}

		internal class TestRegistrar
		{
			public static void RegisterCommands(CommandFactory factory)
			{
				factory.AddClass(typeof(TestCommandFactory.TestCommand1), "tc1");
				factory.AddClass(typeof(TestCommandFactory.TestCommand2), "tc2", "tc2.1");
			}
		}

		internal class TestCommand1 : FsCommand
		{
		}

		internal class TestCommand2 : FsCommand
		{
		}

		internal class TestCommand3 : FsCommand
		{
		}
	}
}
