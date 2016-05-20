using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestCommandFactory
	{
		internal static org.apache.hadoop.fs.shell.CommandFactory factory;

		internal static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
			();

		internal static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
		}

		[NUnit.Framework.SetUp]
		public virtual void testSetup()
		{
			factory = new org.apache.hadoop.fs.shell.CommandFactory(conf);
			NUnit.Framework.Assert.IsNotNull(factory);
		}

		[NUnit.Framework.Test]
		public virtual void testRegistration()
		{
			NUnit.Framework.Assert.assertArrayEquals(new string[] {  }, factory.getNames());
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestRegistrar
				)));
			string[] names = factory.getNames();
			NUnit.Framework.Assert.assertArrayEquals(new string[] { "tc1", "tc2", "tc2.1" }, 
				names);
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand3
				)), "tc3");
			names = factory.getNames();
			NUnit.Framework.Assert.assertArrayEquals(new string[] { "tc1", "tc2", "tc2.1", "tc3"
				 }, names);
		}

		[NUnit.Framework.Test]
		public virtual void testGetInstances()
		{
			factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestRegistrar
				)));
			org.apache.hadoop.fs.shell.Command instance;
			instance = factory.getInstance("blarg");
			NUnit.Framework.Assert.IsNull(instance);
			instance = factory.getInstance("tc1");
			NUnit.Framework.Assert.IsNotNull(instance);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand1
				)), Sharpen.Runtime.getClassForObject(instance));
			NUnit.Framework.Assert.AreEqual("tc1", instance.getCommandName());
			instance = factory.getInstance("tc2");
			NUnit.Framework.Assert.IsNotNull(instance);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand2
				)), Sharpen.Runtime.getClassForObject(instance));
			NUnit.Framework.Assert.AreEqual("tc2", instance.getCommandName());
			instance = factory.getInstance("tc2.1");
			NUnit.Framework.Assert.IsNotNull(instance);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand2
				)), Sharpen.Runtime.getClassForObject(instance));
			NUnit.Framework.Assert.AreEqual("tc2.1", instance.getCommandName());
		}

		internal class TestRegistrar
		{
			public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
				)
			{
				factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand1
					)), "tc1");
				factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestCommandFactory.TestCommand2
					)), "tc2", "tc2.1");
			}
		}

		internal class TestCommand1 : org.apache.hadoop.fs.shell.FsCommand
		{
		}

		internal class TestCommand2 : org.apache.hadoop.fs.shell.FsCommand
		{
		}

		internal class TestCommand3 : org.apache.hadoop.fs.shell.FsCommand
		{
		}
	}
}
