using Sharpen;

namespace org.apache.hadoop.cli
{
	/// <summary>Tests for the Command Line Interface (CLI)</summary>
	public class TestCLI : org.apache.hadoop.cli.CLITestHelper
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
		}

		/// <exception cref="System.Exception"/>
		protected internal override org.apache.hadoop.cli.util.CommandExecutor.Result execute
			(org.apache.hadoop.cli.util.CLICommand cmd)
		{
			return cmd.getExecutor(string.Empty).executeCommand(cmd.getCmd());
		}

		protected internal override string getTestFile()
		{
			return "testConf.xml";
		}

		[NUnit.Framework.Test]
		public override void testAll()
		{
			base.testAll();
		}
	}
}
