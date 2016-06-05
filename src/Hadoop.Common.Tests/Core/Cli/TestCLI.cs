using Org.Apache.Hadoop.Cli.Util;


namespace Org.Apache.Hadoop.Cli
{
	/// <summary>Tests for the Command Line Interface (CLI)</summary>
	public class TestCLI : CLITestHelper
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		/// <exception cref="System.Exception"/>
		protected internal override CommandExecutor.Result Execute(CLICommand cmd)
		{
			return cmd.GetExecutor(string.Empty).ExecuteCommand(cmd.GetCmd());
		}

		protected internal override string GetTestFile()
		{
			return "testConf.xml";
		}

		[Fact]
		public override void TestAll()
		{
			base.TestAll();
		}
	}
}
