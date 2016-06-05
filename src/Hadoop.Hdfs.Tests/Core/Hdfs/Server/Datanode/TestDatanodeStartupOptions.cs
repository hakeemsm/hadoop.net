using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This test verifies DataNode command line processing.</summary>
	public class TestDatanodeStartupOptions
	{
		private Configuration conf = null;

		/// <summary>
		/// Process the given arg list as command line arguments to the DataNode
		/// to make sure we get the expected result.
		/// </summary>
		/// <remarks>
		/// Process the given arg list as command line arguments to the DataNode
		/// to make sure we get the expected result. If the expected result is
		/// success then further validate that the parsed startup option is the
		/// same as what was expected.
		/// </remarks>
		/// <param name="expectSuccess"/>
		/// <param name="expectedOption"/>
		/// <param name="conf"/>
		/// <param name="arg"/>
		private static void CheckExpected(bool expectSuccess, HdfsServerConstants.StartupOption
			 expectedOption, Configuration conf, params string[] arg)
		{
			string[] args = new string[arg.Length];
			int i = 0;
			foreach (string currentArg in arg)
			{
				args[i++] = currentArg;
			}
			bool returnValue = DataNode.ParseArguments(args, conf);
			HdfsServerConstants.StartupOption option = DataNode.GetStartupOption(conf);
			Assert.AssertThat(returnValue, IS.Is(expectSuccess));
			if (expectSuccess)
			{
				Assert.AssertThat(option, IS.Is(expectedOption));
			}
		}

		/// <summary>
		/// Reinitialize configuration before every test since DN stores the
		/// parsed StartupOption in the configuration.
		/// </summary>
		[SetUp]
		public virtual void InitConfiguration()
		{
			conf = new HdfsConfiguration();
		}

		/// <summary>A few options that should all parse successfully.</summary>
		public virtual void TestStartupSuccess()
		{
			CheckExpected(true, HdfsServerConstants.StartupOption.Regular, conf);
			CheckExpected(true, HdfsServerConstants.StartupOption.Regular, conf, "-regular");
			CheckExpected(true, HdfsServerConstants.StartupOption.Regular, conf, "-REGULAR");
			CheckExpected(true, HdfsServerConstants.StartupOption.Rollback, conf, "-rollback"
				);
		}

		/// <summary>A few options that should all fail to parse.</summary>
		public virtual void TestStartupFailure()
		{
			CheckExpected(false, HdfsServerConstants.StartupOption.Regular, conf, "unknownoption"
				);
			CheckExpected(false, HdfsServerConstants.StartupOption.Regular, conf, "-regular -rollback"
				);
		}
	}
}
