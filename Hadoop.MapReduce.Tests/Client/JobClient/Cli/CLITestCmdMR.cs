using System;
using Org.Apache.Hadoop.Cli.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class CLITestCmdMR : CLITestCmd
	{
		public CLITestCmdMR(string str, CLICommandTypes type)
			: base(str, type)
		{
		}

		/// <summary>
		/// This is not implemented because HadoopArchive constructor requires JobConf
		/// to create an archive object.
		/// </summary>
		/// <remarks>
		/// This is not implemented because HadoopArchive constructor requires JobConf
		/// to create an archive object. Because TestMRCLI uses setup method from
		/// TestHDFSCLI the initialization of executor objects happens before a config
		/// is created and updated. Thus, actual calls to executors happen in the body
		/// of the test method.
		/// </remarks>
		/// <exception cref="System.ArgumentException"/>
		public override CommandExecutor GetExecutor(string tag)
		{
			throw new ArgumentException("Method isn't supported");
		}
	}
}
