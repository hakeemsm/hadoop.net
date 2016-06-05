using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.Hdfs.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	public class CLITestCmdDFS : CLITestCmd
	{
		public CLITestCmdDFS(string str, CLICommandTypes type)
			: base(str, type)
		{
		}

		/// <exception cref="System.ArgumentException"/>
		public override CommandExecutor GetExecutor(string tag)
		{
			if (GetType() is CLICommandDFSAdmin)
			{
				return new FSCmdExecutor(tag, new DFSAdmin());
			}
			return base.GetExecutor(tag);
		}
	}
}
