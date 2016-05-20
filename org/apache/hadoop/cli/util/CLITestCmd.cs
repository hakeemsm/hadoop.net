using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>Class to define Test Command along with its type</summary>
	public class CLITestCmd : org.apache.hadoop.cli.util.CLICommand
	{
		private readonly org.apache.hadoop.cli.util.CLICommandTypes type;

		private readonly string cmd;

		public CLITestCmd(string str, org.apache.hadoop.cli.util.CLICommandTypes type)
		{
			cmd = str;
			this.type = type;
		}

		/// <exception cref="System.ArgumentException"/>
		public virtual org.apache.hadoop.cli.util.CommandExecutor getExecutor(string tag)
		{
			if (getType() is org.apache.hadoop.cli.util.CLICommandFS)
			{
				return new org.apache.hadoop.cli.util.FSCmdExecutor(tag, new org.apache.hadoop.fs.FsShell
					());
			}
			throw new System.ArgumentException("Unknown type of test command: " + getType());
		}

		public virtual org.apache.hadoop.cli.util.CLICommandTypes getType()
		{
			return type;
		}

		public virtual string getCmd()
		{
			return cmd;
		}

		public override string ToString()
		{
			return cmd;
		}
	}
}
