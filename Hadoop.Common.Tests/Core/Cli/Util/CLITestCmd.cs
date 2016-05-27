using System;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Class to define Test Command along with its type</summary>
	public class CLITestCmd : CLICommand
	{
		private readonly CLICommandTypes type;

		private readonly string cmd;

		public CLITestCmd(string str, CLICommandTypes type)
		{
			cmd = str;
			this.type = type;
		}

		/// <exception cref="System.ArgumentException"/>
		public virtual CommandExecutor GetExecutor(string tag)
		{
			if (GetType() is CLICommandFS)
			{
				return new FSCmdExecutor(tag, new FsShell());
			}
			throw new ArgumentException("Unknown type of test command: " + GetType());
		}

		public virtual CLICommandTypes GetType()
		{
			return type;
		}

		public virtual string GetCmd()
		{
			return cmd;
		}

		public override string ToString()
		{
			return cmd;
		}
	}
}
