using Sharpen;

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>This interface is to generalize types of test command for upstream projects
	/// 	</summary>
	public interface CLICommand
	{
		/// <exception cref="System.ArgumentException"/>
		CommandExecutor GetExecutor(string tag);

		CLICommandTypes GetType();

		string GetCmd();

		string ToString();
	}
}
