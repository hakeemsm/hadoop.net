using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>This interface is to generalize types of test command for upstream projects
	/// 	</summary>
	public interface CLICommand
	{
		/// <exception cref="System.ArgumentException"/>
		org.apache.hadoop.cli.util.CommandExecutor getExecutor(string tag);

		org.apache.hadoop.cli.util.CLICommandTypes getType();

		string getCmd();

		string ToString();
	}
}
