using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>Helper methods for the find expression unit tests.</summary>
	internal class TestHelper
	{
		/// <summary>Adds an argument string to an expression</summary>
		internal static void addArgument(org.apache.hadoop.fs.shell.find.Expression expr, 
			string arg)
		{
			expr.addArguments(new System.Collections.Generic.LinkedList<string>(java.util.Collections
				.singletonList(arg)));
		}

		/// <summary>Converts a command string into a list of arguments.</summary>
		internal static System.Collections.Generic.LinkedList<string> getArgs(string cmd)
		{
			return new System.Collections.Generic.LinkedList<string>(java.util.Arrays.asList(
				cmd.split(" ")));
		}
	}
}
