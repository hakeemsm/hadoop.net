using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
{
	/// <summary>Helper methods for the find expression unit tests.</summary>
	internal class TestHelper
	{
		/// <summary>Adds an argument string to an expression</summary>
		internal static void AddArgument(Expression expr, string arg)
		{
			expr.AddArguments(new List<string>(Sharpen.Collections.SingletonList(arg)));
		}

		/// <summary>Converts a command string into a list of arguments.</summary>
		internal static List<string> GetArgs(string cmd)
		{
			return new List<string>(Arrays.AsList(cmd.Split(" ")));
		}
	}
}
