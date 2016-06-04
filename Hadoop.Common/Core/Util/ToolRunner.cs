using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A utility to help run
	/// <see cref="Tool"/>
	/// s.
	/// <p><code>ToolRunner</code> can be used to run classes implementing
	/// <code>Tool</code> interface. It works in conjunction with
	/// <see cref="GenericOptionsParser"/>
	/// to parse the
	/// &lt;a href="
	/// <docRoot/>
	/// /../hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options"&gt;
	/// generic hadoop command line arguments</a> and modifies the
	/// <code>Configuration</code> of the <code>Tool</code>. The
	/// application-specific options are passed along without being modified.
	/// </p>
	/// </summary>
	/// <seealso cref="Tool"/>
	/// <seealso cref="GenericOptionsParser"/>
	public class ToolRunner
	{
		/// <summary>
		/// Runs the given <code>Tool</code> by
		/// <see cref="Tool.Run(string[])"/>
		/// , after
		/// parsing with the given generic arguments. Uses the given
		/// <code>Configuration</code>, or builds one if null.
		/// Sets the <code>Tool</code>'s configuration with the possibly modified
		/// version of the <code>conf</code>.
		/// </summary>
		/// <param name="conf"><code>Configuration</code> for the <code>Tool</code>.</param>
		/// <param name="tool"><code>Tool</code> to run.</param>
		/// <param name="args">command-line arguments to the tool.</param>
		/// <returns>
		/// exit code of the
		/// <see cref="Tool.Run(string[])"/>
		/// method.
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static int Run(Configuration conf, Tool tool, string[] args)
		{
			if (conf == null)
			{
				conf = new Configuration();
			}
			GenericOptionsParser parser = new GenericOptionsParser(conf, args);
			//set the configuration back, so that Tool can configure itself
			tool.SetConf(conf);
			//get the args w/o generic hadoop args
			string[] toolArgs = parser.GetRemainingArgs();
			return tool.Run(toolArgs);
		}

		/// <summary>Runs the <code>Tool</code> with its <code>Configuration</code>.</summary>
		/// <remarks>
		/// Runs the <code>Tool</code> with its <code>Configuration</code>.
		/// Equivalent to <code>run(tool.getConf(), tool, args)</code>.
		/// </remarks>
		/// <param name="tool"><code>Tool</code> to run.</param>
		/// <param name="args">command-line arguments to the tool.</param>
		/// <returns>
		/// exit code of the
		/// <see cref="Tool.Run(string[])"/>
		/// method.
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static int Run(Tool tool, string[] args)
		{
			return Run(tool.GetConf(), tool, args);
		}

		/// <summary>Prints generic command-line argurments and usage information.</summary>
		/// <param name="out">stream to write usage information to.</param>
		public static void PrintGenericCommandUsage(TextWriter @out)
		{
			GenericOptionsParser.PrintGenericCommandUsage(@out);
		}

		/// <summary>
		/// Print out a prompt to the user, and return true if the user
		/// responds with "y" or "yes".
		/// </summary>
		/// <remarks>
		/// Print out a prompt to the user, and return true if the user
		/// responds with "y" or "yes". (case insensitive)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static bool ConfirmPrompt(string prompt)
		{
			while (true)
			{
				System.Console.Error.Write(prompt + " (Y or N) ");
				StringBuilder responseBuilder = new StringBuilder();
				while (true)
				{
					int c = Runtime.@in.Read();
					if (c == -1 || c == '\r' || c == '\n')
					{
						break;
					}
					responseBuilder.Append((char)c);
				}
				string response = responseBuilder.ToString();
				if (Sharpen.Runtime.EqualsIgnoreCase(response, "y") || Sharpen.Runtime.EqualsIgnoreCase
					(response, "yes"))
				{
					return true;
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(response, "n") || Sharpen.Runtime.EqualsIgnoreCase
						(response, "no"))
					{
						return false;
					}
				}
				System.Console.Error.WriteLine("Invalid input: " + response);
			}
		}
		// else ask them again
	}
}
