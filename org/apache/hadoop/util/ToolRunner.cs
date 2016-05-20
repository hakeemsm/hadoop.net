using Sharpen;

namespace org.apache.hadoop.util
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
		/// <see cref="Tool.run(string[])"/>
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
		/// <see cref="Tool.run(string[])"/>
		/// method.
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static int run(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.util.Tool
			 tool, string[] args)
		{
			if (conf == null)
			{
				conf = new org.apache.hadoop.conf.Configuration();
			}
			org.apache.hadoop.util.GenericOptionsParser parser = new org.apache.hadoop.util.GenericOptionsParser
				(conf, args);
			//set the configuration back, so that Tool can configure itself
			tool.setConf(conf);
			//get the args w/o generic hadoop args
			string[] toolArgs = parser.getRemainingArgs();
			return tool.run(toolArgs);
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
		/// <see cref="Tool.run(string[])"/>
		/// method.
		/// </returns>
		/// <exception cref="System.Exception"/>
		public static int run(org.apache.hadoop.util.Tool tool, string[] args)
		{
			return run(tool.getConf(), tool, args);
		}

		/// <summary>Prints generic command-line argurments and usage information.</summary>
		/// <param name="out">stream to write usage information to.</param>
		public static void printGenericCommandUsage(System.IO.TextWriter @out)
		{
			org.apache.hadoop.util.GenericOptionsParser.printGenericCommandUsage(@out);
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
		public static bool confirmPrompt(string prompt)
		{
			while (true)
			{
				System.Console.Error.Write(prompt + " (Y or N) ");
				java.lang.StringBuilder responseBuilder = new java.lang.StringBuilder();
				while (true)
				{
					int c = Sharpen.Runtime.@in.read();
					if (c == -1 || c == '\r' || c == '\n')
					{
						break;
					}
					responseBuilder.Append((char)c);
				}
				string response = responseBuilder.ToString();
				if (Sharpen.Runtime.equalsIgnoreCase(response, "y") || Sharpen.Runtime.equalsIgnoreCase
					(response, "yes"))
				{
					return true;
				}
				else
				{
					if (Sharpen.Runtime.equalsIgnoreCase(response, "n") || Sharpen.Runtime.equalsIgnoreCase
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
