using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>This class execute commands and captures the output</summary>
	public abstract class CommandExecutor
	{
		protected internal virtual string[] getCommandAsArgs(string cmd, string masterKey
			, string master)
		{
			string regex = "\'([^\']*)\'|\"([^\"]*)\"|(\\S+)";
			java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(regex).matcher(
				cmd);
			System.Collections.Generic.List<string> args = new System.Collections.Generic.List
				<string>();
			string arg = null;
			while (matcher.find())
			{
				if (matcher.group(1) != null)
				{
					arg = matcher.group(1);
				}
				else
				{
					if (matcher.group(2) != null)
					{
						arg = matcher.group(2);
					}
					else
					{
						arg = matcher.group(3);
					}
				}
				arg = arg.replaceAll(masterKey, master);
				arg = arg.replaceAll("CLITEST_DATA", new java.io.File(org.apache.hadoop.cli.CLITestHelper
					.TEST_CACHE_DATA_DIR).toURI().ToString().Replace(' ', '+'));
				arg = arg.replaceAll("USERNAME", Sharpen.Runtime.getProperty("user.name"));
				args.add(arg);
			}
			return Sharpen.Collections.ToArray(args, new string[0]);
		}

		/// <exception cref="System.Exception"/>
		public virtual org.apache.hadoop.cli.util.CommandExecutor.Result executeCommand(string
			 cmd)
		{
			int exitCode = 0;
			System.Exception lastException = null;
			java.io.ByteArrayOutputStream bao = new java.io.ByteArrayOutputStream();
			System.IO.TextWriter origOut = System.Console.Out;
			System.IO.TextWriter origErr = System.Console.Error;
			Sharpen.Runtime.setOut(new System.IO.TextWriter(bao));
			Sharpen.Runtime.setErr(new System.IO.TextWriter(bao));
			try
			{
				execute(cmd);
			}
			catch (System.Exception e)
			{
				Sharpen.Runtime.printStackTrace(e);
				lastException = e;
				exitCode = -1;
			}
			finally
			{
				Sharpen.Runtime.setOut(origOut);
				Sharpen.Runtime.setErr(origErr);
			}
			return new org.apache.hadoop.cli.util.CommandExecutor.Result(bao.ToString(), exitCode
				, lastException, cmd);
		}

		/// <exception cref="System.Exception"/>
		protected internal abstract void execute(string cmd);

		public class Result
		{
			internal readonly string commandOutput;

			internal readonly int exitCode;

			internal readonly System.Exception exception;

			internal readonly string cmdExecuted;

			public Result(string commandOutput, int exitCode, System.Exception exception, string
				 cmdExecuted)
			{
				this.commandOutput = commandOutput;
				this.exitCode = exitCode;
				this.exception = exception;
				this.cmdExecuted = cmdExecuted;
			}

			public virtual string getCommandOutput()
			{
				return commandOutput;
			}

			public virtual int getExitCode()
			{
				return exitCode;
			}

			public virtual System.Exception getException()
			{
				return exception;
			}

			public virtual string getCommand()
			{
				return cmdExecuted;
			}
		}
	}
}
