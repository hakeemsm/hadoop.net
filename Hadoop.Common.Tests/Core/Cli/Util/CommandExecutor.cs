using System;
using System.IO;
using Org.Apache.Hadoop.Cli;


namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>This class execute commands and captures the output</summary>
	public abstract class CommandExecutor
	{
		protected internal virtual string[] GetCommandAsArgs(string cmd, string masterKey
			, string master)
		{
			string regex = "\'([^\']*)\'|\"([^\"]*)\"|(\\S+)";
			Matcher matcher = Pattern.Compile(regex).Matcher(cmd);
			AList<string> args = new AList<string>();
			string arg = null;
			while (matcher.Find())
			{
				if (matcher.Group(1) != null)
				{
					arg = matcher.Group(1);
				}
				else
				{
					if (matcher.Group(2) != null)
					{
						arg = matcher.Group(2);
					}
					else
					{
						arg = matcher.Group(3);
					}
				}
				arg = arg.ReplaceAll(masterKey, master);
				arg = arg.ReplaceAll("CLITEST_DATA", new FilePath(CLITestHelper.TestCacheDataDir)
					.ToURI().ToString().Replace(' ', '+'));
				arg = arg.ReplaceAll("USERNAME", Runtime.GetProperty("user.name"));
				args.AddItem(arg);
			}
			return Collections.ToArray(args, new string[0]);
		}

		/// <exception cref="System.Exception"/>
		public virtual CommandExecutor.Result ExecuteCommand(string cmd)
		{
			int exitCode = 0;
			Exception lastException = null;
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
			TextWriter origOut = System.Console.Out;
			TextWriter origErr = System.Console.Error;
			Runtime.SetOut(new TextWriter(bao));
			Runtime.SetErr(new TextWriter(bao));
			try
			{
				Execute(cmd);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				lastException = e;
				exitCode = -1;
			}
			finally
			{
				Runtime.SetOut(origOut);
				Runtime.SetErr(origErr);
			}
			return new CommandExecutor.Result(bao.ToString(), exitCode, lastException, cmd);
		}

		/// <exception cref="System.Exception"/>
		protected internal abstract void Execute(string cmd);

		public class Result
		{
			internal readonly string commandOutput;

			internal readonly int exitCode;

			internal readonly Exception exception;

			internal readonly string cmdExecuted;

			public Result(string commandOutput, int exitCode, Exception exception, string cmdExecuted
				)
			{
				this.commandOutput = commandOutput;
				this.exitCode = exitCode;
				this.exception = exception;
				this.cmdExecuted = cmdExecuted;
			}

			public virtual string GetCommandOutput()
			{
				return commandOutput;
			}

			public virtual int GetExitCode()
			{
				return exitCode;
			}

			public virtual Exception GetException()
			{
				return exception;
			}

			public virtual string GetCommand()
			{
				return cmdExecuted;
			}
		}
	}
}
