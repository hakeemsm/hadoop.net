using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Provide command line access to a FileSystem.</summary>
	public class FsShell : Configured, Tool
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.FsShell
			));

		private const int MaxLineWidth = 80;

		private FileSystem fs;

		private Trash trash;

		protected internal CommandFactory commandFactory;

		private readonly string usagePrefix = "Usage: hadoop fs [generic options]";

		/// <summary>Default ctor with no configuration.</summary>
		/// <remarks>
		/// Default ctor with no configuration.  Be sure to invoke
		/// <see cref="Org.Apache.Hadoop.Conf.Configured.SetConf(Configuration)
		/// 	"/>
		/// with a valid configuration prior
		/// to running commands.
		/// </remarks>
		public FsShell()
			: this(null)
		{
		}

		/// <summary>Construct a FsShell with the given configuration.</summary>
		/// <remarks>
		/// Construct a FsShell with the given configuration.  Commands can be
		/// executed via
		/// <see cref="Run(string[])"/>
		/// </remarks>
		/// <param name="conf">the hadoop configuration</param>
		public FsShell(Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual FileSystem GetFS()
		{
			if (fs == null)
			{
				fs = FileSystem.Get(GetConf());
			}
			return fs;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Trash GetTrash()
		{
			if (this.trash == null)
			{
				this.trash = new Trash(GetConf());
			}
			return this.trash;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Init()
		{
			GetConf().SetQuietMode(true);
			if (commandFactory == null)
			{
				commandFactory = new CommandFactory(GetConf());
				commandFactory.AddObject(new FsShell.Help(this), "-help");
				commandFactory.AddObject(new FsShell.Usage(this), "-usage");
				RegisterCommands(commandFactory);
			}
		}

		protected internal virtual void RegisterCommands(CommandFactory factory)
		{
			// TODO: DFSAdmin subclasses FsShell so need to protect the command
			// registration.  This class should morph into a base class for
			// commands, and then this method can be abstract
			if (this.GetType().Equals(typeof(Org.Apache.Hadoop.FS.FsShell)))
			{
				factory.RegisterCommands(typeof(FsCommand));
			}
		}

		/// <summary>Returns the Trash object associated with this shell.</summary>
		/// <returns>Path to the trash</returns>
		/// <exception cref="System.IO.IOException">upon error</exception>
		public virtual Path GetCurrentTrashDir()
		{
			return GetTrash().GetCurrentTrashDir();
		}

		/// <summary>Display help for commands with their short usage and long description</summary>
		protected internal class Usage : FsCommand
		{
			public const string Name = "usage";

			public const string Usage = "[cmd ...]";

			public const string Description = "Displays the usage for given command or all commands if none "
				 + "is specified.";

			// NOTE: Usage/Help are inner classes to allow access to outer methods
			// that access commandFactory
			protected internal override void ProcessRawArguments(List<string> args)
			{
				if (args.IsEmpty())
				{
					this._enclosing.PrintUsage(System.Console.Out);
				}
				else
				{
					foreach (string arg in args)
					{
						this._enclosing.PrintUsage(System.Console.Out, arg);
					}
				}
			}

			internal Usage(FsShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FsShell _enclosing;
		}

		/// <summary>Displays short usage of commands sans the long description</summary>
		protected internal class Help : FsCommand
		{
			public const string Name = "help";

			public const string Usage = "[cmd ...]";

			public const string Description = "Displays help for given command or all commands if none "
				 + "is specified.";

			protected internal override void ProcessRawArguments(List<string> args)
			{
				if (args.IsEmpty())
				{
					this._enclosing.PrintHelp(System.Console.Out);
				}
				else
				{
					foreach (string arg in args)
					{
						this._enclosing.PrintHelp(System.Console.Out, arg);
					}
				}
			}

			internal Help(FsShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FsShell _enclosing;
		}

		/*
		* The following are helper methods for getInfo().  They are defined
		* outside of the scope of the Help/Usage class because the run() method
		* needs to invoke them too.
		*/
		// print all usages
		private void PrintUsage(TextWriter @out)
		{
			PrintInfo(@out, null, false);
		}

		// print one usage
		private void PrintUsage(TextWriter @out, string cmd)
		{
			PrintInfo(@out, cmd, false);
		}

		// print all helps
		private void PrintHelp(TextWriter @out)
		{
			PrintInfo(@out, null, true);
		}

		// print one help
		private void PrintHelp(TextWriter @out, string cmd)
		{
			PrintInfo(@out, cmd, true);
		}

		private void PrintInfo(TextWriter @out, string cmd, bool showHelp)
		{
			if (cmd != null)
			{
				// display help or usage for one command
				Command instance = commandFactory.GetInstance("-" + cmd);
				if (instance == null)
				{
					throw new FsShell.UnknownCommandException(cmd);
				}
				if (showHelp)
				{
					PrintInstanceHelp(@out, instance);
				}
				else
				{
					PrintInstanceUsage(@out, instance);
				}
			}
			else
			{
				// display help or usage for all commands 
				@out.WriteLine(usagePrefix);
				// display list of short usages
				AList<Command> instances = new AList<Command>();
				foreach (string name in commandFactory.GetNames())
				{
					Command instance = commandFactory.GetInstance(name);
					if (!instance.IsDeprecated())
					{
						@out.WriteLine("\t[" + instance.GetUsage() + "]");
						instances.AddItem(instance);
					}
				}
				// display long descriptions for each command
				if (showHelp)
				{
					foreach (Command instance in instances)
					{
						@out.WriteLine();
						PrintInstanceHelp(@out, instance);
					}
				}
				@out.WriteLine();
				ToolRunner.PrintGenericCommandUsage(@out);
			}
		}

		private void PrintInstanceUsage(TextWriter @out, Command instance)
		{
			@out.WriteLine(usagePrefix + " " + instance.GetUsage());
		}

		private void PrintInstanceHelp(TextWriter @out, Command instance)
		{
			@out.WriteLine(instance.GetUsage() + " :");
			TableListing listing = null;
			string prefix = "  ";
			foreach (string line in instance.GetDescription().Split("\n"))
			{
				if (line.Matches("^[ \t]*[-<].*$"))
				{
					string[] segments = line.Split(":");
					if (segments.Length == 2)
					{
						if (listing == null)
						{
							listing = CreateOptionTableListing();
						}
						listing.AddRow(segments[0].Trim(), segments[1].Trim());
						continue;
					}
				}
				// Normal literal description.
				if (listing != null)
				{
					foreach (string listingLine in listing.ToString().Split("\n"))
					{
						@out.WriteLine(prefix + listingLine);
					}
					listing = null;
				}
				foreach (string descLine in WordUtils.Wrap(line, MaxLineWidth, "\n", true).Split(
					"\n"))
				{
					@out.WriteLine(prefix + descLine);
				}
			}
			if (listing != null)
			{
				foreach (string listingLine in listing.ToString().Split("\n"))
				{
					@out.WriteLine(prefix + listingLine);
				}
			}
		}

		// Creates a two-row table, the first row is for the command line option,
		// the second row is for the option description.
		private TableListing CreateOptionTableListing()
		{
			return new TableListing.Builder().AddField(string.Empty).AddField(string.Empty, true
				).WrapWidth(MaxLineWidth).Build();
		}

		/// <summary>run</summary>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			// initialize FsShell
			Init();
			int exitCode = -1;
			if (argv.Length < 1)
			{
				PrintUsage(System.Console.Error);
			}
			else
			{
				string cmd = argv[0];
				Command instance = null;
				try
				{
					instance = commandFactory.GetInstance(cmd);
					if (instance == null)
					{
						throw new FsShell.UnknownCommandException();
					}
					exitCode = instance.Run(Arrays.CopyOfRange(argv, 1, argv.Length));
				}
				catch (ArgumentException e)
				{
					DisplayError(cmd, e.GetLocalizedMessage());
					if (instance != null)
					{
						PrintInstanceUsage(System.Console.Error, instance);
					}
				}
				catch (Exception e)
				{
					// instance.run catches IOE, so something is REALLY wrong if here
					Log.Debug("Error", e);
					DisplayError(cmd, "Fatal internal error");
					Sharpen.Runtime.PrintStackTrace(e, System.Console.Error);
				}
			}
			return exitCode;
		}

		private void DisplayError(string cmd, string message)
		{
			foreach (string line in message.Split("\n"))
			{
				System.Console.Error.WriteLine(cmd + ": " + line);
				if (cmd[0] != '-')
				{
					Command instance = null;
					instance = commandFactory.GetInstance("-" + cmd);
					if (instance != null)
					{
						System.Console.Error.WriteLine("Did you mean -" + cmd + "?  This command " + "begins with a dash."
							);
					}
				}
			}
		}

		/// <summary>Performs any necessary cleanup</summary>
		/// <exception cref="System.IO.IOException">upon error</exception>
		public virtual void Close()
		{
			if (fs != null)
			{
				fs.Close();
				fs = null;
			}
		}

		/// <summary>main() has some simple utility methods</summary>
		/// <param name="argv">the command and its arguments</param>
		/// <exception cref="System.Exception">upon error</exception>
		public static void Main(string[] argv)
		{
			FsShell shell = NewShellInstance();
			Configuration conf = new Configuration();
			conf.SetQuietMode(false);
			shell.SetConf(conf);
			int res;
			try
			{
				res = ToolRunner.Run(shell, argv);
			}
			finally
			{
				shell.Close();
			}
			System.Environment.Exit(res);
		}

		// TODO: this should be abstract in a base class
		protected internal static FsShell NewShellInstance()
		{
			return new FsShell();
		}

		/// <summary>
		/// The default ctor signals that the command being executed does not exist,
		/// while other ctor signals that a specific command does not exist.
		/// </summary>
		/// <remarks>
		/// The default ctor signals that the command being executed does not exist,
		/// while other ctor signals that a specific command does not exist.  The
		/// latter is used by commands that process other commands, ex. -usage/-help
		/// </remarks>
		[System.Serializable]
		internal class UnknownCommandException : ArgumentException
		{
			private readonly string cmd;

			internal UnknownCommandException()
				: this(null)
			{
			}

			internal UnknownCommandException(string cmd)
			{
				this.cmd = cmd;
			}

			public override string Message
			{
				get
				{
					return ((cmd != null) ? "`" + cmd + "': " : string.Empty) + "Unknown command";
				}
			}
		}
	}
}
