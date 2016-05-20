using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Provide command line access to a FileSystem.</summary>
	public class FsShell : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FsShell)));

		private const int MAX_LINE_WIDTH = 80;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.fs.Trash trash;

		protected internal org.apache.hadoop.fs.shell.CommandFactory commandFactory;

		private readonly string usagePrefix = "Usage: hadoop fs [generic options]";

		/// <summary>Default ctor with no configuration.</summary>
		/// <remarks>
		/// Default ctor with no configuration.  Be sure to invoke
		/// <see cref="org.apache.hadoop.conf.Configured.setConf(org.apache.hadoop.conf.Configuration)
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
		/// <see cref="run(string[])"/>
		/// </remarks>
		/// <param name="conf">the hadoop configuration</param>
		public FsShell(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.FileSystem getFS()
		{
			if (fs == null)
			{
				fs = org.apache.hadoop.fs.FileSystem.get(getConf());
			}
			return fs;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.Trash getTrash()
		{
			if (this.trash == null)
			{
				this.trash = new org.apache.hadoop.fs.Trash(getConf());
			}
			return this.trash;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void init()
		{
			getConf().setQuietMode(true);
			if (commandFactory == null)
			{
				commandFactory = new org.apache.hadoop.fs.shell.CommandFactory(getConf());
				commandFactory.addObject(new org.apache.hadoop.fs.FsShell.Help(this), "-help");
				commandFactory.addObject(new org.apache.hadoop.fs.FsShell.Usage(this), "-usage");
				registerCommands(commandFactory);
			}
		}

		protected internal virtual void registerCommands(org.apache.hadoop.fs.shell.CommandFactory
			 factory)
		{
			// TODO: DFSAdmin subclasses FsShell so need to protect the command
			// registration.  This class should morph into a base class for
			// commands, and then this method can be abstract
			if (Sharpen.Runtime.getClassForObject(this).Equals(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.FsShell))))
			{
				factory.registerCommands(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.FsCommand
					)));
			}
		}

		/// <summary>Returns the Trash object associated with this shell.</summary>
		/// <returns>Path to the trash</returns>
		/// <exception cref="System.IO.IOException">upon error</exception>
		public virtual org.apache.hadoop.fs.Path getCurrentTrashDir()
		{
			return getTrash().getCurrentTrashDir();
		}

		/// <summary>Display help for commands with their short usage and long description</summary>
		protected internal class Usage : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "usage";

			public const string USAGE = "[cmd ...]";

			public const string DESCRIPTION = "Displays the usage for given command or all commands if none "
				 + "is specified.";

			// NOTE: Usage/Help are inner classes to allow access to outer methods
			// that access commandFactory
			protected internal override void processRawArguments(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.isEmpty())
				{
					this._enclosing.printUsage(System.Console.Out);
				}
				else
				{
					foreach (string arg in args)
					{
						this._enclosing.printUsage(System.Console.Out, arg);
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
		protected internal class Help : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "help";

			public const string USAGE = "[cmd ...]";

			public const string DESCRIPTION = "Displays help for given command or all commands if none "
				 + "is specified.";

			protected internal override void processRawArguments(System.Collections.Generic.LinkedList
				<string> args)
			{
				if (args.isEmpty())
				{
					this._enclosing.printHelp(System.Console.Out);
				}
				else
				{
					foreach (string arg in args)
					{
						this._enclosing.printHelp(System.Console.Out, arg);
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
		private void printUsage(System.IO.TextWriter @out)
		{
			printInfo(@out, null, false);
		}

		// print one usage
		private void printUsage(System.IO.TextWriter @out, string cmd)
		{
			printInfo(@out, cmd, false);
		}

		// print all helps
		private void printHelp(System.IO.TextWriter @out)
		{
			printInfo(@out, null, true);
		}

		// print one help
		private void printHelp(System.IO.TextWriter @out, string cmd)
		{
			printInfo(@out, cmd, true);
		}

		private void printInfo(System.IO.TextWriter @out, string cmd, bool showHelp)
		{
			if (cmd != null)
			{
				// display help or usage for one command
				org.apache.hadoop.fs.shell.Command instance = commandFactory.getInstance("-" + cmd
					);
				if (instance == null)
				{
					throw new org.apache.hadoop.fs.FsShell.UnknownCommandException(cmd);
				}
				if (showHelp)
				{
					printInstanceHelp(@out, instance);
				}
				else
				{
					printInstanceUsage(@out, instance);
				}
			}
			else
			{
				// display help or usage for all commands 
				@out.WriteLine(usagePrefix);
				// display list of short usages
				System.Collections.Generic.List<org.apache.hadoop.fs.shell.Command> instances = new 
					System.Collections.Generic.List<org.apache.hadoop.fs.shell.Command>();
				foreach (string name in commandFactory.getNames())
				{
					org.apache.hadoop.fs.shell.Command instance = commandFactory.getInstance(name);
					if (!instance.isDeprecated())
					{
						@out.WriteLine("\t[" + instance.getUsage() + "]");
						instances.add(instance);
					}
				}
				// display long descriptions for each command
				if (showHelp)
				{
					foreach (org.apache.hadoop.fs.shell.Command instance in instances)
					{
						@out.WriteLine();
						printInstanceHelp(@out, instance);
					}
				}
				@out.WriteLine();
				org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(@out);
			}
		}

		private void printInstanceUsage(System.IO.TextWriter @out, org.apache.hadoop.fs.shell.Command
			 instance)
		{
			@out.WriteLine(usagePrefix + " " + instance.getUsage());
		}

		private void printInstanceHelp(System.IO.TextWriter @out, org.apache.hadoop.fs.shell.Command
			 instance)
		{
			@out.WriteLine(instance.getUsage() + " :");
			org.apache.hadoop.tools.TableListing listing = null;
			string prefix = "  ";
			foreach (string line in instance.getDescription().split("\n"))
			{
				if (line.matches("^[ \t]*[-<].*$"))
				{
					string[] segments = line.split(":");
					if (segments.Length == 2)
					{
						if (listing == null)
						{
							listing = createOptionTableListing();
						}
						listing.addRow(segments[0].Trim(), segments[1].Trim());
						continue;
					}
				}
				// Normal literal description.
				if (listing != null)
				{
					foreach (string listingLine in listing.ToString().split("\n"))
					{
						@out.WriteLine(prefix + listingLine);
					}
					listing = null;
				}
				foreach (string descLine in org.apache.commons.lang.WordUtils.wrap(line, MAX_LINE_WIDTH
					, "\n", true).split("\n"))
				{
					@out.WriteLine(prefix + descLine);
				}
			}
			if (listing != null)
			{
				foreach (string listingLine in listing.ToString().split("\n"))
				{
					@out.WriteLine(prefix + listingLine);
				}
			}
		}

		// Creates a two-row table, the first row is for the command line option,
		// the second row is for the option description.
		private org.apache.hadoop.tools.TableListing createOptionTableListing()
		{
			return new org.apache.hadoop.tools.TableListing.Builder().addField(string.Empty).
				addField(string.Empty, true).wrapWidth(MAX_LINE_WIDTH).build();
		}

		/// <summary>run</summary>
		/// <exception cref="System.Exception"/>
		public virtual int run(string[] argv)
		{
			// initialize FsShell
			init();
			int exitCode = -1;
			if (argv.Length < 1)
			{
				printUsage(System.Console.Error);
			}
			else
			{
				string cmd = argv[0];
				org.apache.hadoop.fs.shell.Command instance = null;
				try
				{
					instance = commandFactory.getInstance(cmd);
					if (instance == null)
					{
						throw new org.apache.hadoop.fs.FsShell.UnknownCommandException();
					}
					exitCode = instance.run(java.util.Arrays.copyOfRange(argv, 1, argv.Length));
				}
				catch (System.ArgumentException e)
				{
					displayError(cmd, e.getLocalizedMessage());
					if (instance != null)
					{
						printInstanceUsage(System.Console.Error, instance);
					}
				}
				catch (System.Exception e)
				{
					// instance.run catches IOE, so something is REALLY wrong if here
					LOG.debug("Error", e);
					displayError(cmd, "Fatal internal error");
					Sharpen.Runtime.printStackTrace(e, System.Console.Error);
				}
			}
			return exitCode;
		}

		private void displayError(string cmd, string message)
		{
			foreach (string line in message.split("\n"))
			{
				System.Console.Error.WriteLine(cmd + ": " + line);
				if (cmd[0] != '-')
				{
					org.apache.hadoop.fs.shell.Command instance = null;
					instance = commandFactory.getInstance("-" + cmd);
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
		public virtual void close()
		{
			if (fs != null)
			{
				fs.close();
				fs = null;
			}
		}

		/// <summary>main() has some simple utility methods</summary>
		/// <param name="argv">the command and its arguments</param>
		/// <exception cref="System.Exception">upon error</exception>
		public static void Main(string[] argv)
		{
			org.apache.hadoop.fs.FsShell shell = newShellInstance();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setQuietMode(false);
			shell.setConf(conf);
			int res;
			try
			{
				res = org.apache.hadoop.util.ToolRunner.run(shell, argv);
			}
			finally
			{
				shell.close();
			}
			System.Environment.Exit(res);
		}

		// TODO: this should be abstract in a base class
		protected internal static org.apache.hadoop.fs.FsShell newShellInstance()
		{
			return new org.apache.hadoop.fs.FsShell();
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
		internal class UnknownCommandException : System.ArgumentException
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
