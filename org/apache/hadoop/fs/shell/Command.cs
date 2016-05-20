using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>An abstract class for the execution of a file system command</summary>
	public abstract class Command : org.apache.hadoop.conf.Configured
	{
		/// <summary>default name of the command</summary>
		public static string NAME;

		/// <summary>the command's usage switches and arguments format</summary>
		public static string USAGE;

		/// <summary>the command's long description</summary>
		public static string DESCRIPTION;

		protected internal string[] args;

		protected internal string name;

		protected internal int exitCode = 0;

		protected internal int numErrors = 0;

		protected internal bool recursive = false;

		private int depth = 0;

		protected internal System.Collections.Generic.List<System.Exception> exceptions = 
			new System.Collections.Generic.List<System.Exception>();

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Command
			)));

		/// <summary>allows stdout to be captured if necessary</summary>
		public System.IO.TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public System.IO.TextWriter err = System.Console.Error;

		/// <summary>allows the command factory to be used if necessary</summary>
		private org.apache.hadoop.fs.shell.CommandFactory commandFactory = null;

		/// <summary>Constructor</summary>
		protected internal Command()
		{
			@out = System.Console.Out;
			err = System.Console.Error;
		}

		/// <summary>Constructor</summary>
		protected internal Command(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		/// <returns>the command's name excluding the leading character -</returns>
		public abstract string getCommandName();

		protected internal virtual void setRecursive(bool flag)
		{
			recursive = flag;
		}

		protected internal virtual bool isRecursive()
		{
			return recursive;
		}

		protected internal virtual int getDepth()
		{
			return depth;
		}

		/// <summary>Execute the command on the input path</summary>
		/// <param name="path">the input path</param>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		protected internal abstract void run(org.apache.hadoop.fs.Path path);

		/// <summary>For each source path, execute the command</summary>
		/// <returns>0 if it runs successfully; -1 if it fails</returns>
		public virtual int runAll()
		{
			int exitCode = 0;
			foreach (string src in args)
			{
				try
				{
					org.apache.hadoop.fs.shell.PathData[] srcs = org.apache.hadoop.fs.shell.PathData.
						expandAsGlob(src, getConf());
					foreach (org.apache.hadoop.fs.shell.PathData s in srcs)
					{
						run(s.path);
					}
				}
				catch (System.IO.IOException e)
				{
					exitCode = -1;
					displayError(e);
				}
			}
			return exitCode;
		}

		/// <summary>sets the command factory for later use</summary>
		public virtual void setCommandFactory(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			this.commandFactory = factory;
		}

		/// <summary>retrieves the command factory</summary>
		protected internal virtual org.apache.hadoop.fs.shell.CommandFactory getCommandFactory
			()
		{
			return this.commandFactory;
		}

		/// <summary>Invokes the command handler.</summary>
		/// <remarks>
		/// Invokes the command handler.  The default behavior is to process options,
		/// expand arguments, and then process each argument.
		/// <pre>
		/// run
		/// |-&gt;
		/// <see cref="processOptions(System.Collections.Generic.LinkedList{E})"/>
		/// \-&gt;
		/// <see cref="processRawArguments(System.Collections.Generic.LinkedList{E})"/>
		/// |-&gt;
		/// <see cref="expandArguments(System.Collections.Generic.LinkedList{E})"/>
		/// |   \-&gt;
		/// <see cref="expandArgument(string)"/>
		/// \-&gt;
		/// <see cref="processArguments(System.Collections.Generic.LinkedList{E})"/>
		/// |-&gt;
		/// <see cref="processArgument(PathData)"/>
		/// |   |-&gt;
		/// <see cref="processPathArgument(PathData)"/>
		/// |   \-&gt;
		/// <see cref="processPaths(PathData, PathData[])"/>
		/// |        \-&gt;
		/// <see cref="processPath(PathData)"/>
		/// \-&gt;
		/// <see cref="processNonexistentPath(PathData)"/>
		/// </pre>
		/// Most commands will chose to implement just
		/// <see cref="processOptions(System.Collections.Generic.LinkedList{E})"/>
		/// and
		/// <see cref="processPath(PathData)"/>
		/// </remarks>
		/// <param name="argv">the list of command line arguments</param>
		/// <returns>the exit code for the command</returns>
		/// <exception cref="System.ArgumentException">if called with invalid arguments</exception>
		public virtual int run(params string[] argv)
		{
			System.Collections.Generic.LinkedList<string> args = new System.Collections.Generic.LinkedList
				<string>(java.util.Arrays.asList(argv));
			try
			{
				if (isDeprecated())
				{
					displayWarning("DEPRECATED: Please use '" + getReplacementCommand() + "' instead."
						);
				}
				processOptions(args);
				processRawArguments(args);
			}
			catch (System.IO.IOException e)
			{
				displayError(e);
			}
			return (numErrors == 0) ? exitCode : exitCodeForError();
		}

		/// <summary>The exit code to be returned if any errors occur during execution.</summary>
		/// <remarks>
		/// The exit code to be returned if any errors occur during execution.
		/// This method is needed to account for the inconsistency in the exit
		/// codes returned by various commands.
		/// </remarks>
		/// <returns>a non-zero exit code</returns>
		protected internal virtual int exitCodeForError()
		{
			return 1;
		}

		/// <summary>
		/// Must be implemented by commands to process the command line flags and
		/// check the bounds of the remaining arguments.
		/// </summary>
		/// <remarks>
		/// Must be implemented by commands to process the command line flags and
		/// check the bounds of the remaining arguments.  If an
		/// IllegalArgumentException is thrown, the FsShell object will print the
		/// short usage of the command.
		/// </remarks>
		/// <param name="args">the command line arguments</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
		}

		/// <summary>Allows commands that don't use paths to handle the raw arguments.</summary>
		/// <remarks>
		/// Allows commands that don't use paths to handle the raw arguments.
		/// Default behavior is to expand the arguments via
		/// <see cref="expandArguments(System.Collections.Generic.LinkedList{E})"/>
		/// and pass the resulting list to
		/// <see cref="processArguments(System.Collections.Generic.LinkedList{E})"/>
		/// 
		/// </remarks>
		/// <param name="args">the list of argument strings</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void processRawArguments(System.Collections.Generic.LinkedList
			<string> args)
		{
			processArguments(expandArguments(args));
		}

		/// <summary>
		/// Expands a list of arguments into
		/// <see cref="PathData"/>
		/// objects.  The default
		/// behavior is to call
		/// <see cref="expandArgument(string)"/>
		/// on each element
		/// which by default globs the argument.  The loop catches IOExceptions,
		/// increments the error count, and displays the exception.
		/// </summary>
		/// <param name="args">
		/// strings to expand into
		/// <see cref="PathData"/>
		/// objects
		/// </param>
		/// <returns>
		/// list of all
		/// <see cref="PathData"/>
		/// objects the arguments
		/// </returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
			> expandArguments(System.Collections.Generic.LinkedList<string> args)
		{
			System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData> expandedArgs
				 = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
				>();
			foreach (string arg in args)
			{
				try
				{
					Sharpen.Collections.AddAll(expandedArgs, expandArgument(arg));
				}
				catch (System.IO.IOException e)
				{
					// other exceptions are probably nasty
					displayError(e);
				}
			}
			return expandedArgs;
		}

		/// <summary>
		/// Expand the given argument into a list of
		/// <see cref="PathData"/>
		/// objects.
		/// The default behavior is to expand globs.  Commands may override to
		/// perform other expansions on an argument.
		/// </summary>
		/// <param name="arg">string pattern to expand</param>
		/// <returns>
		/// list of
		/// <see cref="PathData"/>
		/// objects
		/// </returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
			> expandArgument(string arg)
		{
			org.apache.hadoop.fs.shell.PathData[] items = org.apache.hadoop.fs.shell.PathData
				.expandAsGlob(arg, getConf());
			if (items.Length == 0)
			{
				// it's a glob that failed to match
				throw new org.apache.hadoop.fs.PathNotFoundException(arg);
			}
			return java.util.Arrays.asList(items);
		}

		/// <summary>Processes the command's list of expanded arguments.</summary>
		/// <remarks>
		/// Processes the command's list of expanded arguments.
		/// <see cref="processArgument(PathData)"/>
		/// will be invoked with each item
		/// in the list.  The loop catches IOExceptions, increments the error
		/// count, and displays the exception.
		/// </remarks>
		/// <param name="args">
		/// a list of
		/// <see cref="PathData"/>
		/// to process
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void processArguments(System.Collections.Generic.LinkedList
			<org.apache.hadoop.fs.shell.PathData> args)
		{
			foreach (org.apache.hadoop.fs.shell.PathData arg in args)
			{
				try
				{
					processArgument(arg);
				}
				catch (System.IO.IOException e)
				{
					displayError(e);
				}
			}
		}

		/// <summary>
		/// Processes a
		/// <see cref="PathData"/>
		/// item, calling
		/// <see cref="processPathArgument(PathData)"/>
		/// or
		/// <see cref="processNonexistentPath(PathData)"/>
		/// on each item.
		/// </summary>
		/// <param name="item">
		/// 
		/// <see cref="PathData"/>
		/// item to process
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void processArgument(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			if (item.exists)
			{
				processPathArgument(item);
			}
			else
			{
				processNonexistentPath(item);
			}
		}

		/// <summary>
		/// This is the last chance to modify an argument before going into the
		/// (possibly) recursive
		/// <see cref="processPaths(PathData, PathData[])"/>
		/// -&gt;
		/// <see cref="processPath(PathData)"/>
		/// loop.  Ex.  ls and du use this to
		/// expand out directories.
		/// </summary>
		/// <param name="item">
		/// a
		/// <see cref="PathData"/>
		/// representing a path which exists
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void processPathArgument(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			// null indicates that the call is not via recursion, ie. there is
			// no parent directory that was expanded
			depth = 0;
			processPaths(null, item);
		}

		/// <summary>Provides a hook for handling paths that don't exist.</summary>
		/// <remarks>
		/// Provides a hook for handling paths that don't exist.  By default it
		/// will throw an exception.  Primarily overriden by commands that create
		/// paths such as mkdir or touch.
		/// </remarks>
		/// <param name="item">
		/// the
		/// <see cref="PathData"/>
		/// that doesn't exist
		/// </param>
		/// <exception cref="java.io.FileNotFoundException">if arg is a path and it doesn't exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if anything else goes wrong...</exception>
		protected internal virtual void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			throw new org.apache.hadoop.fs.PathNotFoundException(item.ToString());
		}

		/// <summary>
		/// Iterates over the given expanded paths and invokes
		/// <see cref="processPath(PathData)"/>
		/// on each element.  If "recursive" is true,
		/// will do a post-visit DFS on directories.
		/// </summary>
		/// <param name="parent">if called via a recurse, will be the parent dir, else null</param>
		/// <param name="items">
		/// a list of
		/// <see cref="PathData"/>
		/// objects to process
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void processPaths(org.apache.hadoop.fs.shell.PathData 
			parent, params org.apache.hadoop.fs.shell.PathData[] items)
		{
			// TODO: this really should be iterative
			foreach (org.apache.hadoop.fs.shell.PathData item in items)
			{
				try
				{
					processPath(item);
					if (recursive && isPathRecursable(item))
					{
						recursePath(item);
					}
					postProcessPath(item);
				}
				catch (System.IO.IOException e)
				{
					displayError(e);
				}
			}
		}

		/// <summary>
		/// Determines whether a
		/// <see cref="PathData"/>
		/// item is recursable. Default
		/// implementation is to recurse directories but can be overridden to recurse
		/// through symbolic links.
		/// </summary>
		/// <param name="item">
		/// a
		/// <see cref="PathData"/>
		/// object
		/// </param>
		/// <returns>true if the item is recursable, false otherwise</returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong in the user-implementation
		/// 	</exception>
		protected internal virtual bool isPathRecursable(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			return item.stat.isDirectory();
		}

		/// <summary>
		/// Hook for commands to implement an operation to be applied on each
		/// path for the command.
		/// </summary>
		/// <remarks>
		/// Hook for commands to implement an operation to be applied on each
		/// path for the command.  Note implementation of this method is optional
		/// if earlier methods in the chain handle the operation.
		/// </remarks>
		/// <param name="item">
		/// a
		/// <see cref="PathData"/>
		/// object
		/// </param>
		/// <exception cref="System.Exception">if invoked but not implemented</exception>
		/// <exception cref="System.IO.IOException">if anything else goes wrong in the user-implementation
		/// 	</exception>
		protected internal virtual void processPath(org.apache.hadoop.fs.shell.PathData item
			)
		{
			throw new System.Exception("processPath() is not implemented");
		}

		/// <summary>
		/// Hook for commands to implement an operation to be applied on each
		/// path for the command after being processed successfully
		/// </summary>
		/// <param name="item">
		/// a
		/// <see cref="PathData"/>
		/// object
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void postProcessPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
		}

		/// <summary>
		/// Gets the directory listing for a path and invokes
		/// <see cref="processPaths(PathData, PathData[])"/>
		/// </summary>
		/// <param name="item">
		/// 
		/// <see cref="PathData"/>
		/// for directory to recurse into
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void recursePath(org.apache.hadoop.fs.shell.PathData item
			)
		{
			try
			{
				depth++;
				processPaths(item, item.getDirectoryContents());
			}
			finally
			{
				depth--;
			}
		}

		/// <summary>Display an exception prefaced with the command name.</summary>
		/// <remarks>
		/// Display an exception prefaced with the command name.  Also increments
		/// the error count for the command which will result in a non-zero exit
		/// code.
		/// </remarks>
		/// <param name="e">exception to display</param>
		public virtual void displayError(System.Exception e)
		{
			// build up a list of exceptions that occurred
			exceptions.add(e);
			string errorMessage = e.getLocalizedMessage();
			if (errorMessage == null)
			{
				// this is an unexpected condition, so dump the whole exception since
				// it's probably a nasty internal error where the backtrace would be
				// useful
				errorMessage = org.apache.hadoop.util.StringUtils.stringifyException(e);
				LOG.debug(errorMessage);
			}
			else
			{
				errorMessage = errorMessage.split("\n", 2)[0];
			}
			displayError(errorMessage);
		}

		/// <summary>Display an error string prefaced with the command name.</summary>
		/// <remarks>
		/// Display an error string prefaced with the command name.  Also increments
		/// the error count for the command which will result in a non-zero exit
		/// code.
		/// </remarks>
		/// <param name="message">error message to display</param>
		public virtual void displayError(string message)
		{
			numErrors++;
			displayWarning(message);
		}

		/// <summary>Display an warning string prefaced with the command name.</summary>
		/// <param name="message">warning message to display</param>
		public virtual void displayWarning(string message)
		{
			err.WriteLine(getName() + ": " + message);
		}

		/// <summary>The name of the command.</summary>
		/// <remarks>
		/// The name of the command.  Will first try to use the assigned name
		/// else fallback to the command's preferred name
		/// </remarks>
		/// <returns>name of the command</returns>
		public virtual string getName()
		{
			return (name == null) ? getCommandField("NAME") : name.StartsWith("-") ? Sharpen.Runtime.substring
				(name, 1) : name;
		}

		/// <summary>Define the name of the command.</summary>
		/// <param name="name">as invoked</param>
		public virtual void setName(string name)
		{
			this.name = name;
		}

		/// <summary>The short usage suitable for the synopsis</summary>
		/// <returns>"name options"</returns>
		public virtual string getUsage()
		{
			string cmd = "-" + getName();
			string usage = isDeprecated() ? string.Empty : getCommandField("USAGE");
			return usage.isEmpty() ? cmd : cmd + " " + usage;
		}

		/// <summary>The long usage suitable for help output</summary>
		/// <returns>text of the usage</returns>
		public virtual string getDescription()
		{
			return isDeprecated() ? "(DEPRECATED) Same as '" + getReplacementCommand() + "'" : 
				getCommandField("DESCRIPTION");
		}

		/// <summary>Is the command deprecated?</summary>
		/// <returns>boolean</returns>
		public bool isDeprecated()
		{
			return (getReplacementCommand() != null);
		}

		/// <summary>The replacement for a deprecated command</summary>
		/// <returns>null if not deprecated, else alternative command</returns>
		public virtual string getReplacementCommand()
		{
			return null;
		}

		/// <summary>Get a public static class field</summary>
		/// <param name="field">the field to retrieve</param>
		/// <returns>String of the field</returns>
		private string getCommandField(string field)
		{
			string value;
			try
			{
				java.lang.reflect.Field f = Sharpen.Runtime.getClassForObject(this).getDeclaredField
					(field);
				f.setAccessible(true);
				value = f.get(this).ToString();
			}
			catch (System.Exception e)
			{
				throw new System.Exception("failed to get " + Sharpen.Runtime.getClassForObject(this
					).getSimpleName() + "." + field, e);
			}
			return value;
		}
	}
}
