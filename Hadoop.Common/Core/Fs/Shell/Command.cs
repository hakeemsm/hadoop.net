using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>An abstract class for the execution of a file system command</summary>
	public abstract class Command : Configured
	{
		/// <summary>default name of the command</summary>
		public static string Name;

		/// <summary>the command's usage switches and arguments format</summary>
		public static string Usage;

		/// <summary>the command's long description</summary>
		public static string Description;

		protected internal string[] args;

		protected internal string name;

		protected internal int exitCode = 0;

		protected internal int numErrors = 0;

		protected internal bool recursive = false;

		private int depth = 0;

		protected internal AList<Exception> exceptions = new AList<Exception>();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Shell.Command
			));

		/// <summary>allows stdout to be captured if necessary</summary>
		public TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public TextWriter err = System.Console.Error;

		/// <summary>allows the command factory to be used if necessary</summary>
		private CommandFactory commandFactory = null;

		/// <summary>Constructor</summary>
		protected internal Command()
		{
			@out = System.Console.Out;
			err = System.Console.Error;
		}

		/// <summary>Constructor</summary>
		protected internal Command(Configuration conf)
			: base(conf)
		{
		}

		/// <returns>the command's name excluding the leading character -</returns>
		public abstract string GetCommandName();

		protected internal virtual void SetRecursive(bool flag)
		{
			recursive = flag;
		}

		protected internal virtual bool IsRecursive()
		{
			return recursive;
		}

		protected internal virtual int GetDepth()
		{
			return depth;
		}

		/// <summary>Execute the command on the input path</summary>
		/// <param name="path">the input path</param>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		protected internal abstract void Run(Path path);

		/// <summary>For each source path, execute the command</summary>
		/// <returns>0 if it runs successfully; -1 if it fails</returns>
		public virtual int RunAll()
		{
			int exitCode = 0;
			foreach (string src in args)
			{
				try
				{
					PathData[] srcs = PathData.ExpandAsGlob(src, GetConf());
					foreach (PathData s in srcs)
					{
						Run(s.path);
					}
				}
				catch (IOException e)
				{
					exitCode = -1;
					DisplayError(e);
				}
			}
			return exitCode;
		}

		/// <summary>sets the command factory for later use</summary>
		public virtual void SetCommandFactory(CommandFactory factory)
		{
			this.commandFactory = factory;
		}

		/// <summary>retrieves the command factory</summary>
		protected internal virtual CommandFactory GetCommandFactory()
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
		/// <see cref="ProcessOptions(System.Collections.Generic.List{E})"/>
		/// \-&gt;
		/// <see cref="ProcessRawArguments(System.Collections.Generic.List{E})"/>
		/// |-&gt;
		/// <see cref="ExpandArguments(System.Collections.Generic.List{E})"/>
		/// |   \-&gt;
		/// <see cref="ExpandArgument(string)"/>
		/// \-&gt;
		/// <see cref="ProcessArguments(System.Collections.Generic.List{E})"/>
		/// |-&gt;
		/// <see cref="ProcessArgument(PathData)"/>
		/// |   |-&gt;
		/// <see cref="ProcessPathArgument(PathData)"/>
		/// |   \-&gt;
		/// <see cref="ProcessPaths(PathData, PathData[])"/>
		/// |        \-&gt;
		/// <see cref="ProcessPath(PathData)"/>
		/// \-&gt;
		/// <see cref="ProcessNonexistentPath(PathData)"/>
		/// </pre>
		/// Most commands will chose to implement just
		/// <see cref="ProcessOptions(System.Collections.Generic.List{E})"/>
		/// and
		/// <see cref="ProcessPath(PathData)"/>
		/// </remarks>
		/// <param name="argv">the list of command line arguments</param>
		/// <returns>the exit code for the command</returns>
		/// <exception cref="System.ArgumentException">if called with invalid arguments</exception>
		public virtual int Run(params string[] argv)
		{
			List<string> args = new List<string>(Arrays.AsList(argv));
			try
			{
				if (IsDeprecated())
				{
					DisplayWarning("DEPRECATED: Please use '" + GetReplacementCommand() + "' instead."
						);
				}
				ProcessOptions(args);
				ProcessRawArguments(args);
			}
			catch (IOException e)
			{
				DisplayError(e);
			}
			return (numErrors == 0) ? exitCode : ExitCodeForError();
		}

		/// <summary>The exit code to be returned if any errors occur during execution.</summary>
		/// <remarks>
		/// The exit code to be returned if any errors occur during execution.
		/// This method is needed to account for the inconsistency in the exit
		/// codes returned by various commands.
		/// </remarks>
		/// <returns>a non-zero exit code</returns>
		protected internal virtual int ExitCodeForError()
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
		protected internal virtual void ProcessOptions(List<string> args)
		{
		}

		/// <summary>Allows commands that don't use paths to handle the raw arguments.</summary>
		/// <remarks>
		/// Allows commands that don't use paths to handle the raw arguments.
		/// Default behavior is to expand the arguments via
		/// <see cref="ExpandArguments(System.Collections.Generic.List{E})"/>
		/// and pass the resulting list to
		/// <see cref="ProcessArguments(System.Collections.Generic.List{E})"/>
		/// 
		/// </remarks>
		/// <param name="args">the list of argument strings</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ProcessRawArguments(List<string> args)
		{
			ProcessArguments(ExpandArguments(args));
		}

		/// <summary>
		/// Expands a list of arguments into
		/// <see cref="PathData"/>
		/// objects.  The default
		/// behavior is to call
		/// <see cref="ExpandArgument(string)"/>
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
		protected internal virtual List<PathData> ExpandArguments(List<string> args)
		{
			List<PathData> expandedArgs = new List<PathData>();
			foreach (string arg in args)
			{
				try
				{
					Sharpen.Collections.AddAll(expandedArgs, ExpandArgument(arg));
				}
				catch (IOException e)
				{
					// other exceptions are probably nasty
					DisplayError(e);
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
		protected internal virtual IList<PathData> ExpandArgument(string arg)
		{
			PathData[] items = PathData.ExpandAsGlob(arg, GetConf());
			if (items.Length == 0)
			{
				// it's a glob that failed to match
				throw new PathNotFoundException(arg);
			}
			return Arrays.AsList(items);
		}

		/// <summary>Processes the command's list of expanded arguments.</summary>
		/// <remarks>
		/// Processes the command's list of expanded arguments.
		/// <see cref="ProcessArgument(PathData)"/>
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
		protected internal virtual void ProcessArguments(List<PathData> args)
		{
			foreach (PathData arg in args)
			{
				try
				{
					ProcessArgument(arg);
				}
				catch (IOException e)
				{
					DisplayError(e);
				}
			}
		}

		/// <summary>
		/// Processes a
		/// <see cref="PathData"/>
		/// item, calling
		/// <see cref="ProcessPathArgument(PathData)"/>
		/// or
		/// <see cref="ProcessNonexistentPath(PathData)"/>
		/// on each item.
		/// </summary>
		/// <param name="item">
		/// 
		/// <see cref="PathData"/>
		/// item to process
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void ProcessArgument(PathData item)
		{
			if (item.exists)
			{
				ProcessPathArgument(item);
			}
			else
			{
				ProcessNonexistentPath(item);
			}
		}

		/// <summary>
		/// This is the last chance to modify an argument before going into the
		/// (possibly) recursive
		/// <see cref="ProcessPaths(PathData, PathData[])"/>
		/// -&gt;
		/// <see cref="ProcessPath(PathData)"/>
		/// loop.  Ex.  ls and du use this to
		/// expand out directories.
		/// </summary>
		/// <param name="item">
		/// a
		/// <see cref="PathData"/>
		/// representing a path which exists
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void ProcessPathArgument(PathData item)
		{
			// null indicates that the call is not via recursion, ie. there is
			// no parent directory that was expanded
			depth = 0;
			ProcessPaths(null, item);
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
		/// <exception cref="System.IO.FileNotFoundException">if arg is a path and it doesn't exist
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if anything else goes wrong...</exception>
		protected internal virtual void ProcessNonexistentPath(PathData item)
		{
			throw new PathNotFoundException(item.ToString());
		}

		/// <summary>
		/// Iterates over the given expanded paths and invokes
		/// <see cref="ProcessPath(PathData)"/>
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
		protected internal virtual void ProcessPaths(PathData parent, params PathData[] items
			)
		{
			// TODO: this really should be iterative
			foreach (PathData item in items)
			{
				try
				{
					ProcessPath(item);
					if (recursive && IsPathRecursable(item))
					{
						RecursePath(item);
					}
					PostProcessPath(item);
				}
				catch (IOException e)
				{
					DisplayError(e);
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
		protected internal virtual bool IsPathRecursable(PathData item)
		{
			return item.stat.IsDirectory();
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
		/// <exception cref="Sharpen.RuntimeException">if invoked but not implemented</exception>
		/// <exception cref="System.IO.IOException">if anything else goes wrong in the user-implementation
		/// 	</exception>
		protected internal virtual void ProcessPath(PathData item)
		{
			throw new RuntimeException("processPath() is not implemented");
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
		protected internal virtual void PostProcessPath(PathData item)
		{
		}

		/// <summary>
		/// Gets the directory listing for a path and invokes
		/// <see cref="ProcessPaths(PathData, PathData[])"/>
		/// </summary>
		/// <param name="item">
		/// 
		/// <see cref="PathData"/>
		/// for directory to recurse into
		/// </param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		protected internal virtual void RecursePath(PathData item)
		{
			try
			{
				depth++;
				ProcessPaths(item, item.GetDirectoryContents());
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
		public virtual void DisplayError(Exception e)
		{
			// build up a list of exceptions that occurred
			exceptions.AddItem(e);
			string errorMessage = e.GetLocalizedMessage();
			if (errorMessage == null)
			{
				// this is an unexpected condition, so dump the whole exception since
				// it's probably a nasty internal error where the backtrace would be
				// useful
				errorMessage = StringUtils.StringifyException(e);
				Log.Debug(errorMessage);
			}
			else
			{
				errorMessage = errorMessage.Split("\n", 2)[0];
			}
			DisplayError(errorMessage);
		}

		/// <summary>Display an error string prefaced with the command name.</summary>
		/// <remarks>
		/// Display an error string prefaced with the command name.  Also increments
		/// the error count for the command which will result in a non-zero exit
		/// code.
		/// </remarks>
		/// <param name="message">error message to display</param>
		public virtual void DisplayError(string message)
		{
			numErrors++;
			DisplayWarning(message);
		}

		/// <summary>Display an warning string prefaced with the command name.</summary>
		/// <param name="message">warning message to display</param>
		public virtual void DisplayWarning(string message)
		{
			err.WriteLine(GetName() + ": " + message);
		}

		/// <summary>The name of the command.</summary>
		/// <remarks>
		/// The name of the command.  Will first try to use the assigned name
		/// else fallback to the command's preferred name
		/// </remarks>
		/// <returns>name of the command</returns>
		public virtual string GetName()
		{
			return (name == null) ? GetCommandField("NAME") : name.StartsWith("-") ? Sharpen.Runtime.Substring
				(name, 1) : name;
		}

		/// <summary>Define the name of the command.</summary>
		/// <param name="name">as invoked</param>
		public virtual void SetName(string name)
		{
			this.name = name;
		}

		/// <summary>The short usage suitable for the synopsis</summary>
		/// <returns>"name options"</returns>
		public virtual string GetUsage()
		{
			string cmd = "-" + GetName();
			string usage = IsDeprecated() ? string.Empty : GetCommandField("USAGE");
			return usage.IsEmpty() ? cmd : cmd + " " + usage;
		}

		/// <summary>The long usage suitable for help output</summary>
		/// <returns>text of the usage</returns>
		public virtual string GetDescription()
		{
			return IsDeprecated() ? "(DEPRECATED) Same as '" + GetReplacementCommand() + "'" : 
				GetCommandField("DESCRIPTION");
		}

		/// <summary>Is the command deprecated?</summary>
		/// <returns>boolean</returns>
		public bool IsDeprecated()
		{
			return (GetReplacementCommand() != null);
		}

		/// <summary>The replacement for a deprecated command</summary>
		/// <returns>null if not deprecated, else alternative command</returns>
		public virtual string GetReplacementCommand()
		{
			return null;
		}

		/// <summary>Get a public static class field</summary>
		/// <param name="field">the field to retrieve</param>
		/// <returns>String of the field</returns>
		private string GetCommandField(string field)
		{
			string value;
			try
			{
				FieldInfo f = Sharpen.Runtime.GetDeclaredField(this.GetType(), field);
				value = f.GetValue(this).ToString();
			}
			catch (Exception e)
			{
				throw new RuntimeException("failed to get " + this.GetType().Name + "." + field, 
					e);
			}
			return value;
		}
	}
}
