using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Classes that delete paths</summary>
	internal class Delete
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Delete.RM), "-rm");
			factory.AddClass(typeof(Delete.Rmdir), "-rmdir");
			factory.AddClass(typeof(Delete.Rmr), "-rmr");
			factory.AddClass(typeof(Delete.Expunge), "-expunge");
		}

		/// <summary>remove non-directory paths</summary>
		public class RM : FsCommand
		{
			public const string Name = "rm";

			public const string Usage = "[-f] [-r|-R] [-skipTrash] <src> ...";

			public const string Description = "Delete all files that match the specified file pattern. "
				 + "Equivalent to the Unix command \"rm <src>\"\n" + "-skipTrash: option bypasses trash, if enabled, and immediately "
				 + "deletes <src>\n" + "-f: If the file does not exist, do not display a diagnostic "
				 + "message or modify the exit status to reflect an error.\n" + "-[rR]:  Recursively deletes directories";

			private bool skipTrash = false;

			private bool deleteDirs = false;

			private bool ignoreFNF = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue, "f", "r", "R", "skipTrash");
				cf.Parse(args);
				ignoreFNF = cf.GetOpt("f");
				deleteDirs = cf.GetOpt("r") || cf.GetOpt("R");
				skipTrash = cf.GetOpt("skipTrash");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override IList<PathData> ExpandArgument(string arg)
			{
				try
				{
					return base.ExpandArgument(arg);
				}
				catch (PathNotFoundException e)
				{
					if (!ignoreFNF)
					{
						throw;
					}
					// prevent -f on a non-existent glob from failing
					return new List<PathData>();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessNonexistentPath(PathData item)
			{
				if (!ignoreFNF)
				{
					base.ProcessNonexistentPath(item);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (item.stat.IsDirectory() && !deleteDirs)
				{
					throw new PathIsDirectoryException(item.ToString());
				}
				// TODO: if the user wants the trash to be used but there is any
				// problem (ie. creating the trash dir, moving the item to be deleted,
				// etc), then the path will just be deleted because moveToTrash returns
				// false and it falls thru to fs.delete.  this doesn't seem right
				if (MoveToTrash(item))
				{
					return;
				}
				if (!item.fs.Delete(item.path, deleteDirs))
				{
					throw new PathIOException(item.ToString());
				}
				@out.WriteLine("Deleted " + item);
			}

			/// <exception cref="System.IO.IOException"/>
			private bool MoveToTrash(PathData item)
			{
				bool success = false;
				if (!skipTrash)
				{
					try
					{
						success = Trash.MoveToAppropriateTrash(item.fs, item.path, GetConf());
					}
					catch (FileNotFoundException fnfe)
					{
						throw;
					}
					catch (IOException ioe)
					{
						string msg = ioe.Message;
						if (ioe.InnerException != null)
						{
							msg += ": " + ioe.InnerException.Message;
						}
						throw new IOException(msg + ". Consider using -skipTrash option", ioe);
					}
				}
				return success;
			}
		}

		/// <summary>remove any path</summary>
		internal class Rmr : Delete.RM
		{
			public const string Name = "rmr";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				args.AddFirst("-r");
				base.ProcessOptions(args);
			}

			public override string GetReplacementCommand()
			{
				return "rm -r";
			}
		}

		/// <summary>remove only empty directories</summary>
		internal class Rmdir : FsCommand
		{
			public const string Name = "rmdir";

			public const string Usage = "[--ignore-fail-on-non-empty] <dir> ...";

			public const string Description = "Removes the directory entry specified by each directory argument, "
				 + "provided it is empty.\n";

			private bool ignoreNonEmpty = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue, "-ignore-fail-on-non-empty"
					);
				cf.Parse(args);
				ignoreNonEmpty = cf.GetOpt("-ignore-fail-on-non-empty");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (!item.stat.IsDirectory())
				{
					throw new PathIsNotDirectoryException(item.ToString());
				}
				if (item.fs.ListStatus(item.path).Length == 0)
				{
					if (!item.fs.Delete(item.path, false))
					{
						throw new PathIOException(item.ToString());
					}
				}
				else
				{
					if (!ignoreNonEmpty)
					{
						throw new PathIsNotEmptyDirectoryException(item.ToString());
					}
				}
			}
		}

		/// <summary>empty the trash</summary>
		internal class Expunge : FsCommand
		{
			public const string Name = "expunge";

			public const string Usage = string.Empty;

			public const string Description = "Empty the Trash";

			// TODO: should probably allow path arguments for the filesystems
			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(0, 0);
				cf.Parse(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> args)
			{
				Trash trash = new Trash(GetConf());
				trash.Expunge();
				trash.Checkpoint();
			}
		}
	}
}
