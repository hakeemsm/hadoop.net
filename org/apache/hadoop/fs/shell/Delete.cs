using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Classes that delete paths</summary>
	internal class Delete
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Delete.Rm
				)), "-rm");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Delete.Rmdir
				)), "-rmdir");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Delete.Rmr
				)), "-rmr");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Delete.Expunge
				)), "-expunge");
		}

		/// <summary>remove non-directory paths</summary>
		public class Rm : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "rm";

			public const string USAGE = "[-f] [-r|-R] [-skipTrash] <src> ...";

			public const string DESCRIPTION = "Delete all files that match the specified file pattern. "
				 + "Equivalent to the Unix command \"rm <src>\"\n" + "-skipTrash: option bypasses trash, if enabled, and immediately "
				 + "deletes <src>\n" + "-f: If the file does not exist, do not display a diagnostic "
				 + "message or modify the exit status to reflect an error.\n" + "-[rR]:  Recursively deletes directories";

			private bool skipTrash = false;

			private bool deleteDirs = false;

			private bool ignoreFNF = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue, "f", "r", "R", "skipTrash");
				cf.parse(args);
				ignoreFNF = cf.getOpt("f");
				deleteDirs = cf.getOpt("r") || cf.getOpt("R");
				skipTrash = cf.getOpt("skipTrash");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
				> expandArgument(string arg)
			{
				try
				{
					return base.expandArgument(arg);
				}
				catch (org.apache.hadoop.fs.PathNotFoundException e)
				{
					if (!ignoreFNF)
					{
						throw;
					}
					// prevent -f on a non-existent glob from failing
					return new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
						>();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				if (!ignoreFNF)
				{
					base.processNonexistentPath(item);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (item.stat.isDirectory() && !deleteDirs)
				{
					throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
				}
				// TODO: if the user wants the trash to be used but there is any
				// problem (ie. creating the trash dir, moving the item to be deleted,
				// etc), then the path will just be deleted because moveToTrash returns
				// false and it falls thru to fs.delete.  this doesn't seem right
				if (moveToTrash(item))
				{
					return;
				}
				if (!item.fs.delete(item.path, deleteDirs))
				{
					throw new org.apache.hadoop.fs.PathIOException(item.ToString());
				}
				@out.WriteLine("Deleted " + item);
			}

			/// <exception cref="System.IO.IOException"/>
			private bool moveToTrash(org.apache.hadoop.fs.shell.PathData item)
			{
				bool success = false;
				if (!skipTrash)
				{
					try
					{
						success = org.apache.hadoop.fs.Trash.moveToAppropriateTrash(item.fs, item.path, getConf
							());
					}
					catch (java.io.FileNotFoundException fnfe)
					{
						throw;
					}
					catch (System.IO.IOException ioe)
					{
						string msg = ioe.Message;
						if (ioe.InnerException != null)
						{
							msg += ": " + ioe.InnerException.Message;
						}
						throw new System.IO.IOException(msg + ". Consider using -skipTrash option", ioe);
					}
				}
				return success;
			}
		}

		/// <summary>remove any path</summary>
		internal class Rmr : org.apache.hadoop.fs.shell.Delete.Rm
		{
			public const string NAME = "rmr";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				args.addFirst("-r");
				base.processOptions(args);
			}

			public override string getReplacementCommand()
			{
				return "rm -r";
			}
		}

		/// <summary>remove only empty directories</summary>
		internal class Rmdir : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "rmdir";

			public const string USAGE = "[--ignore-fail-on-non-empty] <dir> ...";

			public const string DESCRIPTION = "Removes the directory entry specified by each directory argument, "
				 + "provided it is empty.\n";

			private bool ignoreNonEmpty = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue, "-ignore-fail-on-non-empty");
				cf.parse(args);
				ignoreNonEmpty = cf.getOpt("-ignore-fail-on-non-empty");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (!item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsNotDirectoryException(item.ToString());
				}
				if (item.fs.listStatus(item.path).Length == 0)
				{
					if (!item.fs.delete(item.path, false))
					{
						throw new org.apache.hadoop.fs.PathIOException(item.ToString());
					}
				}
				else
				{
					if (!ignoreNonEmpty)
					{
						throw new org.apache.hadoop.fs.PathIsNotEmptyDirectoryException(item.ToString());
					}
				}
			}
		}

		/// <summary>empty the trash</summary>
		internal class Expunge : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "expunge";

			public const string USAGE = string.Empty;

			public const string DESCRIPTION = "Empty the Trash";

			// TODO: should probably allow path arguments for the filesystems
			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, 0);
				cf.parse(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> args)
			{
				org.apache.hadoop.fs.Trash trash = new org.apache.hadoop.fs.Trash(getConf());
				trash.expunge();
				trash.checkpoint();
			}
		}
	}
}
