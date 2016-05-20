using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Various commands for moving files</summary>
	internal class MoveCommands
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.MoveCommands.MoveFromLocal
				)), "-moveFromLocal");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.MoveCommands.MoveToLocal
				)), "-moveToLocal");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.MoveCommands.Rename
				)), "-mv");
		}

		/// <summary>Move local files to a remote filesystem</summary>
		public class MoveFromLocal : org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal
		{
			public const string NAME = "moveFromLocal";

			public const string USAGE = "<localsrc> ... <dst>";

			public const string DESCRIPTION = "Same as -put, except that the source is " + "deleted after it's copied.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				src, org.apache.hadoop.fs.shell.PathData target)
			{
				// unlike copy, don't merge existing dirs during move
				if (target.exists && target.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathExistsException(target.ToString());
				}
				base.processPath(src, target);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void postProcessPath(org.apache.hadoop.fs.shell.PathData
				 src)
			{
				if (!src.fs.delete(src.path, false))
				{
					// we have no way to know the actual error...
					org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
						(src.ToString());
					e.setOperation("remove");
					throw e;
				}
			}
		}

		/// <summary>Move remote files to a local filesystem</summary>
		public class MoveToLocal : org.apache.hadoop.fs.shell.FsCommand
		{
			public const string NAME = "moveToLocal";

			public const string USAGE = "<src> <localdst>";

			public const string DESCRIPTION = "Not implemented yet";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				throw new System.IO.IOException("Option '-moveToLocal' is not implemented yet.");
			}
		}

		/// <summary>move/rename paths on the same fileystem</summary>
		public class Rename : org.apache.hadoop.fs.shell.CommandWithDestination
		{
			public const string NAME = "mv";

			public const string USAGE = "<src> ... <dst>";

			public const string DESCRIPTION = "Move files that match the specified file pattern <src> "
				 + "to a destination <dst>.  When moving multiple files, the " + "destination must be a directory.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(2, int.MaxValue);
				cf.parse(args);
				getRemoteDestination(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				src, org.apache.hadoop.fs.shell.PathData target)
			{
				if (!src.fs.getUri().Equals(target.fs.getUri()))
				{
					throw new org.apache.hadoop.fs.PathIOException(src.ToString(), "Does not match target filesystem"
						);
				}
				if (target.exists)
				{
					throw new org.apache.hadoop.fs.PathExistsException(target.ToString());
				}
				if (!target.fs.rename(src.path, target.path))
				{
					// we have no way to know the actual error...
					throw new org.apache.hadoop.fs.PathIOException(src.ToString());
				}
			}
		}
	}
}
