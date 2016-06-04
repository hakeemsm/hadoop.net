using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Various commands for moving files</summary>
	internal class MoveCommands
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(MoveCommands.MoveFromLocal), "-moveFromLocal");
			factory.AddClass(typeof(MoveCommands.MoveToLocal), "-moveToLocal");
			factory.AddClass(typeof(MoveCommands.Rename), "-mv");
		}

		/// <summary>Move local files to a remote filesystem</summary>
		public class MoveFromLocal : CopyCommands.CopyFromLocal
		{
			public const string Name = "moveFromLocal";

			public const string Usage = "<localsrc> ... <dst>";

			public const string Description = "Same as -put, except that the source is " + "deleted after it's copied.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData src, PathData target)
			{
				// unlike copy, don't merge existing dirs during move
				if (target.exists && target.stat.IsDirectory())
				{
					throw new PathExistsException(target.ToString());
				}
				base.ProcessPath(src, target);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void PostProcessPath(PathData src)
			{
				if (!src.fs.Delete(src.path, false))
				{
					// we have no way to know the actual error...
					PathIOException e = new PathIOException(src.ToString());
					e.SetOperation("remove");
					throw e;
				}
			}
		}

		/// <summary>Move remote files to a local filesystem</summary>
		public class MoveToLocal : FsCommand
		{
			public const string Name = "moveToLocal";

			public const string Usage = "<src> <localdst>";

			public const string Description = "Not implemented yet";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				throw new IOException("Option '-moveToLocal' is not implemented yet.");
			}
		}

		/// <summary>move/rename paths on the same fileystem</summary>
		public class Rename : CommandWithDestination
		{
			public const string Name = "mv";

			public const string Usage = "<src> ... <dst>";

			public const string Description = "Move files that match the specified file pattern <src> "
				 + "to a destination <dst>.  When moving multiple files, the " + "destination must be a directory.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(2, int.MaxValue);
				cf.Parse(args);
				GetRemoteDestination(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData src, PathData target)
			{
				if (!src.fs.GetUri().Equals(target.fs.GetUri()))
				{
					throw new PathIOException(src.ToString(), "Does not match target filesystem");
				}
				if (target.exists)
				{
					throw new PathExistsException(target.ToString());
				}
				if (!target.fs.Rename(src.path, target.path))
				{
					// we have no way to know the actual error...
					throw new PathIOException(src.ToString());
				}
			}
		}
	}
}
