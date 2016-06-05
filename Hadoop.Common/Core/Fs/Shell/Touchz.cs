using System.Collections.Generic;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Unix touch like commands</summary>
	internal class Touch : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Touch.Touchz), "-touchz");
		}

		/// <summary>(Re)create zero-length file at the specified path.</summary>
		/// <remarks>
		/// (Re)create zero-length file at the specified path.
		/// This will be replaced by a more UNIX-like touch when files may be
		/// modified.
		/// </remarks>
		public class Touchz : Touch
		{
			public const string Name = "touchz";

			public const string Usage = "<path> ...";

			public const string Description = "Creates a file of zero length " + "at <path> with current time as the timestamp of that <path>. "
				 + "An error is returned if the file exists with non-zero length\n";

			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue);
				cf.Parse(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (item.stat.IsDirectory())
				{
					// TODO: handle this
					throw new PathIsDirectoryException(item.ToString());
				}
				if (item.stat.GetLen() != 0)
				{
					throw new PathIOException(item.ToString(), "Not a zero-length file");
				}
				Touchz(item);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessNonexistentPath(PathData item)
			{
				if (!item.ParentExists())
				{
					throw new PathNotFoundException(item.ToString());
				}
				Touchz(item);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Touchz(PathData item)
			{
				item.fs.Create(item.path).Close();
			}
		}
	}
}
