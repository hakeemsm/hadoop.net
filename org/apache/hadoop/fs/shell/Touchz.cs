using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Unix touch like commands</summary>
	internal class Touch : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Touch.Touchz
				)), "-touchz");
		}

		/// <summary>(Re)create zero-length file at the specified path.</summary>
		/// <remarks>
		/// (Re)create zero-length file at the specified path.
		/// This will be replaced by a more UNIX-like touch when files may be
		/// modified.
		/// </remarks>
		public class Touchz : org.apache.hadoop.fs.shell.Touch
		{
			public const string NAME = "touchz";

			public const string USAGE = "<path> ...";

			public const string DESCRIPTION = "Creates a file of zero length " + "at <path> with current time as the timestamp of that <path>. "
				 + "An error is returned if the file exists with non-zero length\n";

			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue);
				cf.parse(args);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (item.stat.isDirectory())
				{
					// TODO: handle this
					throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
				}
				if (item.stat.getLen() != 0)
				{
					throw new org.apache.hadoop.fs.PathIOException(item.ToString(), "Not a zero-length file"
						);
				}
				touchz(item);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				if (!item.parentExists())
				{
					throw new org.apache.hadoop.fs.PathNotFoundException(item.ToString());
				}
				touchz(item);
			}

			/// <exception cref="System.IO.IOException"/>
			private void touchz(org.apache.hadoop.fs.shell.PathData item)
			{
				item.fs.create(item.path).close();
			}
		}
	}
}
