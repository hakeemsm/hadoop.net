using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Get a listing of all files in that match the file patterns.</summary>
	internal class Ls : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Ls
				)), "-ls");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Ls.Lsr
				)), "-lsr");
		}

		public const string NAME = "ls";

		public const string USAGE = "[-d] [-h] [-R] [<path> ...]";

		public const string DESCRIPTION = "List the contents that match the specified file pattern. If "
			 + "path is not specified, the contents of /user/<currentUser> " + "will be listed. Directory entries are of the form:\n"
			 + "\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\n"
			 + "and file entries are of the form:\n" + "\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n"
			 + "-d:  Directories are listed as plain files.\n" + "-h:  Formats the sizes of files in a human-readable fashion "
			 + "rather than a number of bytes.\n" + "-R:  Recursively list the contents of directories.";

		protected internal readonly java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat
			("yyyy-MM-dd HH:mm");

		protected internal int maxRepl = 3;

		protected internal int maxLen = 10;

		protected internal int maxOwner = 0;

		protected internal int maxGroup = 0;

		protected internal string lineFormat;

		protected internal bool dirRecurse;

		protected internal bool humanReadable = false;

		protected internal virtual string formatSize(long size)
		{
			return humanReadable ? org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.long2String(size, string.Empty, 1) : Sharpen.Runtime.getStringValueOf(size);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(0, int.MaxValue, "d", "h", "R");
			cf.parse(args);
			dirRecurse = !cf.getOpt("d");
			setRecursive(cf.getOpt("R") && dirRecurse);
			humanReadable = cf.getOpt("h");
			if (args.isEmpty())
			{
				args.add(org.apache.hadoop.fs.Path.CUR_DIR);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPathArgument(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			// implicitly recurse once for cmdline directories
			if (dirRecurse && item.stat.isDirectory())
			{
				recursePath(item);
			}
			else
			{
				base.processPathArgument(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPaths(org.apache.hadoop.fs.shell.PathData
			 parent, params org.apache.hadoop.fs.shell.PathData[] items)
		{
			if (parent != null && !isRecursive() && items.Length != 0)
			{
				@out.WriteLine("Found " + items.Length + " items");
			}
			adjustColumnWidths(items);
			base.processPaths(parent, items);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			org.apache.hadoop.fs.FileStatus stat = item.stat;
			string line = string.format(lineFormat, (stat.isDirectory() ? "d" : "-"), stat.getPermission
				() + (stat.getPermission().getAclBit() ? "+" : " "), (stat.isFile() ? stat.getReplication
				() : "-"), stat.getOwner(), stat.getGroup(), formatSize(stat.getLen()), dateFormat
				.format(new System.DateTime(stat.getModificationTime())), item);
			@out.WriteLine(line);
		}

		/// <summary>Compute column widths and rebuild the format string</summary>
		/// <param name="items">to find the max field width for each column</param>
		private void adjustColumnWidths(org.apache.hadoop.fs.shell.PathData[] items)
		{
			foreach (org.apache.hadoop.fs.shell.PathData item in items)
			{
				org.apache.hadoop.fs.FileStatus stat = item.stat;
				maxRepl = maxLength(maxRepl, stat.getReplication());
				maxLen = maxLength(maxLen, stat.getLen());
				maxOwner = maxLength(maxOwner, stat.getOwner());
				maxGroup = maxLength(maxGroup, stat.getGroup());
			}
			java.lang.StringBuilder fmt = new java.lang.StringBuilder();
			fmt.Append("%s%s");
			// permission string
			fmt.Append("%" + maxRepl + "s ");
			// Do not use '%-0s' as a formatting conversion, since it will throw a
			// a MissingFormatWidthException if it is used in String.format().
			// http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Formatter.html#intFlags
			fmt.Append((maxOwner > 0) ? "%-" + maxOwner + "s " : "%s");
			fmt.Append((maxGroup > 0) ? "%-" + maxGroup + "s " : "%s");
			fmt.Append("%" + maxLen + "s ");
			fmt.Append("%s %s");
			// mod time & path
			lineFormat = fmt.ToString();
		}

		private int maxLength(int n, object value)
		{
			return System.Math.max(n, (value != null) ? Sharpen.Runtime.getStringValueOf(value
				).Length : 0);
		}

		/// <summary>Get a recursive listing of all files in that match the file patterns.</summary>
		/// <remarks>
		/// Get a recursive listing of all files in that match the file patterns.
		/// Same as "-ls -R"
		/// </remarks>
		public class Lsr : org.apache.hadoop.fs.shell.Ls
		{
			public const string NAME = "lsr";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				args.addFirst("-R");
				base.processOptions(args);
			}

			public override string getReplacementCommand()
			{
				return "ls -R";
			}
		}
	}
}
