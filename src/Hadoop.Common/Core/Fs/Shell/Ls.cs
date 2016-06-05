using System;
using System.Collections.Generic;
using System.Text;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Get a listing of all files in that match the file patterns.</summary>
	internal class LS : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(LS), "-ls");
			factory.AddClass(typeof(LS.Lsr), "-lsr");
		}

		public const string Name = "ls";

		public const string Usage = "[-d] [-h] [-R] [<path> ...]";

		public const string Description = "List the contents that match the specified file pattern. If "
			 + "path is not specified, the contents of /user/<currentUser> " + "will be listed. Directory entries are of the form:\n"
			 + "\tpermissions - userId groupId sizeOfDirectory(in bytes) modificationDate(yyyy-MM-dd HH:mm) directoryName\n\n"
			 + "and file entries are of the form:\n" + "\tpermissions numberOfReplicas userId groupId sizeOfFile(in bytes) modificationDate(yyyy-MM-dd HH:mm) fileName\n"
			 + "-d:  Directories are listed as plain files.\n" + "-h:  Formats the sizes of files in a human-readable fashion "
			 + "rather than a number of bytes.\n" + "-R:  Recursively list the contents of directories.";

		protected internal readonly SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm"
			);

		protected internal int maxRepl = 3;

		protected internal int maxLen = 10;

		protected internal int maxOwner = 0;

		protected internal int maxGroup = 0;

		protected internal string lineFormat;

		protected internal bool dirRecurse;

		protected internal bool humanReadable = false;

		protected internal virtual string FormatSize(long size)
		{
			return humanReadable ? StringUtils.TraditionalBinaryPrefix.Long2String(size, string.Empty
				, 1) : size.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(0, int.MaxValue, "d", "h", "R");
			cf.Parse(args);
			dirRecurse = !cf.GetOpt("d");
			SetRecursive(cf.GetOpt("R") && dirRecurse);
			humanReadable = cf.GetOpt("h");
			if (args.IsEmpty())
			{
				args.AddItem(Path.CurDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPathArgument(PathData item)
		{
			// implicitly recurse once for cmdline directories
			if (dirRecurse && item.stat.IsDirectory())
			{
				RecursePath(item);
			}
			else
			{
				base.ProcessPathArgument(item);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPaths(PathData parent, params PathData[] 
			items)
		{
			if (parent != null && !IsRecursive() && items.Length != 0)
			{
				@out.WriteLine("Found " + items.Length + " items");
			}
			AdjustColumnWidths(items);
			base.ProcessPaths(parent, items);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			FileStatus stat = item.stat;
			string line = string.Format(lineFormat, (stat.IsDirectory() ? "d" : "-"), stat.GetPermission
				() + (stat.GetPermission().GetAclBit() ? "+" : " "), (stat.IsFile() ? stat.GetReplication
				() : "-"), stat.GetOwner(), stat.GetGroup(), FormatSize(stat.GetLen()), dateFormat
				.Format(Extensions.CreateDate(stat.GetModificationTime())), item);
			@out.WriteLine(line);
		}

		/// <summary>Compute column widths and rebuild the format string</summary>
		/// <param name="items">to find the max field width for each column</param>
		private void AdjustColumnWidths(PathData[] items)
		{
			foreach (PathData item in items)
			{
				FileStatus stat = item.stat;
				maxRepl = MaxLength(maxRepl, stat.GetReplication());
				maxLen = MaxLength(maxLen, stat.GetLen());
				maxOwner = MaxLength(maxOwner, stat.GetOwner());
				maxGroup = MaxLength(maxGroup, stat.GetGroup());
			}
			StringBuilder fmt = new StringBuilder();
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

		private int MaxLength(int n, object value)
		{
			return Math.Max(n, (value != null) ? value.ToString().Length : 0);
		}

		/// <summary>Get a recursive listing of all files in that match the file patterns.</summary>
		/// <remarks>
		/// Get a recursive listing of all files in that match the file patterns.
		/// Same as "-ls -R"
		/// </remarks>
		public class Lsr : LS
		{
			public const string Name = "lsr";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				args.AddFirst("-R");
				base.ProcessOptions(args);
			}

			public override string GetReplacementCommand()
			{
				return "ls -R";
			}
		}
	}
}
