using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>
	/// Base class for commands related to viewing filesystem usage, such as
	/// du and df
	/// </summary>
	internal class FsUsage : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(FsUsage.DF), "-df");
			factory.AddClass(typeof(FsUsage.DU), "-du");
			factory.AddClass(typeof(FsUsage.Dus), "-dus");
		}

		protected internal bool humanReadable = false;

		protected internal FsUsage.TableBuilder usagesTable;

		protected internal virtual string FormatSize(long size)
		{
			return humanReadable ? StringUtils.TraditionalBinaryPrefix.Long2String(size, string.Empty
				, 1) : size.ToString();
		}

		/// <summary>Show the size of a partition in the filesystem</summary>
		public class DF : FsUsage
		{
			public const string Name = "df";

			public const string Usage = "[-h] [<path> ...]";

			public const string Description = "Shows the capacity, free and used space of the filesystem. "
				 + "If the filesystem has multiple partitions, and no path to a " + "particular partition is specified, then the status of the root "
				 + "partitions will be shown.\n" + "-h: Formats the sizes of files in a human-readable fashion "
				 + "rather than a number of bytes.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(0, int.MaxValue, "h");
				cf.Parse(args);
				humanReadable = cf.GetOpt("h");
				if (args.IsEmpty())
				{
					args.AddItem(Path.Separator);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessArguments(List<PathData> args)
			{
				usagesTable = new FsUsage.TableBuilder("Filesystem", "Size", "Used", "Available", 
					"Use%");
				usagesTable.SetRightAlign(1, 2, 3, 4);
				base.ProcessArguments(args);
				if (!usagesTable.IsEmpty())
				{
					usagesTable.PrintToStream(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				FsStatus fsStats = item.fs.GetStatus(item.path);
				long size = fsStats.GetCapacity();
				long used = fsStats.GetUsed();
				long free = fsStats.GetRemaining();
				usagesTable.AddRow(item.fs.GetUri(), FormatSize(size), FormatSize(used), FormatSize
					(free), StringUtils.FormatPercent((double)used / (double)size, 0));
			}
		}

		/// <summary>show disk usage</summary>
		public class DU : FsUsage
		{
			public const string Name = "du";

			public const string Usage = "[-s] [-h] <path> ...";

			public const string Description = "Show the amount of space, in bytes, used by the files that "
				 + "match the specified file pattern. The following flags are optional:\n" + "-s: Rather than showing the size of each individual file that"
				 + " matches the pattern, shows the total (summary) size.\n" + "-h: Formats the sizes of files in a human-readable fashion"
				 + " rather than a number of bytes.\n\n" + "Note that, even without the -s option, this only shows size summaries "
				 + "one level deep into a directory.\n\n" + "The output is in the form \n" + "\tsize\tname(full path)\n";

			protected internal bool summary = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(0, int.MaxValue, "h", "s");
				cf.Parse(args);
				humanReadable = cf.GetOpt("h");
				summary = cf.GetOpt("s");
				if (args.IsEmpty())
				{
					args.AddItem(Path.CurDir);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPathArgument(PathData item)
			{
				usagesTable = new FsUsage.TableBuilder(2);
				// go one level deep on dirs from cmdline unless in summary mode
				if (!summary && item.stat.IsDirectory())
				{
					RecursePath(item);
				}
				else
				{
					base.ProcessPathArgument(item);
				}
				usagesTable.PrintToStream(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				long length;
				if (item.stat.IsDirectory())
				{
					length = item.fs.GetContentSummary(item.path).GetLength();
				}
				else
				{
					length = item.stat.GetLen();
				}
				usagesTable.AddRow(FormatSize(length), item);
			}
		}

		/// <summary>show disk usage summary</summary>
		public class Dus : FsUsage.DU
		{
			public const string Name = "dus";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				args.AddFirst("-s");
				base.ProcessOptions(args);
			}

			public override string GetReplacementCommand()
			{
				return "du -s";
			}
		}

		/// <summary>
		/// Creates a table of aligned values based on the maximum width of each
		/// column as a string
		/// </summary>
		private class TableBuilder
		{
			protected internal bool hasHeader = false;

			protected internal IList<string[]> rows;

			protected internal int[] widths;

			protected internal bool[] rightAlign;

			/// <summary>Create a table w/o headers</summary>
			/// <param name="columns">number of columns</param>
			public TableBuilder(int columns)
			{
				rows = new AList<string[]>();
				widths = new int[columns];
				rightAlign = new bool[columns];
			}

			/// <summary>Create a table with headers</summary>
			/// <param name="headers">list of headers</param>
			public TableBuilder(params object[] headers)
				: this(headers.Length)
			{
				this.AddRow(headers);
				hasHeader = true;
			}

			/// <summary>Change the default left-align of columns</summary>
			/// <param name="indexes">of columns to right align</param>
			public virtual void SetRightAlign(params int[] indexes)
			{
				foreach (int i in indexes)
				{
					rightAlign[i] = true;
				}
			}

			/// <summary>Add a row of objects to the table</summary>
			/// <param name="objects">the values</param>
			public virtual void AddRow(params object[] objects)
			{
				string[] row = new string[widths.Length];
				for (int col = 0; col < widths.Length; col++)
				{
					row[col] = objects[col].ToString();
					widths[col] = Math.Max(widths[col], row[col].Length);
				}
				rows.AddItem(row);
			}

			/// <summary>Render the table to a stream</summary>
			/// <param name="out">PrintStream for output</param>
			public virtual void PrintToStream(TextWriter @out)
			{
				if (IsEmpty())
				{
					return;
				}
				StringBuilder fmt = new StringBuilder();
				for (int i = 0; i < widths.Length; i++)
				{
					if (fmt.Length != 0)
					{
						fmt.Append("  ");
					}
					if (rightAlign[i])
					{
						fmt.Append("%" + widths[i] + "s");
					}
					else
					{
						if (i != widths.Length - 1)
						{
							fmt.Append("%-" + widths[i] + "s");
						}
						else
						{
							// prevent trailing spaces if the final column is left-aligned
							fmt.Append("%s");
						}
					}
				}
				foreach (object[] row in rows)
				{
					@out.WriteLine(string.Format(fmt.ToString(), row));
				}
			}

			/// <summary>Number of rows excluding header</summary>
			/// <returns>rows</returns>
			public virtual int Size()
			{
				return rows.Count - (hasHeader ? 1 : 0);
			}

			/// <summary>Does table have any rows</summary>
			/// <returns>boolean</returns>
			public virtual bool IsEmpty()
			{
				return Size() == 0;
			}
		}
	}
}
