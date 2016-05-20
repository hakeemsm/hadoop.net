using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>
	/// Base class for commands related to viewing filesystem usage, such as
	/// du and df
	/// </summary>
	internal class FsUsage : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.FsUsage.Df
				)), "-df");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.FsUsage.Du
				)), "-du");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.FsUsage.Dus
				)), "-dus");
		}

		protected internal bool humanReadable = false;

		protected internal org.apache.hadoop.fs.shell.FsUsage.TableBuilder usagesTable;

		protected internal virtual string formatSize(long size)
		{
			return humanReadable ? org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.long2String(size, string.Empty, 1) : Sharpen.Runtime.getStringValueOf(size);
		}

		/// <summary>Show the size of a partition in the filesystem</summary>
		public class Df : org.apache.hadoop.fs.shell.FsUsage
		{
			public const string NAME = "df";

			public const string USAGE = "[-h] [<path> ...]";

			public const string DESCRIPTION = "Shows the capacity, free and used space of the filesystem. "
				 + "If the filesystem has multiple partitions, and no path to a " + "particular partition is specified, then the status of the root "
				 + "partitions will be shown.\n" + "-h: Formats the sizes of files in a human-readable fashion "
				 + "rather than a number of bytes.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, int.MaxValue, "h");
				cf.parse(args);
				humanReadable = cf.getOpt("h");
				if (args.isEmpty())
				{
					args.add(org.apache.hadoop.fs.Path.SEPARATOR);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processArguments(System.Collections.Generic.LinkedList
				<org.apache.hadoop.fs.shell.PathData> args)
			{
				usagesTable = new org.apache.hadoop.fs.shell.FsUsage.TableBuilder("Filesystem", "Size"
					, "Used", "Available", "Use%");
				usagesTable.setRightAlign(1, 2, 3, 4);
				base.processArguments(args);
				if (!usagesTable.isEmpty())
				{
					usagesTable.printToStream(@out);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				org.apache.hadoop.fs.FsStatus fsStats = item.fs.getStatus(item.path);
				long size = fsStats.getCapacity();
				long used = fsStats.getUsed();
				long free = fsStats.getRemaining();
				usagesTable.addRow(item.fs.getUri(), formatSize(size), formatSize(used), formatSize
					(free), org.apache.hadoop.util.StringUtils.formatPercent((double)used / (double)
					size, 0));
			}
		}

		/// <summary>show disk usage</summary>
		public class Du : org.apache.hadoop.fs.shell.FsUsage
		{
			public const string NAME = "du";

			public const string USAGE = "[-s] [-h] <path> ...";

			public const string DESCRIPTION = "Show the amount of space, in bytes, used by the files that "
				 + "match the specified file pattern. The following flags are optional:\n" + "-s: Rather than showing the size of each individual file that"
				 + " matches the pattern, shows the total (summary) size.\n" + "-h: Formats the sizes of files in a human-readable fashion"
				 + " rather than a number of bytes.\n\n" + "Note that, even without the -s option, this only shows size summaries "
				 + "one level deep into a directory.\n\n" + "The output is in the form \n" + "\tsize\tname(full path)\n";

			protected internal bool summary = false;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(0, int.MaxValue, "h", "s");
				cf.parse(args);
				humanReadable = cf.getOpt("h");
				summary = cf.getOpt("s");
				if (args.isEmpty())
				{
					args.add(org.apache.hadoop.fs.Path.CUR_DIR);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPathArgument(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				usagesTable = new org.apache.hadoop.fs.shell.FsUsage.TableBuilder(2);
				// go one level deep on dirs from cmdline unless in summary mode
				if (!summary && item.stat.isDirectory())
				{
					recursePath(item);
				}
				else
				{
					base.processPathArgument(item);
				}
				usagesTable.printToStream(@out);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				long length;
				if (item.stat.isDirectory())
				{
					length = item.fs.getContentSummary(item.path).getLength();
				}
				else
				{
					length = item.stat.getLen();
				}
				usagesTable.addRow(formatSize(length), item);
			}
		}

		/// <summary>show disk usage summary</summary>
		public class Dus : org.apache.hadoop.fs.shell.FsUsage.Du
		{
			public const string NAME = "dus";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				args.addFirst("-s");
				base.processOptions(args);
			}

			public override string getReplacementCommand()
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

			protected internal System.Collections.Generic.IList<string[]> rows;

			protected internal int[] widths;

			protected internal bool[] rightAlign;

			/// <summary>Create a table w/o headers</summary>
			/// <param name="columns">number of columns</param>
			public TableBuilder(int columns)
			{
				rows = new System.Collections.Generic.List<string[]>();
				widths = new int[columns];
				rightAlign = new bool[columns];
			}

			/// <summary>Create a table with headers</summary>
			/// <param name="headers">list of headers</param>
			public TableBuilder(params object[] headers)
				: this(headers.Length)
			{
				this.addRow(headers);
				hasHeader = true;
			}

			/// <summary>Change the default left-align of columns</summary>
			/// <param name="indexes">of columns to right align</param>
			public virtual void setRightAlign(params int[] indexes)
			{
				foreach (int i in indexes)
				{
					rightAlign[i] = true;
				}
			}

			/// <summary>Add a row of objects to the table</summary>
			/// <param name="objects">the values</param>
			public virtual void addRow(params object[] objects)
			{
				string[] row = new string[widths.Length];
				for (int col = 0; col < widths.Length; col++)
				{
					row[col] = Sharpen.Runtime.getStringValueOf(objects[col]);
					widths[col] = System.Math.max(widths[col], row[col].Length);
				}
				rows.add(row);
			}

			/// <summary>Render the table to a stream</summary>
			/// <param name="out">PrintStream for output</param>
			public virtual void printToStream(System.IO.TextWriter @out)
			{
				if (isEmpty())
				{
					return;
				}
				java.lang.StringBuilder fmt = new java.lang.StringBuilder();
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
					@out.WriteLine(string.format(fmt.ToString(), row));
				}
			}

			/// <summary>Number of rows excluding header</summary>
			/// <returns>rows</returns>
			public virtual int size()
			{
				return rows.Count - (hasHeader ? 1 : 0);
			}

			/// <summary>Does table have any rows</summary>
			/// <returns>boolean</returns>
			public virtual bool isEmpty()
			{
				return size() == 0;
			}
		}
	}
}
