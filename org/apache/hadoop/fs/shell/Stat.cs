using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Print statistics about path in specified format.</summary>
	/// <remarks>
	/// Print statistics about path in specified format.
	/// Format sequences:<br />
	/// %b: Size of file in blocks<br />
	/// %F: Type<br />
	/// %g: Group name of owner<br />
	/// %n: Filename<br />
	/// %o: Block size<br />
	/// %r: replication<br />
	/// %u: User name of owner<br />
	/// %y: UTC date as &quot;yyyy-MM-dd HH:mm:ss&quot;<br />
	/// %Y: Milliseconds since January 1, 1970 UTC<br />
	/// If the format is not specified, %y is used by default.
	/// </remarks>
	internal class Stat : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Stat
				)), "-stat");
		}

		private static readonly string NEWLINE = Sharpen.Runtime.getProperty("line.separator"
			);

		public const string NAME = "stat";

		public const string USAGE = "[format] <path> ...";

		public static readonly string DESCRIPTION = "Print statistics about the file/directory at <path>"
			 + NEWLINE + "in the specified format. Format accepts filesize in" + NEWLINE + "blocks (%b), type (%F), group name of owner (%g),"
			 + NEWLINE + "name (%n), block size (%o), replication (%r), user name" + NEWLINE
			 + "of owner (%u), modification date (%y, %Y)." + NEWLINE + "%y shows UTC date as \"yyyy-MM-dd HH:mm:ss\" and"
			 + NEWLINE + "%Y shows milliseconds since January 1, 1970 UTC." + NEWLINE + "If the format is not specified, %y is used by default."
			 + NEWLINE;

		protected internal readonly java.text.SimpleDateFormat timeFmt;

		protected internal string format = "%y";

		// default format string
		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, int.MaxValue, "R");
			cf.parse(args);
			setRecursive(cf.getOpt("R"));
			if (args.getFirst().contains("%"))
			{
				format = args.removeFirst();
			}
			cf.parse(args);
		}

		// make sure there's still at least one arg
		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			org.apache.hadoop.fs.FileStatus stat = item.stat;
			java.lang.StringBuilder buf = new java.lang.StringBuilder();
			char[] fmt = format.ToCharArray();
			for (int i = 0; i < fmt.Length; ++i)
			{
				if (fmt[i] != '%')
				{
					buf.Append(fmt[i]);
				}
				else
				{
					// this silently drops a trailing %?
					if (i + 1 == fmt.Length)
					{
						break;
					}
					switch (fmt[++i])
					{
						case 'b':
						{
							buf.Append(stat.getLen());
							break;
						}

						case 'F':
						{
							buf.Append(stat.isDirectory() ? "directory" : (stat.isFile() ? "regular file" : "symlink"
								));
							break;
						}

						case 'g':
						{
							buf.Append(stat.getGroup());
							break;
						}

						case 'n':
						{
							buf.Append(item.path.getName());
							break;
						}

						case 'o':
						{
							buf.Append(stat.getBlockSize());
							break;
						}

						case 'r':
						{
							buf.Append(stat.getReplication());
							break;
						}

						case 'u':
						{
							buf.Append(stat.getOwner());
							break;
						}

						case 'y':
						{
							buf.Append(timeFmt.format(new System.DateTime(stat.getModificationTime())));
							break;
						}

						case 'Y':
						{
							buf.Append(stat.getModificationTime());
							break;
						}

						default:
						{
							// this leaves %<unknown> alone, which causes the potential for
							// future format options to break strings; should use %% to
							// escape percents
							buf.Append(fmt[i]);
							break;
						}
					}
				}
			}
			@out.WriteLine(buf.ToString());
		}

		public Stat()
		{
			{
				timeFmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				timeFmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
			}
		}
	}
}
