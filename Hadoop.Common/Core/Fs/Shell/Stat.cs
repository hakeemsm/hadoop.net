using System;
using System.Collections.Generic;
using System.Text;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Shell
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
	internal class Stat : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Stat), "-stat");
		}

		private static readonly string Newline = Runtime.GetProperty("line.separator");

		public const string Name = "stat";

		public const string Usage = "[format] <path> ...";

		public static readonly string Description = "Print statistics about the file/directory at <path>"
			 + Newline + "in the specified format. Format accepts filesize in" + Newline + "blocks (%b), type (%F), group name of owner (%g),"
			 + Newline + "name (%n), block size (%o), replication (%r), user name" + Newline
			 + "of owner (%u), modification date (%y, %Y)." + Newline + "%y shows UTC date as \"yyyy-MM-dd HH:mm:ss\" and"
			 + Newline + "%Y shows milliseconds since January 1, 1970 UTC." + Newline + "If the format is not specified, %y is used by default."
			 + Newline;

		protected internal readonly SimpleDateFormat timeFmt;

		protected internal string format = "%y";

		// default format string
		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, int.MaxValue, "R");
			cf.Parse(args);
			SetRecursive(cf.GetOpt("R"));
			if (args.GetFirst().Contains("%"))
			{
				format = args.RemoveFirst();
			}
			cf.Parse(args);
		}

		// make sure there's still at least one arg
		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			FileStatus stat = item.stat;
			StringBuilder buf = new StringBuilder();
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
							buf.Append(stat.GetLen());
							break;
						}

						case 'F':
						{
							buf.Append(stat.IsDirectory() ? "directory" : (stat.IsFile() ? "regular file" : "symlink"
								));
							break;
						}

						case 'g':
						{
							buf.Append(stat.GetGroup());
							break;
						}

						case 'n':
						{
							buf.Append(item.path.GetName());
							break;
						}

						case 'o':
						{
							buf.Append(stat.GetBlockSize());
							break;
						}

						case 'r':
						{
							buf.Append(stat.GetReplication());
							break;
						}

						case 'u':
						{
							buf.Append(stat.GetOwner());
							break;
						}

						case 'y':
						{
							buf.Append(timeFmt.Format(Extensions.CreateDate(stat.GetModificationTime(
								))));
							break;
						}

						case 'Y':
						{
							buf.Append(stat.GetModificationTime());
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
				timeFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				timeFmt.SetTimeZone(Extensions.GetTimeZone("UTC"));
			}
		}
	}
}
