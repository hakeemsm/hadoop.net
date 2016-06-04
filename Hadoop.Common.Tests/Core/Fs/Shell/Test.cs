using System;
using System.Collections.Generic;
using Hadoop.Common.Core.Fs.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Perform shell-like file tests</summary>
	internal class Test : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Org.Apache.Hadoop.FS.Shell.Test), "-test");
		}

		public const string Name = "test";

		public const string Usage = "-[defsz] <path>";

		public const string Description = "Answer various questions about <path>, with result via exit status.\n"
			 + "  -d  return 0 if <path> is a directory.\n" + "  -e  return 0 if <path> exists.\n"
			 + "  -f  return 0 if <path> is a file.\n" + "  -s  return 0 if file <path> is greater than zero bytes in size.\n"
			 + "  -z  return 0 if file <path> is zero bytes in size, else return 1.";

		private char flag;

		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, 1, "e", "d", "f", "s", "z");
			cf.Parse(args);
			string[] opts = Sharpen.Collections.ToArray(cf.GetOpts(), new string[0]);
			switch (opts.Length)
			{
				case 0:
				{
					throw new ArgumentException("No test flag given");
				}

				case 1:
				{
					flag = opts[0][0];
					break;
				}

				default:
				{
					throw new ArgumentException("Only one test flag is allowed");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			bool test = false;
			switch (flag)
			{
				case 'e':
				{
					test = true;
					break;
				}

				case 'd':
				{
					test = item.stat.IsDirectory();
					break;
				}

				case 'f':
				{
					test = item.stat.IsFile();
					break;
				}

				case 's':
				{
					test = (item.stat.GetLen() > 0);
					break;
				}

				case 'z':
				{
					test = (item.stat.GetLen() == 0);
					break;
				}

				default:
				{
					break;
				}
			}
			if (!test)
			{
				exitCode = 1;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessNonexistentPath(PathData item)
		{
			exitCode = 1;
		}
	}
}
