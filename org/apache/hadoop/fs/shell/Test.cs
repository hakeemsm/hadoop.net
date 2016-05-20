using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Perform shell-like file tests</summary>
	internal class Test : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Test
				)), "-test");
		}

		public const string NAME = "test";

		public const string USAGE = "-[defsz] <path>";

		public const string DESCRIPTION = "Answer various questions about <path>, with result via exit status.\n"
			 + "  -d  return 0 if <path> is a directory.\n" + "  -e  return 0 if <path> exists.\n"
			 + "  -f  return 0 if <path> is a file.\n" + "  -s  return 0 if file <path> is greater than zero bytes in size.\n"
			 + "  -z  return 0 if file <path> is zero bytes in size, else return 1.";

		private char flag;

		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, 1, "e", "d", "f", "s", "z");
			cf.parse(args);
			string[] opts = Sharpen.Collections.ToArray(cf.getOpts(), new string[0]);
			switch (opts.Length)
			{
				case 0:
				{
					throw new System.ArgumentException("No test flag given");
				}

				case 1:
				{
					flag = opts[0][0];
					break;
				}

				default:
				{
					throw new System.ArgumentException("Only one test flag is allowed");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
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
					test = item.stat.isDirectory();
					break;
				}

				case 'f':
				{
					test = item.stat.isFile();
					break;
				}

				case 's':
				{
					test = (item.stat.getLen() > 0);
					break;
				}

				case 'z':
				{
					test = (item.stat.getLen() == 0);
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
		protected internal override void processNonexistentPath(org.apache.hadoop.fs.shell.PathData
			 item)
		{
			exitCode = 1;
		}
	}
}
