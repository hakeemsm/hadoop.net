using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Truncates a file to a new size</summary>
	public class Truncate : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Truncate), "-truncate");
		}

		public const string Name = "truncate";

		public const string Usage = "[-w] <length> <path> ...";

		public const string Description = "Truncate all files that match the specified file pattern to the "
			 + "specified length.\n" + "-w: Requests that the command wait for block recovery to complete, "
			 + "if necessary.";

		protected internal long newLength = -1;

		protected internal IList<PathData> waitList = new List<PathData>();

		protected internal bool waitOpt = false;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(2, int.MaxValue, "w");
			cf.Parse(args);
			waitOpt = cf.GetOpt("w");
			try
			{
				newLength = long.Parse(args.RemoveFirst());
			}
			catch (FormatException nfe)
			{
				DisplayWarning("Illegal length, a non-negative integer expected");
				throw;
			}
			if (newLength < 0)
			{
				throw new ArgumentException("length must be >= 0");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessArguments(List<PathData> args)
		{
			base.ProcessArguments(args);
			if (waitOpt)
			{
				WaitForRecovery();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			if (item.stat.IsDirectory())
			{
				throw new PathIsDirectoryException(item.ToString());
			}
			long oldLength = item.stat.GetLen();
			if (newLength > oldLength)
			{
				throw new ArgumentException("Cannot truncate to a larger file size. Current size: "
					 + oldLength + ", truncate size: " + newLength + ".");
			}
			if (item.fs.Truncate(item.path, newLength))
			{
				@out.WriteLine("Truncated " + item + " to length: " + newLength);
			}
			else
			{
				if (waitOpt)
				{
					waitList.AddItem(item);
				}
				else
				{
					@out.WriteLine("Truncating " + item + " to length: " + newLength + ". " + "Wait for block recovery to complete before further updating this "
						 + "file.");
				}
			}
		}

		/// <summary>Wait for all files in waitList to have length equal to newLength.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void WaitForRecovery()
		{
			foreach (PathData item in waitList)
			{
				@out.WriteLine("Waiting for " + item + " ...");
				@out.Flush();
				for (; ; )
				{
					item.RefreshStatus();
					if (item.stat.GetLen() == newLength)
					{
						break;
					}
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				@out.WriteLine("Truncated " + item + " to length: " + newLength);
				@out.Flush();
			}
		}
	}
}
