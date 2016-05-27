using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Get a listing of all files in that match the file patterns.</summary>
	internal class Tail : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Tail), "-tail");
		}

		public const string Name = "tail";

		public const string Usage = "[-f] <file>";

		public const string Description = "Show the last 1KB of the file.\n" + "-f: Shows appended data as the file grows.\n";

		private long startingOffset = -1024;

		private bool follow = false;

		private long followDelay = 5000;

		// milliseconds
		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(1, 1, "f");
			cf.Parse(args);
			follow = cf.GetOpt("f");
		}

		// TODO: HADOOP-7234 will add glob support; for now, be backwards compat
		/// <exception cref="System.IO.IOException"/>
		protected internal override IList<PathData> ExpandArgument(string arg)
		{
			IList<PathData> items = new List<PathData>();
			items.AddItem(new PathData(arg, GetConf()));
			return items;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			if (item.stat.IsDirectory())
			{
				throw new PathIsDirectoryException(item.ToString());
			}
			long offset = DumpFromOffset(item, startingOffset);
			while (follow)
			{
				try
				{
					Sharpen.Thread.Sleep(followDelay);
				}
				catch (Exception)
				{
					break;
				}
				offset = DumpFromOffset(item, offset);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long DumpFromOffset(PathData item, long offset)
		{
			long fileSize = item.RefreshStatus().GetLen();
			if (offset > fileSize)
			{
				return fileSize;
			}
			// treat a negative offset as relative to end of the file, floor of 0
			if (offset < 0)
			{
				offset = Math.Max(fileSize + offset, 0);
			}
			FSDataInputStream @in = item.fs.Open(item.path);
			try
			{
				@in.Seek(offset);
				// use conf so the system configured io block size is used
				IOUtils.CopyBytes(@in, System.Console.Out, GetConf(), false);
				offset = @in.GetPos();
			}
			finally
			{
				@in.Close();
			}
			return offset;
		}
	}
}
