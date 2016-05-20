using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Get a listing of all files in that match the file patterns.</summary>
	internal class Tail : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Tail
				)), "-tail");
		}

		public const string NAME = "tail";

		public const string USAGE = "[-f] <file>";

		public const string DESCRIPTION = "Show the last 1KB of the file.\n" + "-f: Shows appended data as the file grows.\n";

		private long startingOffset = -1024;

		private bool follow = false;

		private long followDelay = 5000;

		// milliseconds
		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(1, 1, "f");
			cf.parse(args);
			follow = cf.getOpt("f");
		}

		// TODO: HADOOP-7234 will add glob support; for now, be backwards compat
		/// <exception cref="System.IO.IOException"/>
		protected internal override System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
			> expandArgument(string arg)
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData> items = new 
				System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData>();
			items.add(new org.apache.hadoop.fs.shell.PathData(arg, getConf()));
			return items;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (item.stat.isDirectory())
			{
				throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
			}
			long offset = dumpFromOffset(item, startingOffset);
			while (follow)
			{
				try
				{
					java.lang.Thread.sleep(followDelay);
				}
				catch (System.Exception)
				{
					break;
				}
				offset = dumpFromOffset(item, offset);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long dumpFromOffset(org.apache.hadoop.fs.shell.PathData item, long offset
			)
		{
			long fileSize = item.refreshStatus().getLen();
			if (offset > fileSize)
			{
				return fileSize;
			}
			// treat a negative offset as relative to end of the file, floor of 0
			if (offset < 0)
			{
				offset = System.Math.max(fileSize + offset, 0);
			}
			org.apache.hadoop.fs.FSDataInputStream @in = item.fs.open(item.path);
			try
			{
				@in.seek(offset);
				// use conf so the system configured io block size is used
				org.apache.hadoop.io.IOUtils.copyBytes(@in, System.Console.Out, getConf(), false);
				offset = @in.getPos();
			}
			finally
			{
				@in.close();
			}
			return offset;
		}
	}
}
