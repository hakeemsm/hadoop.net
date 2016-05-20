using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Truncates a file to a new size</summary>
	public class Truncate : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Truncate
				)), "-truncate");
		}

		public const string NAME = "truncate";

		public const string USAGE = "[-w] <length> <path> ...";

		public const string DESCRIPTION = "Truncate all files that match the specified file pattern to the "
			 + "specified length.\n" + "-w: Requests that the command wait for block recovery to complete, "
			 + "if necessary.";

		protected internal long newLength = -1;

		protected internal System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
			> waitList = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
			>();

		protected internal bool waitOpt = false;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(2, int.MaxValue, "w");
			cf.parse(args);
			waitOpt = cf.getOpt("w");
			try
			{
				newLength = long.Parse(args.removeFirst());
			}
			catch (java.lang.NumberFormatException nfe)
			{
				displayWarning("Illegal length, a non-negative integer expected");
				throw;
			}
			if (newLength < 0)
			{
				throw new System.ArgumentException("length must be >= 0");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processArguments(System.Collections.Generic.LinkedList
			<org.apache.hadoop.fs.shell.PathData> args)
		{
			base.processArguments(args);
			if (waitOpt)
			{
				waitForRecovery();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (item.stat.isDirectory())
			{
				throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
			}
			long oldLength = item.stat.getLen();
			if (newLength > oldLength)
			{
				throw new System.ArgumentException("Cannot truncate to a larger file size. Current size: "
					 + oldLength + ", truncate size: " + newLength + ".");
			}
			if (item.fs.truncate(item.path, newLength))
			{
				@out.WriteLine("Truncated " + item + " to length: " + newLength);
			}
			else
			{
				if (waitOpt)
				{
					waitList.add(item);
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
		private void waitForRecovery()
		{
			foreach (org.apache.hadoop.fs.shell.PathData item in waitList)
			{
				@out.WriteLine("Waiting for " + item + " ...");
				@out.flush();
				for (; ; )
				{
					item.refreshStatus();
					if (item.stat.getLen() == newLength)
					{
						break;
					}
					try
					{
						java.lang.Thread.sleep(1000);
					}
					catch (System.Exception)
					{
					}
				}
				@out.WriteLine("Truncated " + item + " to length: " + newLength);
				@out.flush();
			}
		}
	}
}
