using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Modifies the replication factor</summary>
	internal class SetReplication : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.SetReplication
				)), "-setrep");
		}

		public const string NAME = "setrep";

		public const string USAGE = "[-R] [-w] <rep> <path> ...";

		public const string DESCRIPTION = "Set the replication level of a file. If <path> is a directory "
			 + "then the command recursively changes the replication factor of " + "all files under the directory tree rooted at <path>.\n"
			 + "-w: It requests that the command waits for the replication " + "to complete. This can potentially take a very long time.\n"
			 + "-R: It is accepted for backwards compatibility. It has no effect.";

		protected internal short newRep = 0;

		protected internal System.Collections.Generic.IList<org.apache.hadoop.fs.shell.PathData
			> waitList = new System.Collections.Generic.LinkedList<org.apache.hadoop.fs.shell.PathData
			>();

		protected internal bool waitOpt = false;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processOptions(System.Collections.Generic.LinkedList
			<string> args)
		{
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(2, int.MaxValue, "R", "w");
			cf.parse(args);
			waitOpt = cf.getOpt("w");
			setRecursive(true);
			try
			{
				newRep = short.parseShort(args.removeFirst());
			}
			catch (java.lang.NumberFormatException nfe)
			{
				displayWarning("Illegal replication, a positive integer expected");
				throw;
			}
			if (newRep < 1)
			{
				throw new System.ArgumentException("replication must be >= 1");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processArguments(System.Collections.Generic.LinkedList
			<org.apache.hadoop.fs.shell.PathData> args)
		{
			base.processArguments(args);
			if (waitOpt)
			{
				waitForReplication();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			item)
		{
			if (item.stat.isSymlink())
			{
				throw new org.apache.hadoop.fs.PathIOException(item.ToString(), "Symlinks unsupported"
					);
			}
			if (item.stat.isFile())
			{
				if (!item.fs.setReplication(item.path, newRep))
				{
					throw new System.IO.IOException("Could not set replication for: " + item);
				}
				@out.WriteLine("Replication " + newRep + " set: " + item);
				if (waitOpt)
				{
					waitList.add(item);
				}
			}
		}

		/// <summary>Wait for all files in waitList to have replication number equal to rep.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void waitForReplication()
		{
			foreach (org.apache.hadoop.fs.shell.PathData item in waitList)
			{
				@out.Write("Waiting for " + item + " ...");
				@out.flush();
				bool printedWarning = false;
				bool done = false;
				while (!done)
				{
					item.refreshStatus();
					org.apache.hadoop.fs.BlockLocation[] locations = item.fs.getFileBlockLocations(item
						.stat, 0, item.stat.getLen());
					int i = 0;
					for (; i < locations.Length; i++)
					{
						int currentRep = locations[i].getHosts().Length;
						if (currentRep != newRep)
						{
							if (!printedWarning && currentRep > newRep)
							{
								@out.WriteLine("\nWARNING: the waiting time may be long for " + "DECREASING the number of replications."
									);
								printedWarning = true;
							}
							break;
						}
					}
					done = i == locations.Length;
					if (done)
					{
						break;
					}
					@out.Write(".");
					@out.flush();
					try
					{
						java.lang.Thread.sleep(10000);
					}
					catch (System.Exception)
					{
					}
				}
				@out.WriteLine(" done");
			}
		}
	}
}
