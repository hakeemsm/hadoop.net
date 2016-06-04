using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Modifies the replication factor</summary>
	internal class SetReplication : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(SetReplication), "-setrep");
		}

		public const string Name = "setrep";

		public const string Usage = "[-R] [-w] <rep> <path> ...";

		public const string Description = "Set the replication level of a file. If <path> is a directory "
			 + "then the command recursively changes the replication factor of " + "all files under the directory tree rooted at <path>.\n"
			 + "-w: It requests that the command waits for the replication " + "to complete. This can potentially take a very long time.\n"
			 + "-R: It is accepted for backwards compatibility. It has no effect.";

		protected internal short newRep = 0;

		protected internal IList<PathData> waitList = new List<PathData>();

		protected internal bool waitOpt = false;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessOptions(List<string> args)
		{
			CommandFormat cf = new CommandFormat(2, int.MaxValue, "R", "w");
			cf.Parse(args);
			waitOpt = cf.GetOpt("w");
			SetRecursive(true);
			try
			{
				newRep = short.ParseShort(args.RemoveFirst());
			}
			catch (FormatException nfe)
			{
				DisplayWarning("Illegal replication, a positive integer expected");
				throw;
			}
			if (newRep < 1)
			{
				throw new ArgumentException("replication must be >= 1");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessArguments(List<PathData> args)
		{
			base.ProcessArguments(args);
			if (waitOpt)
			{
				WaitForReplication();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData item)
		{
			if (item.stat.IsSymlink())
			{
				throw new PathIOException(item.ToString(), "Symlinks unsupported");
			}
			if (item.stat.IsFile())
			{
				if (!item.fs.SetReplication(item.path, newRep))
				{
					throw new IOException("Could not set replication for: " + item);
				}
				@out.WriteLine("Replication " + newRep + " set: " + item);
				if (waitOpt)
				{
					waitList.AddItem(item);
				}
			}
		}

		/// <summary>Wait for all files in waitList to have replication number equal to rep.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void WaitForReplication()
		{
			foreach (PathData item in waitList)
			{
				@out.Write("Waiting for " + item + " ...");
				@out.Flush();
				bool printedWarning = false;
				bool done = false;
				while (!done)
				{
					item.RefreshStatus();
					BlockLocation[] locations = item.fs.GetFileBlockLocations(item.stat, 0, item.stat
						.GetLen());
					int i = 0;
					for (; i < locations.Length; i++)
					{
						int currentRep = locations[i].GetHosts().Length;
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
					@out.Flush();
					try
					{
						Sharpen.Thread.Sleep(10000);
					}
					catch (Exception)
					{
					}
				}
				@out.WriteLine(" done");
			}
		}
	}
}
