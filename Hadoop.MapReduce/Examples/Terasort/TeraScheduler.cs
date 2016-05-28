using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Server.Tasktracker;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	internal class TeraScheduler
	{
		internal static string Use = "mapreduce.terasort.use.terascheduler";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Examples.Terasort.TeraScheduler
			));

		private TeraScheduler.Split[] splits;

		private IList<TeraScheduler.Host> hosts = new AList<TeraScheduler.Host>();

		private int slotsPerHost;

		private int remainingSplits = 0;

		private FileSplit[] realSplits = null;

		internal class Split
		{
			internal string filename;

			internal bool isAssigned = false;

			internal IList<TeraScheduler.Host> locations = new AList<TeraScheduler.Host>();

			internal Split(string filename)
			{
				this.filename = filename;
			}

			public override string ToString()
			{
				StringBuilder result = new StringBuilder();
				result.Append(filename);
				result.Append(" on ");
				foreach (TeraScheduler.Host host in locations)
				{
					result.Append(host.hostname);
					result.Append(", ");
				}
				return result.ToString();
			}
		}

		internal class Host
		{
			internal string hostname;

			internal IList<TeraScheduler.Split> splits = new AList<TeraScheduler.Split>();

			internal Host(string hostname)
			{
				this.hostname = hostname;
			}

			public override string ToString()
			{
				StringBuilder result = new StringBuilder();
				result.Append(splits.Count);
				result.Append(" ");
				result.Append(hostname);
				return result.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<string> ReadFile(string filename)
		{
			IList<string> result = new AList<string>(10000);
			BufferedReader @in = new BufferedReader(new InputStreamReader(new FileInputStream
				(filename), Charsets.Utf8));
			string line = @in.ReadLine();
			while (line != null)
			{
				result.AddItem(line);
				line = @in.ReadLine();
			}
			@in.Close();
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public TeraScheduler(string splitFilename, string nodeFilename)
		{
			slotsPerHost = 4;
			// get the hosts
			IDictionary<string, TeraScheduler.Host> hostIds = new Dictionary<string, TeraScheduler.Host
				>();
			foreach (string hostName in ReadFile(nodeFilename))
			{
				TeraScheduler.Host host = new TeraScheduler.Host(hostName);
				hosts.AddItem(host);
				hostIds[hostName] = host;
			}
			// read the blocks
			IList<string> splitLines = ReadFile(splitFilename);
			splits = new TeraScheduler.Split[splitLines.Count];
			remainingSplits = 0;
			foreach (string line in splitLines)
			{
				StringTokenizer itr = new StringTokenizer(line);
				TeraScheduler.Split newSplit = new TeraScheduler.Split(itr.NextToken());
				splits[remainingSplits++] = newSplit;
				while (itr.HasMoreTokens())
				{
					TeraScheduler.Host host = hostIds[itr.NextToken()];
					newSplit.locations.AddItem(host);
					host.splits.AddItem(newSplit);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public TeraScheduler(FileSplit[] realSplits, Configuration conf)
		{
			this.realSplits = realSplits;
			this.slotsPerHost = conf.GetInt(TTConfig.TtMapSlots, 4);
			IDictionary<string, TeraScheduler.Host> hostTable = new Dictionary<string, TeraScheduler.Host
				>();
			splits = new TeraScheduler.Split[realSplits.Length];
			foreach (FileSplit realSplit in realSplits)
			{
				TeraScheduler.Split split = new TeraScheduler.Split(realSplit.GetPath().ToString(
					));
				splits[remainingSplits++] = split;
				foreach (string hostname in realSplit.GetLocations())
				{
					TeraScheduler.Host host = hostTable[hostname];
					if (host == null)
					{
						host = new TeraScheduler.Host(hostname);
						hostTable[hostname] = host;
						hosts.AddItem(host);
					}
					host.splits.AddItem(split);
					split.locations.AddItem(host);
				}
			}
		}

		internal virtual TeraScheduler.Host PickBestHost()
		{
			TeraScheduler.Host result = null;
			int splits = int.MaxValue;
			foreach (TeraScheduler.Host host in hosts)
			{
				if (host.splits.Count < splits)
				{
					result = host;
					splits = host.splits.Count;
				}
			}
			if (result != null)
			{
				hosts.Remove(result);
				Log.Debug("picking " + result);
			}
			return result;
		}

		internal virtual void PickBestSplits(TeraScheduler.Host host)
		{
			int tasksToPick = Math.Min(slotsPerHost, (int)Math.Ceil((double)remainingSplits /
				 hosts.Count));
			TeraScheduler.Split[] best = new TeraScheduler.Split[tasksToPick];
			foreach (TeraScheduler.Split cur in host.splits)
			{
				Log.Debug("  examine: " + cur.filename + " " + cur.locations.Count);
				int i = 0;
				while (i < tasksToPick && best[i] != null && best[i].locations.Count <= cur.locations
					.Count)
				{
					i += 1;
				}
				if (i < tasksToPick)
				{
					for (int j = tasksToPick - 1; j > i; --j)
					{
						best[j] = best[j - 1];
					}
					best[i] = cur;
				}
			}
			// for the chosen blocks, remove them from the other locations
			for (int i_1 = 0; i_1 < tasksToPick; ++i_1)
			{
				if (best[i_1] != null)
				{
					Log.Debug(" best: " + best[i_1].filename);
					foreach (TeraScheduler.Host other in best[i_1].locations)
					{
						other.splits.Remove(best[i_1]);
					}
					best[i_1].locations.Clear();
					best[i_1].locations.AddItem(host);
					best[i_1].isAssigned = true;
					remainingSplits -= 1;
				}
			}
			// for the non-chosen blocks, remove this host
			foreach (TeraScheduler.Split cur_1 in host.splits)
			{
				if (!cur_1.isAssigned)
				{
					cur_1.locations.Remove(host);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Solve()
		{
			TeraScheduler.Host host = PickBestHost();
			while (host != null)
			{
				PickBestSplits(host);
				host = PickBestHost();
			}
		}

		/// <summary>
		/// Solve the schedule and modify the FileSplit array to reflect the new
		/// schedule.
		/// </summary>
		/// <remarks>
		/// Solve the schedule and modify the FileSplit array to reflect the new
		/// schedule. It will move placed splits to front and unplacable splits
		/// to the end.
		/// </remarks>
		/// <returns>
		/// a new list of FileSplits that are modified to have the
		/// best host as the only host.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<InputSplit> GetNewFileSplits()
		{
			Solve();
			FileSplit[] result = new FileSplit[realSplits.Length];
			int left = 0;
			int right = realSplits.Length - 1;
			for (int i = 0; i < splits.Length; ++i)
			{
				if (splits[i].isAssigned)
				{
					// copy the split and fix up the locations
					string[] newLocations = new string[] { splits[i].locations[0].hostname };
					realSplits[i] = new FileSplit(realSplits[i].GetPath(), realSplits[i].GetStart(), 
						realSplits[i].GetLength(), newLocations);
					result[left++] = realSplits[i];
				}
				else
				{
					result[right--] = realSplits[i];
				}
			}
			IList<InputSplit> ret = new AList<InputSplit>();
			foreach (FileSplit fs in result)
			{
				ret.AddItem(fs);
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			TeraScheduler problem = new TeraScheduler("block-loc.txt", "nodes");
			foreach (TeraScheduler.Host host in problem.hosts)
			{
				System.Console.Out.WriteLine(host);
			}
			Log.Info("starting solve");
			problem.Solve();
			IList<TeraScheduler.Split> leftOvers = new AList<TeraScheduler.Split>();
			for (int i = 0; i < problem.splits.Length; ++i)
			{
				if (problem.splits[i].isAssigned)
				{
					System.Console.Out.WriteLine("sched: " + problem.splits[i]);
				}
				else
				{
					leftOvers.AddItem(problem.splits[i]);
				}
			}
			foreach (TeraScheduler.Split cur in leftOvers)
			{
				System.Console.Out.WriteLine("left: " + cur);
			}
			System.Console.Out.WriteLine("left over: " + leftOvers.Count);
			Log.Info("done");
		}
	}
}
