using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>
	/// <p>The balancer is a tool that balances disk space usage on an HDFS cluster
	/// when some datanodes become full or when new empty nodes join the cluster.
	/// </summary>
	/// <remarks>
	/// <p>The balancer is a tool that balances disk space usage on an HDFS cluster
	/// when some datanodes become full or when new empty nodes join the cluster.
	/// The tool is deployed as an application program that can be run by the
	/// cluster administrator on a live HDFS cluster while applications
	/// adding and deleting files.
	/// <p>SYNOPSIS
	/// <pre>
	/// To start:
	/// bin/start-balancer.sh [-threshold <threshold>]
	/// Example: bin/ start-balancer.sh
	/// start the balancer with a default threshold of 10%
	/// bin/ start-balancer.sh -threshold 5
	/// start the balancer with a threshold of 5%
	/// bin/ start-balancer.sh -idleiterations 20
	/// start the balancer with maximum 20 consecutive idle iterations
	/// bin/ start-balancer.sh -idleiterations -1
	/// run the balancer with default threshold infinitely
	/// To stop:
	/// bin/ stop-balancer.sh
	/// </pre>
	/// <p>DESCRIPTION
	/// <p>The threshold parameter is a fraction in the range of (1%, 100%) with a
	/// default value of 10%. The threshold sets a target for whether the cluster
	/// is balanced. A cluster is balanced if for each datanode, the utilization
	/// of the node (ratio of used space at the node to total capacity of the node)
	/// differs from the utilization of the (ratio of used space in the cluster
	/// to total capacity of the cluster) by no more than the threshold value.
	/// The smaller the threshold, the more balanced a cluster will become.
	/// It takes more time to run the balancer for small threshold values.
	/// Also for a very small threshold the cluster may not be able to reach the
	/// balanced state when applications write and delete files concurrently.
	/// <p>The tool moves blocks from highly utilized datanodes to poorly
	/// utilized datanodes iteratively. In each iteration a datanode moves or
	/// receives no more than the lesser of 10G bytes or the threshold fraction
	/// of its capacity. Each iteration runs no more than 20 minutes.
	/// At the end of each iteration, the balancer obtains updated datanodes
	/// information from the namenode.
	/// <p>A system property that limits the balancer's use of bandwidth is
	/// defined in the default configuration file:
	/// <pre>
	/// <property>
	/// <name>dfs.balance.bandwidthPerSec</name>
	/// <value>1048576</value>
	/// <description>  Specifies the maximum bandwidth that each datanode
	/// can utilize for the balancing purpose in term of the number of bytes
	/// per second. </description>
	/// </property>
	/// </pre>
	/// <p>This property determines the maximum speed at which a block will be
	/// moved from one datanode to another. The default value is 1MB/s. The higher
	/// the bandwidth, the faster a cluster can reach the balanced state,
	/// but with greater competition with application processes. If an
	/// administrator changes the value of this property in the configuration
	/// file, the change is observed when HDFS is next restarted.
	/// <p>MONITERING BALANCER PROGRESS
	/// <p>After the balancer is started, an output file name where the balancer
	/// progress will be recorded is printed on the screen.  The administrator
	/// can monitor the running of the balancer by reading the output file.
	/// The output shows the balancer's status iteration by iteration. In each
	/// iteration it prints the starting time, the iteration number, the total
	/// number of bytes that have been moved in the previous iterations,
	/// the total number of bytes that are left to move in order for the cluster
	/// to be balanced, and the number of bytes that are being moved in this
	/// iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left
	/// To Move" is decreasing.
	/// <p>Running multiple instances of the balancer in an HDFS cluster is
	/// prohibited by the tool.
	/// <p>The balancer automatically exits when any of the following five
	/// conditions is satisfied:
	/// <ol>
	/// <li>The cluster is balanced;
	/// <li>No block can be moved;
	/// <li>No block has been moved for specified consecutive iterations (5 by default);
	/// <li>An IOException occurs while communicating with the namenode;
	/// <li>Another balancer is running.
	/// </ol>
	/// <p>Upon exit, a balancer returns an exit code and prints one of the
	/// following messages to the output file in corresponding to the above exit
	/// reasons:
	/// <ol>
	/// <li>The cluster is balanced. Exiting
	/// <li>No block can be moved. Exiting...
	/// <li>No block has been moved for specified iterations (5 by default). Exiting...
	/// <li>Received an IO exception: failure reason. Exiting...
	/// <li>Another balancer is running. Exiting...
	/// </ol>
	/// <p>The administrator can interrupt the execution of the balancer at any
	/// time by running the command "stop-balancer.sh" on the machine where the
	/// balancer is running.
	/// </remarks>
	public class Balancer
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
			));

		internal static readonly Path BalancerIdPath = new Path("/system/balancer.id");

		private const long Gb = 1L << 30;

		private const long MaxSizeToMove = 10 * Gb;

		private static readonly string Usage = "Usage: hdfs balancer" + "\n\t[-policy <policy>]\tthe balancing policy: "
			 + BalancingPolicy.Node.Instance.GetName() + " or " + BalancingPolicy.Pool.Instance
			.GetName() + "\n\t[-threshold <threshold>]\tPercentage of disk capacity" + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
			 + "\tExcludes the specified datanodes." + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
			 + "\tIncludes only the specified datanodes." + "\n\t[-idleiterations <idleiterations>]"
			 + "\tNumber of consecutive idle iterations (-1 for Infinite) before exit.";

		private readonly Dispatcher dispatcher;

		private readonly BalancingPolicy policy;

		private readonly double threshold;

		private readonly ICollection<Dispatcher.Source> overUtilized = new List<Dispatcher.Source
			>();

		private readonly ICollection<Dispatcher.Source> aboveAvgUtilized = new List<Dispatcher.Source
			>();

		private readonly ICollection<Dispatcher.DDatanode.StorageGroup> belowAvgUtilized = 
			new List<Dispatcher.DDatanode.StorageGroup>();

		private readonly ICollection<Dispatcher.DDatanode.StorageGroup> underUtilized = new 
			List<Dispatcher.DDatanode.StorageGroup>();

		//1GB
		// all data node lists
		/* Check that this Balancer is compatible with the Block Placement Policy
		* used by the Namenode.
		*/
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.UnsupportedActionException
		/// 	"/>
		private static void CheckReplicationPolicyCompatibility(Configuration conf)
		{
			if (!(BlockPlacementPolicy.GetInstance(conf, null, null, null) is BlockPlacementPolicyDefault
				))
			{
				throw new UnsupportedActionException("Balancer without BlockPlacementPolicyDefault"
					);
			}
		}

		/// <summary>Construct a balancer.</summary>
		/// <remarks>
		/// Construct a balancer.
		/// Initialize balancer. It sets the value of the threshold, and
		/// builds the communication proxies to
		/// namenode as a client and a secondary namenode and retry proxies
		/// when connection fails.
		/// </remarks>
		internal Balancer(NameNodeConnector theblockpool, Balancer.Parameters p, Configuration
			 conf)
		{
			long movedWinWidth = conf.GetLong(DFSConfigKeys.DfsBalancerMovedwinwidthKey, DFSConfigKeys
				.DfsBalancerMovedwinwidthDefault);
			int moverThreads = conf.GetInt(DFSConfigKeys.DfsBalancerMoverthreadsKey, DFSConfigKeys
				.DfsBalancerMoverthreadsDefault);
			int dispatcherThreads = conf.GetInt(DFSConfigKeys.DfsBalancerDispatcherthreadsKey
				, DFSConfigKeys.DfsBalancerDispatcherthreadsDefault);
			int maxConcurrentMovesPerNode = conf.GetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey
				, DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesDefault);
			this.dispatcher = new Dispatcher(theblockpool, p.nodesToBeIncluded, p.nodesToBeExcluded
				, movedWinWidth, moverThreads, dispatcherThreads, maxConcurrentMovesPerNode, conf
				);
			this.threshold = p.threshold;
			this.policy = p.policy;
		}

		private static long GetCapacity(DatanodeStorageReport report, StorageType t)
		{
			long capacity = 0L;
			foreach (StorageReport r in report.GetStorageReports())
			{
				if (r.GetStorage().GetStorageType() == t)
				{
					capacity += r.GetCapacity();
				}
			}
			return capacity;
		}

		private static long GetRemaining(DatanodeStorageReport report, StorageType t)
		{
			long remaining = 0L;
			foreach (StorageReport r in report.GetStorageReports())
			{
				if (r.GetStorage().GetStorageType() == t)
				{
					remaining += r.GetRemaining();
				}
			}
			return remaining;
		}

		/// <summary>
		/// Given a datanode storage set, build a network topology and decide
		/// over-utilized storages, above average utilized storages,
		/// below average utilized storages, and underutilized storages.
		/// </summary>
		/// <remarks>
		/// Given a datanode storage set, build a network topology and decide
		/// over-utilized storages, above average utilized storages,
		/// below average utilized storages, and underutilized storages.
		/// The input datanode storage set is shuffled in order to randomize
		/// to the storage matching later on.
		/// </remarks>
		/// <returns>the number of bytes needed to move in order to balance the cluster.</returns>
		private long Init(IList<DatanodeStorageReport> reports)
		{
			// compute average utilization
			foreach (DatanodeStorageReport r in reports)
			{
				policy.AccumulateSpaces(r);
			}
			policy.InitAvgUtilization();
			// create network topology and classify utilization collections: 
			//   over-utilized, above-average, below-average and under-utilized.
			long overLoadedBytes = 0L;
			long underLoadedBytes = 0L;
			foreach (DatanodeStorageReport r_1 in reports)
			{
				Dispatcher.DDatanode dn = dispatcher.NewDatanode(r_1.GetDatanodeInfo());
				foreach (StorageType t in StorageType.GetMovableTypes())
				{
					double utilization = policy.GetUtilization(r_1, t);
					if (utilization == null)
					{
						// datanode does not have such storage type 
						continue;
					}
					long capacity = GetCapacity(r_1, t);
					double utilizationDiff = utilization - policy.GetAvgUtilization(t);
					double thresholdDiff = Math.Abs(utilizationDiff) - threshold;
					long maxSize2Move = ComputeMaxSize2Move(capacity, GetRemaining(r_1, t), utilizationDiff
						, threshold);
					Dispatcher.DDatanode.StorageGroup g;
					if (utilizationDiff > 0)
					{
						Dispatcher.Source s = dn.AddSource(t, maxSize2Move, dispatcher);
						if (thresholdDiff <= 0)
						{
							// within threshold
							aboveAvgUtilized.AddItem(s);
						}
						else
						{
							overLoadedBytes += Precentage2bytes(thresholdDiff, capacity);
							overUtilized.AddItem(s);
						}
						g = s;
					}
					else
					{
						g = dn.AddTarget(t, maxSize2Move);
						if (thresholdDiff <= 0)
						{
							// within threshold
							belowAvgUtilized.AddItem(g);
						}
						else
						{
							underLoadedBytes += Precentage2bytes(thresholdDiff, capacity);
							underUtilized.AddItem(g);
						}
					}
					dispatcher.GetStorageGroupMap().Put(g);
				}
			}
			LogUtilizationCollections();
			Preconditions.CheckState(dispatcher.GetStorageGroupMap().Size() == overUtilized.Count
				 + underUtilized.Count + aboveAvgUtilized.Count + belowAvgUtilized.Count, "Mismatched number of storage groups"
				);
			// return number of bytes to be moved in order to make the cluster balanced
			return Math.Max(overLoadedBytes, underLoadedBytes);
		}

		private static long ComputeMaxSize2Move(long capacity, long remaining, double utilizationDiff
			, double threshold)
		{
			double diff = Math.Min(threshold, Math.Abs(utilizationDiff));
			long maxSizeToMove = Precentage2bytes(diff, capacity);
			if (utilizationDiff < 0)
			{
				maxSizeToMove = Math.Min(remaining, maxSizeToMove);
			}
			return Math.Min(MaxSizeToMove, maxSizeToMove);
		}

		private static long Precentage2bytes(double precentage, long capacity)
		{
			Preconditions.CheckArgument(precentage >= 0, "precentage = " + precentage + " < 0"
				);
			return (long)(precentage * capacity / 100.0);
		}

		/* log the over utilized & under utilized nodes */
		private void LogUtilizationCollections()
		{
			LogUtilizationCollection("over-utilized", overUtilized);
			if (Log.IsTraceEnabled())
			{
				LogUtilizationCollection("above-average", aboveAvgUtilized);
				LogUtilizationCollection("below-average", belowAvgUtilized);
			}
			LogUtilizationCollection("underutilized", underUtilized);
		}

		private static void LogUtilizationCollection<T>(string name, ICollection<T> items
			)
			where T : Dispatcher.DDatanode.StorageGroup
		{
			Log.Info(items.Count + " " + name + ": " + items);
		}

		/// <summary>
		/// Decide all <source, target> pairs and
		/// the number of bytes to move from a source to a target
		/// Maximum bytes to be moved per storage group is
		/// min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
		/// </summary>
		/// <returns>total number of bytes to move in this iteration</returns>
		private long ChooseStorageGroups()
		{
			// First, match nodes on the same node group if cluster is node group aware
			if (dispatcher.GetCluster().IsNodeGroupAware())
			{
				ChooseStorageGroups(Matcher.SameNodeGroup);
			}
			// Then, match nodes on the same rack
			ChooseStorageGroups(Matcher.SameRack);
			// At last, match all remaining nodes
			ChooseStorageGroups(Matcher.AnyOther);
			return dispatcher.BytesToMove();
		}

		/// <summary>Decide all <source, target> pairs according to the matcher.</summary>
		private void ChooseStorageGroups(Matcher matcher)
		{
			/* first step: match each overUtilized datanode (source) to
			* one or more underUtilized datanodes (targets).
			*/
			ChooseStorageGroups(overUtilized, underUtilized, matcher);
			/* match each remaining overutilized datanode (source) to
			* below average utilized datanodes (targets).
			* Note only overutilized datanodes that haven't had that max bytes to move
			* satisfied in step 1 are selected
			*/
			ChooseStorageGroups(overUtilized, belowAvgUtilized, matcher);
			/* match each remaining underutilized datanode (target) to
			* above average utilized datanodes (source).
			* Note only underutilized datanodes that have not had that max bytes to
			* move satisfied in step 1 are selected.
			*/
			ChooseStorageGroups(underUtilized, aboveAvgUtilized, matcher);
		}

		/// <summary>For each datanode, choose matching nodes from the candidates.</summary>
		/// <remarks>
		/// For each datanode, choose matching nodes from the candidates. Either the
		/// datanodes or the candidates are source nodes with (utilization &gt; Avg), and
		/// the others are target nodes with (utilization &lt; Avg).
		/// </remarks>
		private void ChooseStorageGroups<G, C>(ICollection<G> groups, ICollection<C> candidates
			, Matcher matcher)
			where G : Dispatcher.DDatanode.StorageGroup
			where C : Dispatcher.DDatanode.StorageGroup
		{
			for (IEnumerator<G> i = groups.GetEnumerator(); i.HasNext(); )
			{
				G g = i.Next();
				for (; Choose4One(g, candidates, matcher); )
				{
				}
				if (!g.HasSpaceForScheduling())
				{
					i.Remove();
				}
			}
		}

		/// <summary>For the given datanode, choose a candidate and then schedule it.</summary>
		/// <returns>true if a candidate is chosen; false if no candidates is chosen.</returns>
		private bool Choose4One<C>(Dispatcher.DDatanode.StorageGroup g, ICollection<C> candidates
			, Matcher matcher)
			where C : Dispatcher.DDatanode.StorageGroup
		{
			IEnumerator<C> i = candidates.GetEnumerator();
			C chosen = ChooseCandidate(g, i, matcher);
			if (chosen == null)
			{
				return false;
			}
			if (g is Dispatcher.Source)
			{
				MatchSourceWithTargetToMove((Dispatcher.Source)g, chosen);
			}
			else
			{
				MatchSourceWithTargetToMove((Dispatcher.Source)chosen, g);
			}
			if (!chosen.HasSpaceForScheduling())
			{
				i.Remove();
			}
			return true;
		}

		private void MatchSourceWithTargetToMove(Dispatcher.Source source, Dispatcher.DDatanode.StorageGroup
			 target)
		{
			long size = Math.Min(source.AvailableSizeToMove(), target.AvailableSizeToMove());
			Dispatcher.Task task = new Dispatcher.Task(target, size);
			source.AddTask(task);
			target.IncScheduledSize(task.GetSize());
			dispatcher.Add(source, target);
			Log.Info("Decided to move " + StringUtils.ByteDesc(size) + " bytes from " + source
				.GetDisplayName() + " to " + target.GetDisplayName());
		}

		/// <summary>Choose a candidate for the given datanode.</summary>
		private C ChooseCandidate<G, C>(G g, IEnumerator<C> candidates, Matcher matcher)
			where G : Dispatcher.DDatanode.StorageGroup
			where C : Dispatcher.DDatanode.StorageGroup
		{
			if (g.HasSpaceForScheduling())
			{
				for (; candidates.HasNext(); )
				{
					C c = candidates.Next();
					if (!c.HasSpaceForScheduling())
					{
						candidates.Remove();
					}
					else
					{
						if (matcher.Match(dispatcher.GetCluster(), g.GetDatanodeInfo(), c.GetDatanodeInfo
							()))
						{
							return c;
						}
					}
				}
			}
			return null;
		}

		/* reset all fields in a balancer preparing for the next iteration */
		internal virtual void ResetData(Configuration conf)
		{
			this.overUtilized.Clear();
			this.aboveAvgUtilized.Clear();
			this.belowAvgUtilized.Clear();
			this.underUtilized.Clear();
			this.policy.Reset();
			dispatcher.Reset(conf);
		}

		internal class Result
		{
			internal readonly ExitStatus exitStatus;

			internal readonly long bytesLeftToMove;

			internal readonly long bytesBeingMoved;

			internal readonly long bytesAlreadyMoved;

			internal Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved
				, long bytesAlreadyMoved)
			{
				this.exitStatus = exitStatus;
				this.bytesLeftToMove = bytesLeftToMove;
				this.bytesBeingMoved = bytesBeingMoved;
				this.bytesAlreadyMoved = bytesAlreadyMoved;
			}

			internal virtual void Print(int iteration, TextWriter @out)
			{
				@out.Printf("%-24s %10d  %19s  %18s  %17s%n", DateFormat.GetDateTimeInstance().Format
					(new DateTime()), iteration, StringUtils.ByteDesc(bytesAlreadyMoved), StringUtils
					.ByteDesc(bytesLeftToMove), StringUtils.ByteDesc(bytesBeingMoved));
			}
		}

		internal virtual Balancer.Result NewResult(ExitStatus exitStatus, long bytesLeftToMove
			, long bytesBeingMoved)
		{
			return new Balancer.Result(exitStatus, bytesLeftToMove, bytesBeingMoved, dispatcher
				.GetBytesMoved());
		}

		internal virtual Balancer.Result NewResult(ExitStatus exitStatus)
		{
			return new Balancer.Result(exitStatus, -1, -1, dispatcher.GetBytesMoved());
		}

		/// <summary>Run an iteration for all datanodes.</summary>
		internal virtual Balancer.Result RunOneIteration()
		{
			try
			{
				IList<DatanodeStorageReport> reports = dispatcher.Init();
				long bytesLeftToMove = Init(reports);
				if (bytesLeftToMove == 0)
				{
					System.Console.Out.WriteLine("The cluster is balanced. Exiting...");
					return NewResult(ExitStatus.Success, bytesLeftToMove, -1);
				}
				else
				{
					Log.Info("Need to move " + StringUtils.ByteDesc(bytesLeftToMove) + " to make the cluster balanced."
						);
				}
				/* Decide all the nodes that will participate in the block move and
				* the number of bytes that need to be moved from one node to another
				* in this iteration. Maximum bytes to be moved per node is
				* Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
				*/
				long bytesBeingMoved = ChooseStorageGroups();
				if (bytesBeingMoved == 0)
				{
					System.Console.Out.WriteLine("No block can be moved. Exiting...");
					return NewResult(ExitStatus.NoMoveBlock, bytesLeftToMove, bytesBeingMoved);
				}
				else
				{
					Log.Info("Will move " + StringUtils.ByteDesc(bytesBeingMoved) + " in this iteration"
						);
				}
				/* For each pair of <source, target>, start a thread that repeatedly
				* decide a block to be moved and its proxy source,
				* then initiates the move until all bytes are moved or no more block
				* available to move.
				* Exit no byte has been moved for 5 consecutive iterations.
				*/
				if (!dispatcher.DispatchAndCheckContinue())
				{
					return NewResult(ExitStatus.NoMoveProgress, bytesLeftToMove, bytesBeingMoved);
				}
				return NewResult(ExitStatus.InProgress, bytesLeftToMove, bytesBeingMoved);
			}
			catch (ArgumentException e)
			{
				System.Console.Out.WriteLine(e + ".  Exiting ...");
				return NewResult(ExitStatus.IllegalArguments);
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine(e + ".  Exiting ...");
				return NewResult(ExitStatus.IoException);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine(e + ".  Exiting ...");
				return NewResult(ExitStatus.Interrupted);
			}
			finally
			{
				dispatcher.ShutdownNow();
			}
		}

		/// <summary>Balance all namenodes.</summary>
		/// <remarks>
		/// Balance all namenodes.
		/// For each iteration,
		/// for each namenode,
		/// execute a
		/// <see cref="Balancer"/>
		/// to work through all datanodes once.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static int Run(ICollection<URI> namenodes, Balancer.Parameters p, Configuration
			 conf)
		{
			long sleeptime = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DFSConfigKeys
				.DfsHeartbeatIntervalDefault) * 2000 + conf.GetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey
				, DFSConfigKeys.DfsNamenodeReplicationIntervalDefault) * 1000;
			Log.Info("namenodes  = " + namenodes);
			Log.Info("parameters = " + p);
			System.Console.Out.WriteLine("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved"
				);
			IList<NameNodeConnector> connectors = Sharpen.Collections.EmptyList();
			try
			{
				connectors = NameNodeConnector.NewNameNodeConnectors(namenodes, typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
					).Name, BalancerIdPath, conf, p.maxIdleIteration);
				bool done = false;
				for (int iteration = 0; !done; iteration++)
				{
					done = true;
					Sharpen.Collections.Shuffle(connectors);
					foreach (NameNodeConnector nnc in connectors)
					{
						Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer b = new Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
							(nnc, p, conf);
						Balancer.Result r = b.RunOneIteration();
						r.Print(iteration, System.Console.Out);
						// clean all lists
						b.ResetData(conf);
						if (r.exitStatus == ExitStatus.InProgress)
						{
							done = false;
						}
						else
						{
							if (r.exitStatus != ExitStatus.Success)
							{
								//must be an error statue, return.
								return r.exitStatus.GetExitCode();
							}
						}
					}
					if (!done)
					{
						Sharpen.Thread.Sleep(sleeptime);
					}
				}
			}
			finally
			{
				foreach (NameNodeConnector nnc in connectors)
				{
					IOUtils.Cleanup(Log, nnc);
				}
			}
			return ExitStatus.Success.GetExitCode();
		}

		/* Given elaspedTime in ms, return a printable string */
		private static string Time2Str(long elapsedTime)
		{
			string unit;
			double time = elapsedTime;
			if (elapsedTime < 1000)
			{
				unit = "milliseconds";
			}
			else
			{
				if (elapsedTime < 60 * 1000)
				{
					unit = "seconds";
					time = time / 1000;
				}
				else
				{
					if (elapsedTime < 3600 * 1000)
					{
						unit = "minutes";
						time = time / (60 * 1000);
					}
					else
					{
						unit = "hours";
						time = time / (3600 * 1000);
					}
				}
			}
			return time + " " + unit;
		}

		internal class Parameters
		{
			internal static readonly Balancer.Parameters Default = new Balancer.Parameters(BalancingPolicy.Node
				.Instance, 10.0, NameNodeConnector.DefaultMaxIdleIterations, Sharpen.Collections
				.EmptySet<string>(), Sharpen.Collections.EmptySet<string>());

			internal readonly BalancingPolicy policy;

			internal readonly double threshold;

			internal readonly int maxIdleIteration;

			internal ICollection<string> nodesToBeExcluded;

			internal ICollection<string> nodesToBeIncluded;

			internal Parameters(BalancingPolicy policy, double threshold, int maxIdleIteration
				, ICollection<string> nodesToBeExcluded, ICollection<string> nodesToBeIncluded)
			{
				// exclude the nodes in this set from balancing operations
				//include only these nodes in balancing operations
				this.policy = policy;
				this.threshold = threshold;
				this.maxIdleIteration = maxIdleIteration;
				this.nodesToBeExcluded = nodesToBeExcluded;
				this.nodesToBeIncluded = nodesToBeIncluded;
			}

			public override string ToString()
			{
				return typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer).Name + "." + GetType
					().Name + "[" + policy + ", threshold=" + threshold + ", max idle iteration = " 
					+ maxIdleIteration + ", number of nodes to be excluded = " + nodesToBeExcluded.Count
					 + ", number of nodes to be included = " + nodesToBeIncluded.Count + "]";
			}
		}

		internal class Cli : Configured, Tool
		{
			/// <summary>Parse arguments and then run Balancer.</summary>
			/// <param name="args">command specific arguments.</param>
			/// <returns>exit code. 0 indicates success, non-zero indicates failure.</returns>
			public virtual int Run(string[] args)
			{
				long startTime = Time.MonotonicNow();
				Configuration conf = GetConf();
				try
				{
					CheckReplicationPolicyCompatibility(conf);
					ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
					return Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Parse(args)
						, conf);
				}
				catch (IOException e)
				{
					System.Console.Out.WriteLine(e + ".  Exiting ...");
					return ExitStatus.IoException.GetExitCode();
				}
				catch (Exception e)
				{
					System.Console.Out.WriteLine(e + ".  Exiting ...");
					return ExitStatus.Interrupted.GetExitCode();
				}
				finally
				{
					System.Console.Out.Format("%-24s ", DateFormat.GetDateTimeInstance().Format(new DateTime
						()));
					System.Console.Out.WriteLine("Balancing took " + Time2Str(Time.MonotonicNow() - startTime
						));
				}
			}

			/// <summary>parse command line arguments</summary>
			internal static Balancer.Parameters Parse(string[] args)
			{
				BalancingPolicy policy = Balancer.Parameters.Default.policy;
				double threshold = Balancer.Parameters.Default.threshold;
				int maxIdleIteration = Balancer.Parameters.Default.maxIdleIteration;
				ICollection<string> nodesTobeExcluded = Balancer.Parameters.Default.nodesToBeExcluded;
				ICollection<string> nodesTobeIncluded = Balancer.Parameters.Default.nodesToBeIncluded;
				if (args != null)
				{
					try
					{
						for (int i = 0; i < args.Length; i++)
						{
							if (Sharpen.Runtime.EqualsIgnoreCase("-threshold", args[i]))
							{
								Preconditions.CheckArgument(++i < args.Length, "Threshold value is missing: args = "
									 + Arrays.ToString(args));
								try
								{
									threshold = double.ParseDouble(args[i]);
									if (threshold < 1 || threshold > 100)
									{
										throw new ArgumentException("Number out of range: threshold = " + threshold);
									}
									Log.Info("Using a threshold of " + threshold);
								}
								catch (ArgumentException e)
								{
									System.Console.Error.WriteLine("Expecting a number in the range of [1.0, 100.0]: "
										 + args[i]);
									throw;
								}
							}
							else
							{
								if (Sharpen.Runtime.EqualsIgnoreCase("-policy", args[i]))
								{
									Preconditions.CheckArgument(++i < args.Length, "Policy value is missing: args = "
										 + Arrays.ToString(args));
									try
									{
										policy = BalancingPolicy.Parse(args[i]);
									}
									catch (ArgumentException e)
									{
										System.Console.Error.WriteLine("Illegal policy name: " + args[i]);
										throw;
									}
								}
								else
								{
									if (Sharpen.Runtime.EqualsIgnoreCase("-exclude", args[i]))
									{
										Preconditions.CheckArgument(++i < args.Length, "List of nodes to exclude | -f <filename> is missing: args = "
											 + Arrays.ToString(args));
										if (Sharpen.Runtime.EqualsIgnoreCase("-f", args[i]))
										{
											Preconditions.CheckArgument(++i < args.Length, "File containing nodes to exclude is not specified: args = "
												 + Arrays.ToString(args));
											nodesTobeExcluded = Dispatcher.Util.GetHostListFromFile(args[i], "exclude");
										}
										else
										{
											nodesTobeExcluded = Dispatcher.Util.ParseHostList(args[i]);
										}
									}
									else
									{
										if (Sharpen.Runtime.EqualsIgnoreCase("-include", args[i]))
										{
											Preconditions.CheckArgument(++i < args.Length, "List of nodes to include | -f <filename> is missing: args = "
												 + Arrays.ToString(args));
											if (Sharpen.Runtime.EqualsIgnoreCase("-f", args[i]))
											{
												Preconditions.CheckArgument(++i < args.Length, "File containing nodes to include is not specified: args = "
													 + Arrays.ToString(args));
												nodesTobeIncluded = Dispatcher.Util.GetHostListFromFile(args[i], "include");
											}
											else
											{
												nodesTobeIncluded = Dispatcher.Util.ParseHostList(args[i]);
											}
										}
										else
										{
											if (Sharpen.Runtime.EqualsIgnoreCase("-idleiterations", args[i]))
											{
												Preconditions.CheckArgument(++i < args.Length, "idleiterations value is missing: args = "
													 + Arrays.ToString(args));
												maxIdleIteration = System.Convert.ToInt32(args[i]);
												Log.Info("Using a idleiterations of " + maxIdleIteration);
											}
											else
											{
												throw new ArgumentException("args = " + Arrays.ToString(args));
											}
										}
									}
								}
							}
						}
						Preconditions.CheckArgument(nodesTobeExcluded.IsEmpty() || nodesTobeIncluded.IsEmpty
							(), "-exclude and -include options cannot be specified together.");
					}
					catch (RuntimeException e)
					{
						PrintUsage(System.Console.Error);
						throw;
					}
				}
				return new Balancer.Parameters(policy, threshold, maxIdleIteration, nodesTobeExcluded
					, nodesTobeIncluded);
			}

			private static void PrintUsage(TextWriter @out)
			{
				@out.WriteLine(Usage + "\n");
			}
		}

		/// <summary>Run a balancer</summary>
		/// <param name="args">Command line arguments</param>
		public static void Main(string[] args)
		{
			if (DFSUtil.ParseHelpArgument(args, Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			try
			{
				System.Environment.Exit(ToolRunner.Run(new HdfsConfiguration(), new Balancer.Cli(
					), args));
			}
			catch (Exception e)
			{
				Log.Error("Exiting balancer due an exception", e);
				System.Environment.Exit(-1);
			}
		}
	}
}
