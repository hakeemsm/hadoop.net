using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Main class for a series of name-node benchmarks.</summary>
	/// <remarks>
	/// Main class for a series of name-node benchmarks.
	/// Each benchmark measures throughput and average execution time
	/// of a specific name-node operation, e.g. file creation or block reports.
	/// The benchmark does not involve any other hadoop components
	/// except for the name-node. Each operation is executed
	/// by calling directly the respective name-node method.
	/// The name-node here is real all other components are simulated.
	/// Command line arguments for the benchmark include:
	/// <ol>
	/// <li>total number of operations to be performed,</li>
	/// <li>number of threads to run these operations,</li>
	/// <li>followed by operation specific input parameters.</li>
	/// <li>-logLevel L specifies the logging level when the benchmark runs.
	/// The default logging level is
	/// <see cref="Org.Apache.Log4j.Level.Error"/>
	/// .</li>
	/// <li>-UGCacheRefreshCount G will cause the benchmark to call
	/// <see cref="NameNodeRpcServer.RefreshUserToGroupsMappings()"/>
	/// after
	/// every G operations, which purges the name-node's user group cache.
	/// By default the refresh is never called.</li>
	/// <li>-keepResults do not clean up the name-space after execution.</li>
	/// <li>-useExisting do not recreate the name-space, use existing data.</li>
	/// </ol>
	/// The benchmark first generates inputs for each thread so that the
	/// input generation overhead does not effect the resulting statistics.
	/// The number of operations performed by threads is practically the same.
	/// Precisely, the difference between the number of operations
	/// performed by any two threads does not exceed 1.
	/// Then the benchmark executes the specified number of operations using
	/// the specified number of threads and outputs the resulting stats.
	/// </remarks>
	public class NNThroughputBenchmark : Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.NNThroughputBenchmark
			));

		private const int BlockSize = 16;

		private const string GeneralOptionsUsage = "     [-keepResults] | [-logLevel L] | [-UGCacheRefreshCount G]";

		internal static Configuration config;

		internal static NameNode nameNode;

		internal static NamenodeProtocols nameNodeProto;

		/// <exception cref="System.IO.IOException"/>
		internal NNThroughputBenchmark(Configuration conf)
		{
			config = conf;
			// We do not need many handlers, since each thread simulates a handler
			// by calling name-node methods directly
			config.SetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, 1);
			// Turn off minimum block size verification
			config.SetInt(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, 0);
			// set exclude file
			config.Set(DFSConfigKeys.DfsHostsExclude, "${hadoop.tmp.dir}/dfs/hosts/exclude");
			FilePath excludeFile = new FilePath(config.Get(DFSConfigKeys.DfsHostsExclude, "exclude"
				));
			if (!excludeFile.Exists())
			{
				if (!excludeFile.GetParentFile().Exists() && !excludeFile.GetParentFile().Mkdirs(
					))
				{
					throw new IOException("NNThroughputBenchmark: cannot mkdir " + excludeFile);
				}
			}
			new FileOutputStream(excludeFile).Close();
			// set include file
			config.Set(DFSConfigKeys.DfsHosts, "${hadoop.tmp.dir}/dfs/hosts/include");
			FilePath includeFile = new FilePath(config.Get(DFSConfigKeys.DfsHosts, "include")
				);
			new FileOutputStream(includeFile).Close();
		}

		internal virtual void Close()
		{
			if (nameNode != null)
			{
				nameNode.Stop();
			}
		}

		internal static void SetNameNodeLoggingLevel(Level logLevel)
		{
			Log.Fatal("Log level = " + logLevel.ToString());
			// change log level to NameNode logs
			DFSTestUtil.SetNameNodeLogLevel(logLevel);
			GenericTestUtils.SetLogLevel(LogManager.GetLogger(typeof(NetworkTopology).FullName
				), logLevel);
			GenericTestUtils.SetLogLevel(LogManager.GetLogger(typeof(Groups).FullName), logLevel
				);
		}

		/// <summary>Base class for collecting operation statistics.</summary>
		/// <remarks>
		/// Base class for collecting operation statistics.
		/// Overload this class in order to run statistics for a
		/// specific name-node operation.
		/// </remarks>
		internal abstract class OperationStatsBase
		{
			protected internal const string BaseDirName = "/nnThroughputBenchmark";

			protected internal const string OpAllName = "all";

			protected internal const string OpAllUsage = "-op all <other ops options>";

			protected internal readonly string baseDir;

			protected internal short replication;

			protected internal int numThreads = 0;

			protected internal int numOpsRequired = 0;

			protected internal int numOpsExecuted = 0;

			protected internal long cumulativeTime = 0;

			protected internal long elapsedTime = 0;

			protected internal bool keepResults = false;

			protected internal Level logLevel;

			protected internal int ugcRefreshCount = 0;

			protected internal IList<NNThroughputBenchmark.StatsDaemon> daemons;

			// number of threads
			// number of operations requested
			// number of operations executed
			// sum of times for each op
			// time from start to finish
			// don't clean base directory on exit
			// logging level, ERROR by default
			// user group cache refresh count
			/// <summary>Operation name.</summary>
			internal abstract string GetOpName();

			/// <summary>Parse command line arguments.</summary>
			/// <param name="args">arguments</param>
			/// <exception cref="System.IO.IOException"/>
			internal abstract void ParseArguments(IList<string> args);

			/// <summary>Generate inputs for each daemon thread.</summary>
			/// <param name="opsPerThread">number of inputs for each thread.</param>
			/// <exception cref="System.IO.IOException"/>
			internal abstract void GenerateInputs(int[] opsPerThread);

			/// <summary>
			/// This corresponds to the arg1 argument of
			/// <see cref="ExecuteOp(int, int, string)"/>
			/// , which can have different meanings
			/// depending on the operation performed.
			/// </summary>
			/// <param name="daemonId">id of the daemon calling this method</param>
			/// <returns>the argument</returns>
			internal abstract string GetExecutionArgument(int daemonId);

			/// <summary>Execute name-node operation.</summary>
			/// <param name="daemonId">id of the daemon calling this method.</param>
			/// <param name="inputIdx">serial index of the operation called by the deamon.</param>
			/// <param name="arg1">operation specific argument.</param>
			/// <returns>time of the individual name-node call.</returns>
			/// <exception cref="System.IO.IOException"/>
			internal abstract long ExecuteOp(int daemonId, int inputIdx, string arg1);

			/// <summary>Print the results of the benchmarking.</summary>
			internal abstract void PrintResults();

			internal OperationStatsBase(NNThroughputBenchmark _enclosing)
			{
				this._enclosing = _enclosing;
				this.baseDir = NNThroughputBenchmark.OperationStatsBase.BaseDirName + "/" + this.
					GetOpName();
				this.replication = (short)NNThroughputBenchmark.config.GetInt(DFSConfigKeys.DfsReplicationKey
					, 3);
				this.numOpsRequired = 10;
				this.numThreads = 3;
				this.logLevel = Level.Error;
				this.ugcRefreshCount = int.MaxValue;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Benchmark()
			{
				this.daemons = new AList<NNThroughputBenchmark.StatsDaemon>();
				long start = 0;
				try
				{
					this.numOpsExecuted = 0;
					this.cumulativeTime = 0;
					if (this.numThreads < 1)
					{
						return;
					}
					int tIdx = 0;
					// thread index < nrThreads
					int[] opsPerThread = new int[this.numThreads];
					for (int opsScheduled = 0; opsScheduled < this.numOpsRequired; opsScheduled += opsPerThread
						[tIdx++])
					{
						// execute  in a separate thread
						opsPerThread[tIdx] = (this.numOpsRequired - opsScheduled) / (this.numThreads - tIdx
							);
						if (opsPerThread[tIdx] == 0)
						{
							opsPerThread[tIdx] = 1;
						}
					}
					// if numThreads > numOpsRequired then the remaining threads will do nothing
					for (; tIdx < this.numThreads; tIdx++)
					{
						opsPerThread[tIdx] = 0;
					}
					this.GenerateInputs(opsPerThread);
					NNThroughputBenchmark.SetNameNodeLoggingLevel(this.logLevel);
					for (tIdx = 0; tIdx < this.numThreads; tIdx++)
					{
						this.daemons.AddItem(new NNThroughputBenchmark.StatsDaemon(this, tIdx, opsPerThread
							[tIdx], this));
					}
					start = Time.Now();
					NNThroughputBenchmark.Log.Info("Starting " + this.numOpsRequired + " " + this.GetOpName
						() + "(s).");
					foreach (NNThroughputBenchmark.StatsDaemon d in this.daemons)
					{
						d.Start();
					}
				}
				finally
				{
					while (this.IsInPorgress())
					{
					}
					// try {Thread.sleep(500);} catch (InterruptedException e) {}
					this.elapsedTime = Time.Now() - start;
					foreach (NNThroughputBenchmark.StatsDaemon d in this.daemons)
					{
						this.IncrementStats(d.localNumOpsExecuted, d.localCumulativeTime);
					}
				}
			}

			// System.out.println(d.toString() + ": ops Exec = " + d.localNumOpsExecuted);
			private bool IsInPorgress()
			{
				foreach (NNThroughputBenchmark.StatsDaemon d in this.daemons)
				{
					if (d.IsInProgress())
					{
						return true;
					}
				}
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void CleanUp()
			{
				NNThroughputBenchmark.nameNodeProto.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
					, false);
				if (!this.keepResults)
				{
					NNThroughputBenchmark.nameNodeProto.Delete(this.GetBaseDir(), true);
				}
			}

			internal virtual int GetNumOpsExecuted()
			{
				return this.numOpsExecuted;
			}

			internal virtual long GetCumulativeTime()
			{
				return this.cumulativeTime;
			}

			internal virtual long GetElapsedTime()
			{
				return this.elapsedTime;
			}

			internal virtual long GetAverageTime()
			{
				return this.numOpsExecuted == 0 ? 0 : this.cumulativeTime / this.numOpsExecuted;
			}

			internal virtual double GetOpsPerSecond()
			{
				return this.elapsedTime == 0 ? 0 : 1000 * (double)this.numOpsExecuted / this.elapsedTime;
			}

			internal virtual string GetBaseDir()
			{
				return this.baseDir;
			}

			internal virtual string GetClientName(int idx)
			{
				return this.GetOpName() + "-client-" + idx;
			}

			internal virtual void IncrementStats(int ops, long time)
			{
				this.numOpsExecuted += ops;
				this.cumulativeTime += time;
			}

			/// <summary>Parse first 2 arguments, corresponding to the "-op" option.</summary>
			/// <param name="args">argument list</param>
			/// <returns>
			/// true if operation is all, which means that options not related
			/// to this operation should be ignored, or false otherwise, meaning
			/// that usage should be printed when an unrelated option is encountered.
			/// </returns>
			protected internal virtual bool VerifyOpArgument(IList<string> args)
			{
				if (args.Count < 2 || !args[0].StartsWith("-op"))
				{
					NNThroughputBenchmark.PrintUsage();
				}
				// process common options
				int krIndex = args.IndexOf("-keepResults");
				this.keepResults = (krIndex >= 0);
				if (this.keepResults)
				{
					args.Remove(krIndex);
				}
				int llIndex = args.IndexOf("-logLevel");
				if (llIndex >= 0)
				{
					if (args.Count <= llIndex + 1)
					{
						NNThroughputBenchmark.PrintUsage();
					}
					this.logLevel = Level.ToLevel(args[llIndex + 1], Level.Error);
					args.Remove(llIndex + 1);
					args.Remove(llIndex);
				}
				int ugrcIndex = args.IndexOf("-UGCacheRefreshCount");
				if (ugrcIndex >= 0)
				{
					if (args.Count <= ugrcIndex + 1)
					{
						NNThroughputBenchmark.PrintUsage();
					}
					int g = System.Convert.ToInt32(args[ugrcIndex + 1]);
					if (g > 0)
					{
						this.ugcRefreshCount = g;
					}
					args.Remove(ugrcIndex + 1);
					args.Remove(ugrcIndex);
				}
				string type = args[1];
				if (NNThroughputBenchmark.OperationStatsBase.OpAllName.Equals(type))
				{
					type = this.GetOpName();
					return true;
				}
				if (!this.GetOpName().Equals(type))
				{
					NNThroughputBenchmark.PrintUsage();
				}
				return false;
			}

			internal virtual void PrintStats()
			{
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " stats  ---");
				NNThroughputBenchmark.Log.Info("# operations: " + this.GetNumOpsExecuted());
				NNThroughputBenchmark.Log.Info("Elapsed Time: " + this.GetElapsedTime());
				NNThroughputBenchmark.Log.Info(" Ops per sec: " + this.GetOpsPerSecond());
				NNThroughputBenchmark.Log.Info("Average Time: " + this.GetAverageTime());
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>One of the threads that perform stats operations.</summary>
		private class StatsDaemon : Sharpen.Thread
		{
			private readonly int daemonId;

			private int opsPerThread;

			private string arg1;

			private volatile int localNumOpsExecuted = 0;

			private volatile long localCumulativeTime = 0;

			private readonly NNThroughputBenchmark.OperationStatsBase statsOp;

			internal StatsDaemon(NNThroughputBenchmark _enclosing, int daemonId, int nrOps, NNThroughputBenchmark.OperationStatsBase
				 op)
			{
				this._enclosing = _enclosing;
				// argument passed to executeOp()
				this.daemonId = daemonId;
				this.opsPerThread = nrOps;
				this.statsOp = op;
				this.SetName(this.ToString());
			}

			public override void Run()
			{
				this.localNumOpsExecuted = 0;
				this.localCumulativeTime = 0;
				this.arg1 = this.statsOp.GetExecutionArgument(this.daemonId);
				try
				{
					this.BenchmarkOne();
				}
				catch (IOException ex)
				{
					NNThroughputBenchmark.Log.Error("StatsDaemon " + this.daemonId + " failed: \n" + 
						StringUtils.StringifyException(ex));
				}
			}

			public override string ToString()
			{
				return "StatsDaemon-" + this.daemonId;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void BenchmarkOne()
			{
				for (int idx = 0; idx < this.opsPerThread; idx++)
				{
					if ((this.localNumOpsExecuted + 1) % this.statsOp.ugcRefreshCount == 0)
					{
						NNThroughputBenchmark.nameNodeProto.RefreshUserToGroupsMappings();
					}
					long stat = this.statsOp.ExecuteOp(this.daemonId, idx, this.arg1);
					this.localNumOpsExecuted++;
					this.localCumulativeTime += stat;
				}
			}

			internal virtual bool IsInProgress()
			{
				return this.localNumOpsExecuted < this.opsPerThread;
			}

			/// <summary>Schedule to stop this daemon.</summary>
			internal virtual void Terminate()
			{
				this.opsPerThread = this.localNumOpsExecuted;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Clean all benchmark result directories.</summary>
		internal class CleanAllStats : NNThroughputBenchmark.OperationStatsBase
		{
			internal const string OpCleanName = "clean";

			internal const string OpCleanUsage = "-op clean";

			internal CleanAllStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				// Operation types
				this.ParseArguments(args);
				this.numOpsRequired = 1;
				this.numThreads = 1;
				this.keepResults = true;
			}

			internal override string GetOpName()
			{
				return NNThroughputBenchmark.CleanAllStats.OpCleanName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				bool ignoreUnrelatedOptions = this.VerifyOpArgument(args);
				if (args.Count > 2 && !ignoreUnrelatedOptions)
				{
					NNThroughputBenchmark.PrintUsage();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] opsPerThread)
			{
			}

			// do nothing
			/// <summary>Does not require the argument</summary>
			internal override string GetExecutionArgument(int daemonId)
			{
				return null;
			}

			/// <summary>Remove entire benchmark directory.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				NNThroughputBenchmark.nameNodeProto.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
					, false);
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.Delete(NNThroughputBenchmark.OperationStatsBase
					.BaseDirName, true);
				long end = Time.Now();
				return end - start;
			}

			internal override void PrintResults()
			{
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " inputs ---");
				NNThroughputBenchmark.Log.Info("Remove directory " + NNThroughputBenchmark.OperationStatsBase
					.BaseDirName);
				this.PrintStats();
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>File creation statistics.</summary>
		/// <remarks>
		/// File creation statistics.
		/// Each thread creates the same (+ or -1) number of files.
		/// File names are pre-generated during initialization.
		/// The created files do not have blocks.
		/// </remarks>
		internal class CreateFileStats : NNThroughputBenchmark.OperationStatsBase
		{
			internal const string OpCreateName = "create";

			internal const string OpCreateUsage = "-op create [-threads T] [-files N] [-filesPerDir P] [-close]";

			protected internal FileNameGenerator nameGenerator;

			protected internal string[][] fileNames;

			private bool closeUponCreate;

			internal CreateFileStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				// Operation types
				this.ParseArguments(args);
			}

			internal override string GetOpName()
			{
				return NNThroughputBenchmark.CreateFileStats.OpCreateName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				bool ignoreUnrelatedOptions = this.VerifyOpArgument(args);
				int nrFilesPerDir = 4;
				this.closeUponCreate = false;
				for (int i = 2; i < args.Count; i++)
				{
					// parse command line
					if (args[i].Equals("-files"))
					{
						if (i + 1 == args.Count)
						{
							NNThroughputBenchmark.PrintUsage();
						}
						this.numOpsRequired = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-threads"))
						{
							if (i + 1 == args.Count)
							{
								NNThroughputBenchmark.PrintUsage();
							}
							this.numThreads = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-filesPerDir"))
							{
								if (i + 1 == args.Count)
								{
									NNThroughputBenchmark.PrintUsage();
								}
								nrFilesPerDir = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-close"))
								{
									this.closeUponCreate = true;
								}
								else
								{
									if (!ignoreUnrelatedOptions)
									{
										NNThroughputBenchmark.PrintUsage();
									}
								}
							}
						}
					}
				}
				this.nameGenerator = new FileNameGenerator(this.GetBaseDir(), nrFilesPerDir);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] opsPerThread)
			{
				System.Diagnostics.Debug.Assert(opsPerThread.Length == this.numThreads, "Error opsPerThread.length"
					);
				NNThroughputBenchmark.nameNodeProto.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
					, false);
				// int generatedFileIdx = 0;
				NNThroughputBenchmark.Log.Info("Generate " + this.numOpsRequired + " intputs for "
					 + this.GetOpName());
				this.fileNames = new string[this.numThreads][];
				for (int idx = 0; idx < this.numThreads; idx++)
				{
					int threadOps = opsPerThread[idx];
					this.fileNames[idx] = new string[threadOps];
					for (int jdx = 0; jdx < threadOps; jdx++)
					{
						this.fileNames[idx][jdx] = this.nameGenerator.GetNextFileName("ThroughputBench");
					}
				}
			}

			internal virtual void DummyActionNoSynch(int daemonId, int fileIdx)
			{
				for (int i = 0; i < 2000; i++)
				{
					this.fileNames[daemonId][fileIdx].Contains(string.Empty + i);
				}
			}

			/// <summary>returns client name</summary>
			internal override string GetExecutionArgument(int daemonId)
			{
				return this.GetClientName(daemonId);
			}

			/// <summary>Do file create.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string clientName)
			{
				long start = Time.Now();
				// dummyActionNoSynch(fileIdx);
				NNThroughputBenchmark.nameNodeProto.Create(this.fileNames[daemonId][inputIdx], FsPermission
					.GetDefault(), clientName, new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag
					.Create, CreateFlag.Overwrite)), true, this.replication, NNThroughputBenchmark.BlockSize
					, null);
				long end = Time.Now();
				for (bool written = !this.closeUponCreate; !written; written = NNThroughputBenchmark
					.nameNodeProto.Complete(this.fileNames[daemonId][inputIdx], clientName, null, INodeId
					.GrandfatherInodeId))
				{
				}
				return end - start;
			}

			internal override void PrintResults()
			{
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " inputs ---");
				NNThroughputBenchmark.Log.Info("nrFiles = " + this.numOpsRequired);
				NNThroughputBenchmark.Log.Info("nrThreads = " + this.numThreads);
				NNThroughputBenchmark.Log.Info("nrFilesPerDir = " + this.nameGenerator.GetFilesPerDirectory
					());
				this.PrintStats();
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Directory creation statistics.</summary>
		/// <remarks>
		/// Directory creation statistics.
		/// Each thread creates the same (+ or -1) number of directories.
		/// Directory names are pre-generated during initialization.
		/// </remarks>
		internal class MkdirsStats : NNThroughputBenchmark.OperationStatsBase
		{
			internal const string OpMkdirsName = "mkdirs";

			internal const string OpMkdirsUsage = "-op mkdirs [-threads T] [-dirs N] " + "[-dirsPerDir P]";

			protected internal FileNameGenerator nameGenerator;

			protected internal string[][] dirPaths;

			internal MkdirsStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				// Operation types
				this.ParseArguments(args);
			}

			internal override string GetOpName()
			{
				return NNThroughputBenchmark.MkdirsStats.OpMkdirsName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				bool ignoreUnrelatedOptions = this.VerifyOpArgument(args);
				int nrDirsPerDir = 2;
				for (int i = 2; i < args.Count; i++)
				{
					// parse command line
					if (args[i].Equals("-dirs"))
					{
						if (i + 1 == args.Count)
						{
							NNThroughputBenchmark.PrintUsage();
						}
						this.numOpsRequired = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-threads"))
						{
							if (i + 1 == args.Count)
							{
								NNThroughputBenchmark.PrintUsage();
							}
							this.numThreads = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-dirsPerDir"))
							{
								if (i + 1 == args.Count)
								{
									NNThroughputBenchmark.PrintUsage();
								}
								nrDirsPerDir = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (!ignoreUnrelatedOptions)
								{
									NNThroughputBenchmark.PrintUsage();
								}
							}
						}
					}
				}
				this.nameGenerator = new FileNameGenerator(this.GetBaseDir(), nrDirsPerDir);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] opsPerThread)
			{
				System.Diagnostics.Debug.Assert(opsPerThread.Length == this.numThreads, "Error opsPerThread.length"
					);
				NNThroughputBenchmark.nameNodeProto.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
					, false);
				NNThroughputBenchmark.Log.Info("Generate " + this.numOpsRequired + " inputs for "
					 + this.GetOpName());
				this.dirPaths = new string[this.numThreads][];
				for (int idx = 0; idx < this.numThreads; idx++)
				{
					int threadOps = opsPerThread[idx];
					this.dirPaths[idx] = new string[threadOps];
					for (int jdx = 0; jdx < threadOps; jdx++)
					{
						this.dirPaths[idx][jdx] = this.nameGenerator.GetNextFileName("ThroughputBench");
					}
				}
			}

			/// <summary>returns client name</summary>
			internal override string GetExecutionArgument(int daemonId)
			{
				return this.GetClientName(daemonId);
			}

			/// <summary>Do mkdirs operation.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string clientName)
			{
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.Mkdirs(this.dirPaths[daemonId][inputIdx], FsPermission
					.GetDefault(), true);
				long end = Time.Now();
				return end - start;
			}

			internal override void PrintResults()
			{
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " inputs ---");
				NNThroughputBenchmark.Log.Info("nrDirs = " + this.numOpsRequired);
				NNThroughputBenchmark.Log.Info("nrThreads = " + this.numThreads);
				NNThroughputBenchmark.Log.Info("nrDirsPerDir = " + this.nameGenerator.GetFilesPerDirectory
					());
				this.PrintStats();
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Open file statistics.</summary>
		/// <remarks>
		/// Open file statistics.
		/// Measure how many open calls (getBlockLocations())
		/// the name-node can handle per second.
		/// </remarks>
		internal class OpenFileStats : NNThroughputBenchmark.CreateFileStats
		{
			internal const string OpOpenName = "open";

			internal const string OpUsageArgs = " [-threads T] [-files N] [-filesPerDir P] [-useExisting]";

			internal const string OpOpenUsage = "-op " + NNThroughputBenchmark.OpenFileStats.
				OpOpenName + NNThroughputBenchmark.OpenFileStats.OpUsageArgs;

			private bool useExisting;

			internal OpenFileStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			// Operation types
			// do not generate files, use existing ones
			internal override string GetOpName()
			{
				return NNThroughputBenchmark.OpenFileStats.OpOpenName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				int ueIndex = args.IndexOf("-useExisting");
				this.useExisting = (ueIndex >= 0);
				if (this.useExisting)
				{
					args.Remove(ueIndex);
				}
				base.ParseArguments(args);
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] opsPerThread)
			{
				// create files using opsPerThread
				string[] createArgs = new string[] { "-op", "create", "-threads", this.numThreads
					.ToString(), "-files", this.numOpsRequired.ToString(), "-filesPerDir", this.nameGenerator
					.GetFilesPerDirectory().ToString(), "-close" };
				NNThroughputBenchmark.CreateFileStats opCreate = new NNThroughputBenchmark.CreateFileStats
					(this, Arrays.AsList(createArgs));
				if (!this.useExisting)
				{
					// create files if they were not created before
					opCreate.Benchmark();
					NNThroughputBenchmark.Log.Info("Created " + this.numOpsRequired + " files.");
				}
				else
				{
					NNThroughputBenchmark.Log.Info("useExisting = true. Assuming " + this.numOpsRequired
						 + " files have been created before.");
				}
				// use the same files for open
				base.GenerateInputs(opsPerThread);
				if (NNThroughputBenchmark.nameNodeProto.GetFileInfo(opCreate.GetBaseDir()) != null
					 && NNThroughputBenchmark.nameNodeProto.GetFileInfo(this.GetBaseDir()) == null)
				{
					NNThroughputBenchmark.nameNodeProto.Rename(opCreate.GetBaseDir(), this.GetBaseDir
						());
				}
				if (NNThroughputBenchmark.nameNodeProto.GetFileInfo(this.GetBaseDir()) == null)
				{
					throw new IOException(this.GetBaseDir() + " does not exist.");
				}
			}

			/// <summary>Do file open.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.GetBlockLocations(this.fileNames[daemonId][inputIdx
					], 0L, NNThroughputBenchmark.BlockSize);
				long end = Time.Now();
				return end - start;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Delete file statistics.</summary>
		/// <remarks>
		/// Delete file statistics.
		/// Measure how many delete calls the name-node can handle per second.
		/// </remarks>
		internal class DeleteFileStats : NNThroughputBenchmark.OpenFileStats
		{
			internal const string OpDeleteName = "delete";

			internal const string OpDeleteUsage = "-op " + NNThroughputBenchmark.DeleteFileStats
				.OpDeleteName + NNThroughputBenchmark.OpenFileStats.OpUsageArgs;

			internal DeleteFileStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			// Operation types
			internal override string GetOpName()
			{
				return NNThroughputBenchmark.DeleteFileStats.OpDeleteName;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.Delete(this.fileNames[daemonId][inputIdx], false
					);
				long end = Time.Now();
				return end - start;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>List file status statistics.</summary>
		/// <remarks>
		/// List file status statistics.
		/// Measure how many get-file-status calls the name-node can handle per second.
		/// </remarks>
		internal class FileStatusStats : NNThroughputBenchmark.OpenFileStats
		{
			internal const string OpFileStatusName = "fileStatus";

			internal const string OpFileStatusUsage = "-op " + NNThroughputBenchmark.FileStatusStats
				.OpFileStatusName + NNThroughputBenchmark.OpenFileStats.OpUsageArgs;

			internal FileStatusStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			// Operation types
			internal override string GetOpName()
			{
				return NNThroughputBenchmark.FileStatusStats.OpFileStatusName;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.GetFileInfo(this.fileNames[daemonId][inputIdx
					]);
				long end = Time.Now();
				return end - start;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Rename file statistics.</summary>
		/// <remarks>
		/// Rename file statistics.
		/// Measure how many rename calls the name-node can handle per second.
		/// </remarks>
		internal class RenameFileStats : NNThroughputBenchmark.OpenFileStats
		{
			internal const string OpRenameName = "rename";

			internal const string OpRenameUsage = "-op " + NNThroughputBenchmark.RenameFileStats
				.OpRenameName + NNThroughputBenchmark.OpenFileStats.OpUsageArgs;

			protected internal string[][] destNames;

			internal RenameFileStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			// Operation types
			internal override string GetOpName()
			{
				return NNThroughputBenchmark.RenameFileStats.OpRenameName;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] opsPerThread)
			{
				base.GenerateInputs(opsPerThread);
				this.destNames = new string[this.fileNames.Length][];
				for (int idx = 0; idx < this.numThreads; idx++)
				{
					int nrNames = this.fileNames[idx].Length;
					this.destNames[idx] = new string[nrNames];
					for (int jdx = 0; jdx < nrNames; jdx++)
					{
						this.destNames[idx][jdx] = this.fileNames[idx][jdx] + ".r";
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				long start = Time.Now();
				NNThroughputBenchmark.nameNodeProto.Rename(this.fileNames[daemonId][inputIdx], this
					.destNames[daemonId][inputIdx]);
				long end = Time.Now();
				return end - start;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Minimal data-node simulator.</summary>
		private class TinyDatanode : Comparable<string>
		{
			private const long DfCapacity = 100 * 1024 * 1024;

			private const long DfUsed = 0;

			internal NamespaceInfo nsInfo;

			internal DatanodeRegistration dnRegistration;

			internal DatanodeStorage storage;

			internal readonly AList<BlockListAsLongs.BlockReportReplica> blocks;

			internal int nrBlocks;

			internal BlockListAsLongs blockReportList;

			internal readonly int dnIdx;

			//only one storage 
			// actual number of blocks
			/// <exception cref="System.IO.IOException"/>
			private static int GetNodePort(int num)
			{
				int port = 1 + num;
				Preconditions.CheckState(port < short.MaxValue);
				return port;
			}

			/// <exception cref="System.IO.IOException"/>
			internal TinyDatanode(int dnIdx, int blockCapacity)
			{
				this.dnIdx = dnIdx;
				this.blocks = new AList<BlockListAsLongs.BlockReportReplica>(blockCapacity);
				this.nrBlocks = 0;
			}

			public override string ToString()
			{
				return dnRegistration.ToString();
			}

			internal virtual string GetXferAddr()
			{
				return dnRegistration.GetXferAddr();
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Register()
			{
				// get versions from the namenode
				nsInfo = nameNodeProto.VersionRequest();
				dnRegistration = new DatanodeRegistration(new DatanodeID(DNS.GetDefaultIP("default"
					), DNS.GetDefaultHost("default", "default"), DataNode.GenerateUuid(), GetNodePort
					(dnIdx), DFSConfigKeys.DfsDatanodeHttpDefaultPort, DFSConfigKeys.DfsDatanodeHttpsDefaultPort
					, DFSConfigKeys.DfsDatanodeIpcDefaultPort), new DataStorage(nsInfo), new ExportedBlockKeys
					(), VersionInfo.GetVersion());
				// register datanode
				dnRegistration = nameNodeProto.RegisterDatanode(dnRegistration);
				//first block reports
				storage = new DatanodeStorage(DatanodeStorage.GenerateUuid());
				StorageBlockReport[] reports = new StorageBlockReport[] { new StorageBlockReport(
					storage, BlockListAsLongs.Empty) };
				nameNodeProto.BlockReport(dnRegistration, nameNode.GetNamesystem().GetBlockPoolId
					(), reports, new BlockReportContext(1, 0, Runtime.NanoTime()));
			}

			/// <summary>Send a heartbeat to the name-node.</summary>
			/// <remarks>
			/// Send a heartbeat to the name-node.
			/// Ignore reply commands.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void SendHeartbeat()
			{
				// register datanode
				// TODO:FEDERATION currently a single block pool is supported
				StorageReport[] rep = new StorageReport[] { new StorageReport(storage, false, DfCapacity
					, DfUsed, DfCapacity - DfUsed, DfUsed) };
				DatanodeCommand[] cmds = nameNodeProto.SendHeartbeat(dnRegistration, rep, 0L, 0L, 
					0, 0, 0, null).GetCommands();
				if (cmds != null)
				{
					foreach (DatanodeCommand cmd in cmds)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("sendHeartbeat Name-node reply: " + cmd.GetAction());
						}
					}
				}
			}

			internal virtual bool AddBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block blk)
			{
				if (nrBlocks == blocks.Count)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Cannot add block: datanode capacity = " + blocks.Count);
					}
					return false;
				}
				blocks.Set(nrBlocks, new BlockListAsLongs.BlockReportReplica(blk));
				nrBlocks++;
				return true;
			}

			internal virtual void FormBlockReport()
			{
				// fill remaining slots with blocks that do not exist
				for (int idx = blocks.Count - 1; idx >= nrBlocks; idx--)
				{
					Org.Apache.Hadoop.Hdfs.Protocol.Block block = new Org.Apache.Hadoop.Hdfs.Protocol.Block
						(blocks.Count - idx, 0, 0);
					blocks.Set(idx, new BlockListAsLongs.BlockReportReplica(block));
				}
				blockReportList = BlockListAsLongs.Empty;
			}

			internal virtual BlockListAsLongs GetBlockReportList()
			{
				return blockReportList;
			}

			public virtual int CompareTo(string xferAddr)
			{
				return string.CompareOrdinal(GetXferAddr(), xferAddr);
			}

			/// <summary>Send a heartbeat to the name-node and replicate blocks if requested.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual int ReplicateBlocks()
			{
				// keep it for future blockReceived benchmark
				// register datanode
				StorageReport[] rep = new StorageReport[] { new StorageReport(storage, false, DfCapacity
					, DfUsed, DfCapacity - DfUsed, DfUsed) };
				DatanodeCommand[] cmds = nameNodeProto.SendHeartbeat(dnRegistration, rep, 0L, 0L, 
					0, 0, 0, null).GetCommands();
				if (cmds != null)
				{
					foreach (DatanodeCommand cmd in cmds)
					{
						if (cmd.GetAction() == DatanodeProtocol.DnaTransfer)
						{
							// Send a copy of a block to another datanode
							BlockCommand bcmd = (BlockCommand)cmd;
							return TransferBlocks(bcmd.GetBlocks(), bcmd.GetTargets(), bcmd.GetTargetStorageIDs
								());
						}
					}
				}
				return 0;
			}

			/// <summary>Transfer blocks to another data-node.</summary>
			/// <remarks>
			/// Transfer blocks to another data-node.
			/// Just report on behalf of the other data-node
			/// that the blocks have been received.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			private int TransferBlocks(Org.Apache.Hadoop.Hdfs.Protocol.Block[] blocks, DatanodeInfo
				[][] xferTargets, string[][] targetStorageIDs)
			{
				for (int i = 0; i < blocks.Length; i++)
				{
					DatanodeInfo[] blockTargets = xferTargets[i];
					for (int t = 0; t < blockTargets.Length; t++)
					{
						DatanodeInfo dnInfo = blockTargets[t];
						string targetStorageID = targetStorageIDs[i][t];
						DatanodeRegistration receivedDNReg;
						receivedDNReg = new DatanodeRegistration(dnInfo, new DataStorage(nsInfo), new ExportedBlockKeys
							(), VersionInfo.GetVersion());
						ReceivedDeletedBlockInfo[] rdBlocks = new ReceivedDeletedBlockInfo[] { new ReceivedDeletedBlockInfo
							(blocks[i], ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock, null) };
						StorageReceivedDeletedBlocks[] report = new StorageReceivedDeletedBlocks[] { new 
							StorageReceivedDeletedBlocks(targetStorageID, rdBlocks) };
						nameNodeProto.BlockReceivedAndDeleted(receivedDNReg, nameNode.GetNamesystem().GetBlockPoolId
							(), report);
					}
				}
				return blocks.Length;
			}
		}

		/// <summary>Block report statistics.</summary>
		/// <remarks>
		/// Block report statistics.
		/// Each thread here represents its own data-node.
		/// Data-nodes send the same block report each time.
		/// The block report may contain missing or non-existing blocks.
		/// </remarks>
		internal class BlockReportStats : NNThroughputBenchmark.OperationStatsBase
		{
			internal const string OpBlockReportName = "blockReport";

			internal const string OpBlockReportUsage = "-op blockReport [-datanodes T] [-reports N] "
				 + "[-blocksPerReport B] [-blocksPerFile F]";

			private int blocksPerReport;

			private int blocksPerFile;

			private NNThroughputBenchmark.TinyDatanode[] datanodes;

			internal BlockReportStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				// array of data-nodes sorted by name
				this.blocksPerReport = 100;
				this.blocksPerFile = 10;
				// set heartbeat interval to 3 min, so that expiration were 40 min
				NNThroughputBenchmark.config.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 3 * 60
					);
				this.ParseArguments(args);
				// adjust replication to the number of data-nodes
				this.replication = (short)Math.Min(this.replication, this.GetNumDatanodes());
			}

			/// <summary>Each thread pretends its a data-node here.</summary>
			private int GetNumDatanodes()
			{
				return this.numThreads;
			}

			internal override string GetOpName()
			{
				return NNThroughputBenchmark.BlockReportStats.OpBlockReportName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				bool ignoreUnrelatedOptions = this.VerifyOpArgument(args);
				for (int i = 2; i < args.Count; i++)
				{
					// parse command line
					if (args[i].Equals("-reports"))
					{
						if (i + 1 == args.Count)
						{
							NNThroughputBenchmark.PrintUsage();
						}
						this.numOpsRequired = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-datanodes"))
						{
							if (i + 1 == args.Count)
							{
								NNThroughputBenchmark.PrintUsage();
							}
							this.numThreads = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-blocksPerReport"))
							{
								if (i + 1 == args.Count)
								{
									NNThroughputBenchmark.PrintUsage();
								}
								this.blocksPerReport = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-blocksPerFile"))
								{
									if (i + 1 == args.Count)
									{
										NNThroughputBenchmark.PrintUsage();
									}
									this.blocksPerFile = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (!ignoreUnrelatedOptions)
									{
										NNThroughputBenchmark.PrintUsage();
									}
								}
							}
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] ignore)
			{
				int nrDatanodes = this.GetNumDatanodes();
				int nrBlocks = (int)Math.Ceil((double)this.blocksPerReport * nrDatanodes / this.replication
					);
				int nrFiles = (int)Math.Ceil((double)nrBlocks / this.blocksPerFile);
				this.datanodes = new NNThroughputBenchmark.TinyDatanode[nrDatanodes];
				// create data-nodes
				string prevDNName = string.Empty;
				for (int idx = 0; idx < nrDatanodes; idx++)
				{
					this.datanodes[idx] = new NNThroughputBenchmark.TinyDatanode(idx, this.blocksPerReport
						);
					this.datanodes[idx].Register();
					System.Diagnostics.Debug.Assert(string.CompareOrdinal(this.datanodes[idx].GetXferAddr
						(), prevDNName) > 0, "Data-nodes must be sorted lexicographically.");
					this.datanodes[idx].SendHeartbeat();
					prevDNName = this.datanodes[idx].GetXferAddr();
				}
				// create files 
				NNThroughputBenchmark.Log.Info("Creating " + nrFiles + " files with " + this.blocksPerFile
					 + " blocks each.");
				FileNameGenerator nameGenerator;
				nameGenerator = new FileNameGenerator(this.GetBaseDir(), 100);
				string clientName = this.GetClientName(0x7);
				NNThroughputBenchmark.nameNodeProto.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave
					, false);
				for (int idx_1 = 0; idx_1 < nrFiles; idx_1++)
				{
					string fileName = nameGenerator.GetNextFileName("ThroughputBench");
					NNThroughputBenchmark.nameNodeProto.Create(fileName, FsPermission.GetDefault(), clientName
						, new EnumSetWritable<CreateFlag>(EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite
						)), true, this.replication, NNThroughputBenchmark.BlockSize, null);
					ExtendedBlock lastBlock = this.AddBlocks(fileName, clientName);
					NNThroughputBenchmark.nameNodeProto.Complete(fileName, clientName, lastBlock, INodeId
						.GrandfatherInodeId);
				}
				// prepare block reports
				for (int idx_2 = 0; idx_2 < nrDatanodes; idx_2++)
				{
					this.datanodes[idx_2].FormBlockReport();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private ExtendedBlock AddBlocks(string fileName, string clientName)
			{
				ExtendedBlock prevBlock = null;
				for (int jdx = 0; jdx < this.blocksPerFile; jdx++)
				{
					LocatedBlock loc = NNThroughputBenchmark.nameNodeProto.AddBlock(fileName, clientName
						, prevBlock, null, INodeId.GrandfatherInodeId, null);
					prevBlock = loc.GetBlock();
					foreach (DatanodeInfo dnInfo in loc.GetLocations())
					{
						int dnIdx = System.Array.BinarySearch(this.datanodes, dnInfo.GetXferAddr());
						this.datanodes[dnIdx].AddBlock(loc.GetBlock().GetLocalBlock());
						ReceivedDeletedBlockInfo[] rdBlocks = new ReceivedDeletedBlockInfo[] { new ReceivedDeletedBlockInfo
							(loc.GetBlock().GetLocalBlock(), ReceivedDeletedBlockInfo.BlockStatus.ReceivedBlock
							, null) };
						StorageReceivedDeletedBlocks[] report = new StorageReceivedDeletedBlocks[] { new 
							StorageReceivedDeletedBlocks(this.datanodes[dnIdx].storage.GetStorageID(), rdBlocks
							) };
						NNThroughputBenchmark.nameNodeProto.BlockReceivedAndDeleted(this.datanodes[dnIdx]
							.dnRegistration, loc.GetBlock().GetBlockPoolId(), report);
					}
				}
				return prevBlock;
			}

			/// <summary>Does not require the argument</summary>
			internal override string GetExecutionArgument(int daemonId)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				System.Diagnostics.Debug.Assert(daemonId < this.numThreads, "Wrong daemonId.");
				NNThroughputBenchmark.TinyDatanode dn = this.datanodes[daemonId];
				long start = Time.Now();
				StorageBlockReport[] report = new StorageBlockReport[] { new StorageBlockReport(dn
					.storage, dn.GetBlockReportList()) };
				NNThroughputBenchmark.nameNodeProto.BlockReport(dn.dnRegistration, NNThroughputBenchmark
					.nameNode.GetNamesystem().GetBlockPoolId(), report, new BlockReportContext(1, 0, 
					Runtime.NanoTime()));
				long end = Time.Now();
				return end - start;
			}

			internal override void PrintResults()
			{
				string blockDistribution = string.Empty;
				string delim = "(";
				for (int idx = 0; idx < this.GetNumDatanodes(); idx++)
				{
					blockDistribution += delim + this.datanodes[idx].nrBlocks;
					delim = ", ";
				}
				blockDistribution += ")";
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " inputs ---");
				NNThroughputBenchmark.Log.Info("reports = " + this.numOpsRequired);
				NNThroughputBenchmark.Log.Info("datanodes = " + this.numThreads + " " + blockDistribution
					);
				NNThroughputBenchmark.Log.Info("blocksPerReport = " + this.blocksPerReport);
				NNThroughputBenchmark.Log.Info("blocksPerFile = " + this.blocksPerFile);
				this.PrintStats();
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		/// <summary>Measures how fast replication monitor can compute data-node work.</summary>
		/// <remarks>
		/// Measures how fast replication monitor can compute data-node work.
		/// It runs only one thread until no more work can be scheduled.
		/// </remarks>
		internal class ReplicationStats : NNThroughputBenchmark.OperationStatsBase
		{
			internal const string OpReplicationName = "replication";

			internal const string OpReplicationUsage = "-op replication [-datanodes T] [-nodesToDecommission D] "
				 + "[-nodeReplicationLimit C] [-totalBlocks B] [-replication R]";

			private readonly NNThroughputBenchmark.BlockReportStats blockReportObject;

			private int numDatanodes;

			private int nodesToDecommission;

			private int nodeReplicationLimit;

			private int totalBlocks;

			private int numDecommissionedBlocks;

			private int numPendingBlocks;

			internal ReplicationStats(NNThroughputBenchmark _enclosing, IList<string> args)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				// end BlockReportStats
				this.numThreads = 1;
				this.numDatanodes = 3;
				this.nodesToDecommission = 1;
				this.nodeReplicationLimit = 100;
				this.totalBlocks = 100;
				this.ParseArguments(args);
				// number of operations is 4 times the number of decommissioned
				// blocks divided by the number of needed replications scanned 
				// by the replication monitor in one iteration
				this.numOpsRequired = (this.totalBlocks * this.replication * this.nodesToDecommission
					 * 2) / (this.numDatanodes * this.numDatanodes);
				string[] blkReportArgs = new string[] { "-op", "blockReport", "-datanodes", this.
					numDatanodes.ToString(), "-blocksPerReport", (this.totalBlocks * this.replication
					 / this.numDatanodes).ToString(), "-blocksPerFile", this.numDatanodes.ToString()
					 };
				this.blockReportObject = new NNThroughputBenchmark.BlockReportStats(this, Arrays.
					AsList(blkReportArgs));
				this.numDecommissionedBlocks = 0;
				this.numPendingBlocks = 0;
			}

			internal override string GetOpName()
			{
				return NNThroughputBenchmark.ReplicationStats.OpReplicationName;
			}

			internal override void ParseArguments(IList<string> args)
			{
				bool ignoreUnrelatedOptions = this.VerifyOpArgument(args);
				for (int i = 2; i < args.Count; i++)
				{
					// parse command line
					if (args[i].Equals("-datanodes"))
					{
						if (i + 1 == args.Count)
						{
							NNThroughputBenchmark.PrintUsage();
						}
						this.numDatanodes = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-nodesToDecommission"))
						{
							if (i + 1 == args.Count)
							{
								NNThroughputBenchmark.PrintUsage();
							}
							this.nodesToDecommission = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-nodeReplicationLimit"))
							{
								if (i + 1 == args.Count)
								{
									NNThroughputBenchmark.PrintUsage();
								}
								this.nodeReplicationLimit = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-totalBlocks"))
								{
									if (i + 1 == args.Count)
									{
										NNThroughputBenchmark.PrintUsage();
									}
									this.totalBlocks = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (args[i].Equals("-replication"))
									{
										if (i + 1 == args.Count)
										{
											NNThroughputBenchmark.PrintUsage();
										}
										this.replication = short.ParseShort(args[++i]);
									}
									else
									{
										if (!ignoreUnrelatedOptions)
										{
											NNThroughputBenchmark.PrintUsage();
										}
									}
								}
							}
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void GenerateInputs(int[] ignore)
			{
				FSNamesystem namesystem = NNThroughputBenchmark.nameNode.GetNamesystem();
				// start data-nodes; create a bunch of files; generate block reports.
				this.blockReportObject.GenerateInputs(ignore);
				// stop replication monitor
				BlockManagerTestUtil.StopReplicationThread(namesystem.GetBlockManager());
				// report blocks once
				int nrDatanodes = this.blockReportObject.GetNumDatanodes();
				for (int idx = 0; idx < nrDatanodes; idx++)
				{
					this.blockReportObject.ExecuteOp(idx, 0, null);
				}
				// decommission data-nodes
				this.DecommissionNodes();
				// set node replication limit
				BlockManagerTestUtil.SetNodeReplicationLimit(namesystem.GetBlockManager(), this.nodeReplicationLimit
					);
			}

			/// <exception cref="System.IO.IOException"/>
			private void DecommissionNodes()
			{
				string excludeFN = NNThroughputBenchmark.config.Get(DFSConfigKeys.DfsHostsExclude
					, "exclude");
				FileOutputStream excludeFile = new FileOutputStream(excludeFN);
				excludeFile.GetChannel().Truncate(0L);
				int nrDatanodes = this.blockReportObject.GetNumDatanodes();
				this.numDecommissionedBlocks = 0;
				for (int i = 0; i < this.nodesToDecommission; i++)
				{
					NNThroughputBenchmark.TinyDatanode dn = this.blockReportObject.datanodes[nrDatanodes
						 - 1 - i];
					this.numDecommissionedBlocks += dn.nrBlocks;
					excludeFile.Write(Sharpen.Runtime.GetBytesForString(dn.GetXferAddr()));
					excludeFile.Write('\n');
					NNThroughputBenchmark.Log.Info("Datanode " + dn + " is decommissioned.");
				}
				excludeFile.Close();
				NNThroughputBenchmark.nameNodeProto.RefreshNodes();
			}

			/// <summary>Does not require the argument</summary>
			internal override string GetExecutionArgument(int daemonId)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override long ExecuteOp(int daemonId, int inputIdx, string ignore)
			{
				System.Diagnostics.Debug.Assert(daemonId < this.numThreads, "Wrong daemonId.");
				long start = Time.Now();
				// compute data-node work
				int work = BlockManagerTestUtil.GetComputedDatanodeWork(NNThroughputBenchmark.nameNode
					.GetNamesystem().GetBlockManager());
				long end = Time.Now();
				this.numPendingBlocks += work;
				if (work == 0)
				{
					this.daemons[daemonId].Terminate();
				}
				return end - start;
			}

			internal override void PrintResults()
			{
				string blockDistribution = string.Empty;
				string delim = "(";
				for (int idx = 0; idx < this.blockReportObject.GetNumDatanodes(); idx++)
				{
					blockDistribution += delim + this.blockReportObject.datanodes[idx].nrBlocks;
					delim = ", ";
				}
				blockDistribution += ")";
				NNThroughputBenchmark.Log.Info("--- " + this.GetOpName() + " inputs ---");
				NNThroughputBenchmark.Log.Info("numOpsRequired = " + this.numOpsRequired);
				NNThroughputBenchmark.Log.Info("datanodes = " + this.numDatanodes + " " + blockDistribution
					);
				NNThroughputBenchmark.Log.Info("decommissioned datanodes = " + this.nodesToDecommission
					);
				NNThroughputBenchmark.Log.Info("datanode replication limit = " + this.nodeReplicationLimit
					);
				NNThroughputBenchmark.Log.Info("total blocks = " + this.totalBlocks);
				this.PrintStats();
				NNThroughputBenchmark.Log.Info("decommissioned blocks = " + this.numDecommissionedBlocks
					);
				NNThroughputBenchmark.Log.Info("pending replications = " + this.numPendingBlocks);
				NNThroughputBenchmark.Log.Info("replications per sec: " + this.GetBlocksPerSecond
					());
			}

			private double GetBlocksPerSecond()
			{
				return this.elapsedTime == 0 ? 0 : 1000 * (double)this.numPendingBlocks / this.elapsedTime;
			}

			private readonly NNThroughputBenchmark _enclosing;
		}

		// end ReplicationStats
		internal static void PrintUsage()
		{
			System.Console.Error.WriteLine("Usage: NNThroughputBenchmark" + "\n\t" + NNThroughputBenchmark.OperationStatsBase
				.OpAllUsage + " | \n\t" + NNThroughputBenchmark.CreateFileStats.OpCreateUsage + 
				" | \n\t" + NNThroughputBenchmark.MkdirsStats.OpMkdirsUsage + " | \n\t" + NNThroughputBenchmark.OpenFileStats
				.OpOpenUsage + " | \n\t" + NNThroughputBenchmark.DeleteFileStats.OpDeleteUsage +
				 " | \n\t" + NNThroughputBenchmark.FileStatusStats.OpFileStatusUsage + " | \n\t"
				 + NNThroughputBenchmark.RenameFileStats.OpRenameUsage + " | \n\t" + NNThroughputBenchmark.BlockReportStats
				.OpBlockReportUsage + " | \n\t" + NNThroughputBenchmark.ReplicationStats.OpReplicationUsage
				 + " | \n\t" + NNThroughputBenchmark.CleanAllStats.OpCleanUsage + " | \n\t" + GeneralOptionsUsage
				);
			System.Environment.Exit(-1);
		}

		/// <exception cref="System.Exception"/>
		public static void RunBenchmark(Configuration conf, IList<string> args)
		{
			NNThroughputBenchmark bench = null;
			try
			{
				bench = new NNThroughputBenchmark(conf);
				bench.Run(Sharpen.Collections.ToArray(args, new string[] {  }));
			}
			finally
			{
				if (bench != null)
				{
					bench.Close();
				}
			}
		}

		/// <summary>Main method of the benchmark.</summary>
		/// <param name="aArgs">command line parameters</param>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] aArgs)
		{
			// Tool
			IList<string> args = new AList<string>(Arrays.AsList(aArgs));
			if (args.Count < 2 || !args[0].StartsWith("-op"))
			{
				PrintUsage();
			}
			string type = args[1];
			bool runAll = NNThroughputBenchmark.OperationStatsBase.OpAllName.Equals(type);
			// Start the NameNode
			string[] argv = new string[] {  };
			nameNode = NameNode.CreateNameNode(argv, config);
			nameNodeProto = nameNode.GetRpcServer();
			IList<NNThroughputBenchmark.OperationStatsBase> ops = new AList<NNThroughputBenchmark.OperationStatsBase
				>();
			NNThroughputBenchmark.OperationStatsBase opStat = null;
			try
			{
				if (runAll || NNThroughputBenchmark.CreateFileStats.OpCreateName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.CreateFileStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.MkdirsStats.OpMkdirsName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.MkdirsStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.OpenFileStats.OpOpenName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.OpenFileStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.DeleteFileStats.OpDeleteName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.DeleteFileStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.FileStatusStats.OpFileStatusName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.FileStatusStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.RenameFileStats.OpRenameName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.RenameFileStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.BlockReportStats.OpBlockReportName.Equals(type
					))
				{
					opStat = new NNThroughputBenchmark.BlockReportStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.ReplicationStats.OpReplicationName.Equals(type
					))
				{
					opStat = new NNThroughputBenchmark.ReplicationStats(this, args);
					ops.AddItem(opStat);
				}
				if (runAll || NNThroughputBenchmark.CleanAllStats.OpCleanName.Equals(type))
				{
					opStat = new NNThroughputBenchmark.CleanAllStats(this, args);
					ops.AddItem(opStat);
				}
				if (ops.Count == 0)
				{
					PrintUsage();
				}
				// run each benchmark
				foreach (NNThroughputBenchmark.OperationStatsBase op in ops)
				{
					Log.Info("Starting benchmark: " + op.GetOpName());
					op.Benchmark();
					op.CleanUp();
				}
				// print statistics
				foreach (NNThroughputBenchmark.OperationStatsBase op_1 in ops)
				{
					Log.Info(string.Empty);
					op_1.PrintResults();
				}
			}
			catch (Exception e)
			{
				Log.Error(StringUtils.StringifyException(e));
				throw;
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			NNThroughputBenchmark bench = null;
			try
			{
				bench = new NNThroughputBenchmark(new HdfsConfiguration());
				ToolRunner.Run(bench, args);
			}
			finally
			{
				if (bench != null)
				{
					bench.Close();
				}
			}
		}

		public virtual void SetConf(Configuration conf)
		{
			// Configurable
			config = conf;
		}

		public virtual Configuration GetConf()
		{
			// Configurable
			return config;
		}
	}
}
