using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Balancer;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Mover
{
	public class Mover
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Mover.Mover
			));

		internal static readonly Path MoverIdPath = new Path("/system/mover.id");

		private class StorageMap
		{
			private readonly Dispatcher.StorageGroupMap<Dispatcher.Source> sources = new Dispatcher.StorageGroupMap
				<Dispatcher.Source>();

			private readonly Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup> targets
				 = new Dispatcher.StorageGroupMap<Dispatcher.DDatanode.StorageGroup>();

			private readonly EnumMap<StorageType, IList<Dispatcher.DDatanode.StorageGroup>> targetStorageTypeMap
				 = new EnumMap<StorageType, IList<Dispatcher.DDatanode.StorageGroup>>(typeof(StorageType
				));

			private StorageMap()
			{
				foreach (StorageType t in StorageType.GetMovableTypes())
				{
					targetStorageTypeMap[t] = new List<Dispatcher.DDatanode.StorageGroup>();
				}
			}

			private void Add(Dispatcher.Source source, Dispatcher.DDatanode.StorageGroup target
				)
			{
				sources.Put(source);
				if (target != null)
				{
					targets.Put(target);
					GetTargetStorages(target.GetStorageType()).AddItem(target);
				}
			}

			private Dispatcher.Source GetSource(Mover.MLocation ml)
			{
				return Get(sources, ml);
			}

			private Dispatcher.DDatanode.StorageGroup GetTarget(string uuid, StorageType storageType
				)
			{
				return targets.Get(uuid, storageType);
			}

			private static G Get<G>(Dispatcher.StorageGroupMap<G> map, Mover.MLocation ml)
				where G : Dispatcher.DDatanode.StorageGroup
			{
				return map.Get(ml.datanode.GetDatanodeUuid(), ml.storageType);
			}

			private IList<Dispatcher.DDatanode.StorageGroup> GetTargetStorages(StorageType t)
			{
				return targetStorageTypeMap[t];
			}
		}

		private readonly Dispatcher dispatcher;

		private readonly Mover.StorageMap storages;

		private readonly IList<Path> targetPaths;

		private readonly int retryMaxAttempts;

		private readonly AtomicInteger retryCount;

		private readonly BlockStoragePolicy[] blockStoragePolicies;

		internal Mover(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount
			)
		{
			long movedWinWidth = conf.GetLong(DFSConfigKeys.DfsMoverMovedwinwidthKey, DFSConfigKeys
				.DfsMoverMovedwinwidthDefault);
			int moverThreads = conf.GetInt(DFSConfigKeys.DfsMoverMoverthreadsKey, DFSConfigKeys
				.DfsMoverMoverthreadsDefault);
			int maxConcurrentMovesPerNode = conf.GetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey
				, DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesDefault);
			this.retryMaxAttempts = conf.GetInt(DFSConfigKeys.DfsMoverRetryMaxAttemptsKey, DFSConfigKeys
				.DfsMoverRetryMaxAttemptsDefault);
			this.retryCount = retryCount;
			this.dispatcher = new Dispatcher(nnc, Sharpen.Collections.EmptySet<string>(), Sharpen.Collections
				.EmptySet<string>(), movedWinWidth, moverThreads, 0, maxConcurrentMovesPerNode, 
				conf);
			this.storages = new Mover.StorageMap();
			this.targetPaths = nnc.GetTargetPaths();
			this.blockStoragePolicies = new BlockStoragePolicy[1 << BlockStoragePolicySuite.IdBitLength
				];
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Init()
		{
			InitStoragePolicies();
			IList<DatanodeStorageReport> reports = dispatcher.Init();
			foreach (DatanodeStorageReport r in reports)
			{
				Dispatcher.DDatanode dn = dispatcher.NewDatanode(r.GetDatanodeInfo());
				foreach (StorageType t in StorageType.GetMovableTypes())
				{
					Dispatcher.Source source = dn.AddSource(t, long.MaxValue, dispatcher);
					long maxRemaining = GetMaxRemaining(r, t);
					Dispatcher.DDatanode.StorageGroup target = maxRemaining > 0L ? dn.AddTarget(t, maxRemaining
						) : null;
					storages.Add(source, target);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitStoragePolicies()
		{
			BlockStoragePolicy[] policies = dispatcher.GetDistributedFileSystem().GetStoragePolicies
				();
			foreach (BlockStoragePolicy policy in policies)
			{
				this.blockStoragePolicies[policy.GetId()] = policy;
			}
		}

		private ExitStatus Run()
		{
			try
			{
				Init();
				bool hasRemaining = new Mover.Processor(this).ProcessNamespace();
				return hasRemaining ? ExitStatus.InProgress : ExitStatus.Success;
			}
			catch (ArgumentException e)
			{
				System.Console.Out.WriteLine(e + ".  Exiting ...");
				return ExitStatus.IllegalArguments;
			}
			catch (IOException e)
			{
				System.Console.Out.WriteLine(e + ".  Exiting ...");
				return ExitStatus.IoException;
			}
			finally
			{
				dispatcher.ShutdownNow();
			}
		}

		internal virtual Dispatcher.DBlock NewDBlock(Block block, IList<Mover.MLocation> 
			locations)
		{
			Dispatcher.DBlock db = new Dispatcher.DBlock(block);
			foreach (Mover.MLocation ml in locations)
			{
				Dispatcher.DDatanode.StorageGroup source = storages.GetSource(ml);
				if (source != null)
				{
					db.AddLocation(source);
				}
			}
			return db;
		}

		private static long GetMaxRemaining(DatanodeStorageReport report, StorageType t)
		{
			long max = 0L;
			foreach (StorageReport r in report.GetStorageReports())
			{
				if (r.GetStorage().GetStorageType() == t)
				{
					if (r.GetRemaining() > max)
					{
						max = r.GetRemaining();
					}
				}
			}
			return max;
		}

		/// <summary>convert a snapshot path to non-snapshot path.</summary>
		/// <remarks>
		/// convert a snapshot path to non-snapshot path. E.g.,
		/// /foo/.snapshot/snapshot-name/bar --&gt; /foo/bar
		/// </remarks>
		private static string ConvertSnapshotPath(string[] pathComponents)
		{
			StringBuilder sb = new StringBuilder(Path.Separator);
			for (int i = 0; i < pathComponents.Length; i++)
			{
				if (pathComponents[i].Equals(HdfsConstants.DotSnapshotDir))
				{
					i++;
				}
				else
				{
					sb.Append(pathComponents[i]);
				}
			}
			return sb.ToString();
		}

		internal class Processor
		{
			private readonly DFSClient dfs;

			private readonly IList<string> snapshottableDirs = new AList<string>();

			internal Processor(Mover _enclosing)
			{
				this._enclosing = _enclosing;
				this.dfs = this._enclosing.dispatcher.GetDistributedFileSystem().GetClient();
			}

			private void GetSnapshottableDirs()
			{
				SnapshottableDirectoryStatus[] dirs = null;
				try
				{
					dirs = this.dfs.GetSnapshottableDirListing();
				}
				catch (IOException e)
				{
					Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Log.Warn("Failed to get snapshottable directories."
						 + " Ignore and continue.", e);
				}
				if (dirs != null)
				{
					foreach (SnapshottableDirectoryStatus dir in dirs)
					{
						this.snapshottableDirs.AddItem(dir.GetFullPath().ToString());
					}
				}
			}

			/// <returns>
			/// true if the given path is a snapshot path and the corresponding
			/// INode is still in the current fsdirectory.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private bool IsSnapshotPathInCurrent(string path)
			{
				// if the parent path contains "/.snapshot/", this is a snapshot path
				if (path.Contains(HdfsConstants.SeparatorDotSnapshotDirSeparator))
				{
					string[] pathComponents = INode.GetPathNames(path);
					if (HdfsConstants.DotSnapshotDir.Equals(pathComponents[pathComponents.Length - 2]
						))
					{
						// this is a path for a specific snapshot (e.g., /foo/.snapshot/s1)
						return false;
					}
					string nonSnapshotPath = Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.ConvertSnapshotPath
						(pathComponents);
					return this.dfs.GetFileInfo(nonSnapshotPath) != null;
				}
				else
				{
					return false;
				}
			}

			/// <returns>
			/// whether there is still remaining migration work for the next
			/// round
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			private bool ProcessNamespace()
			{
				this.GetSnapshottableDirs();
				bool hasRemaining = false;
				foreach (Path target in this._enclosing.targetPaths)
				{
					hasRemaining |= this.ProcessPath(target.ToUri().GetPath());
				}
				// wait for pending move to finish and retry the failed migration
				bool hasFailed = Dispatcher.WaitForMoveCompletion(this._enclosing.storages.targets
					.Values());
				if (hasFailed)
				{
					if (this._enclosing.retryCount.Get() == this._enclosing.retryMaxAttempts)
					{
						throw new IOException("Failed to move some block's after " + this._enclosing.retryMaxAttempts
							 + " retries.");
					}
					else
					{
						this._enclosing.retryCount.IncrementAndGet();
					}
				}
				else
				{
					// Reset retry count if no failure.
					this._enclosing.retryCount.Set(0);
				}
				hasRemaining |= hasFailed;
				return hasRemaining;
			}

			/// <returns>
			/// whether there is still remaing migration work for the next
			/// round
			/// </returns>
			private bool ProcessPath(string fullPath)
			{
				bool hasRemaining = false;
				for (byte[] lastReturnedName = HdfsFileStatus.EmptyName; ; )
				{
					DirectoryListing children;
					try
					{
						children = this.dfs.ListPaths(fullPath, lastReturnedName, true);
					}
					catch (IOException e)
					{
						Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Log.Warn("Failed to list directory " + 
							fullPath + ". Ignore the directory and continue.", e);
						return hasRemaining;
					}
					if (children == null)
					{
						return hasRemaining;
					}
					foreach (HdfsFileStatus child in children.GetPartialListing())
					{
						hasRemaining |= this.ProcessRecursively(fullPath, child);
					}
					if (children.HasMore())
					{
						lastReturnedName = children.GetLastName();
					}
					else
					{
						return hasRemaining;
					}
				}
			}

			/// <returns>whether the migration requires next round</returns>
			private bool ProcessRecursively(string parent, HdfsFileStatus status)
			{
				string fullPath = status.GetFullName(parent);
				bool hasRemaining = false;
				if (status.IsDir())
				{
					if (!fullPath.EndsWith(Path.Separator))
					{
						fullPath = fullPath + Path.Separator;
					}
					hasRemaining = this.ProcessPath(fullPath);
					// process snapshots if this is a snapshottable directory
					if (this.snapshottableDirs.Contains(fullPath))
					{
						string dirSnapshot = fullPath + HdfsConstants.DotSnapshotDir;
						hasRemaining |= this.ProcessPath(dirSnapshot);
					}
				}
				else
				{
					if (!status.IsSymlink())
					{
						// file
						try
						{
							if (!this.IsSnapshotPathInCurrent(fullPath))
							{
								// the full path is a snapshot path but it is also included in the
								// current directory tree, thus ignore it.
								hasRemaining = this.ProcessFile(fullPath, (HdfsLocatedFileStatus)status);
							}
						}
						catch (IOException e)
						{
							Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Log.Warn("Failed to check the status of "
								 + parent + ". Ignore it and continue.", e);
							return false;
						}
					}
				}
				return hasRemaining;
			}

			/// <returns>true if it is necessary to run another round of migration</returns>
			private bool ProcessFile(string fullPath, HdfsLocatedFileStatus status)
			{
				byte policyId = status.GetStoragePolicy();
				// currently we ignore files with unspecified storage policy
				if (policyId == BlockStoragePolicySuite.IdUnspecified)
				{
					return false;
				}
				BlockStoragePolicy policy = this._enclosing.blockStoragePolicies[policyId];
				if (policy == null)
				{
					Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Log.Warn("Failed to get the storage policy of file "
						 + fullPath);
					return false;
				}
				IList<StorageType> types = policy.ChooseStorageTypes(status.GetReplication());
				LocatedBlocks locatedBlocks = status.GetBlockLocations();
				bool hasRemaining = false;
				bool lastBlkComplete = locatedBlocks.IsLastBlockComplete();
				IList<LocatedBlock> lbs = locatedBlocks.GetLocatedBlocks();
				for (int i = 0; i < lbs.Count; i++)
				{
					if (i == lbs.Count - 1 && !lastBlkComplete)
					{
						// last block is incomplete, skip it
						continue;
					}
					LocatedBlock lb = lbs[i];
					Mover.StorageTypeDiff diff = new Mover.StorageTypeDiff(types, lb.GetStorageTypes(
						));
					if (!diff.RemoveOverlap(true))
					{
						if (this.ScheduleMoves4Block(diff, lb))
						{
							hasRemaining |= (diff.existing.Count > 1 && diff.expected.Count > 1);
						}
					}
				}
				return hasRemaining;
			}

			internal virtual bool ScheduleMoves4Block(Mover.StorageTypeDiff diff, LocatedBlock
				 lb)
			{
				IList<Mover.MLocation> locations = Mover.MLocation.ToLocations(lb);
				Sharpen.Collections.Shuffle(locations);
				Dispatcher.DBlock db = this._enclosing.NewDBlock(lb.GetBlock().GetLocalBlock(), locations
					);
				foreach (StorageType t in diff.existing)
				{
					foreach (Mover.MLocation ml in locations)
					{
						Dispatcher.Source source = this._enclosing.storages.GetSource(ml);
						if (ml.storageType == t && source != null)
						{
							// try to schedule one replica move.
							if (this.ScheduleMoveReplica(db, source, diff.expected))
							{
								return true;
							}
						}
					}
				}
				return false;
			}

			[VisibleForTesting]
			internal virtual bool ScheduleMoveReplica(Dispatcher.DBlock db, Mover.MLocation ml
				, IList<StorageType> targetTypes)
			{
				Dispatcher.Source source = this._enclosing.storages.GetSource(ml);
				return source == null ? false : this.ScheduleMoveReplica(db, source, targetTypes);
			}

			internal virtual bool ScheduleMoveReplica(Dispatcher.DBlock db, Dispatcher.Source
				 source, IList<StorageType> targetTypes)
			{
				// Match storage on the same node
				if (this.ChooseTargetInSameNode(db, source, targetTypes))
				{
					return true;
				}
				if (this._enclosing.dispatcher.GetCluster().IsNodeGroupAware())
				{
					if (this.ChooseTarget(db, source, targetTypes, Matcher.SameNodeGroup))
					{
						return true;
					}
				}
				// Then, match nodes on the same rack
				if (this.ChooseTarget(db, source, targetTypes, Matcher.SameRack))
				{
					return true;
				}
				// At last, match all remaining nodes
				return this.ChooseTarget(db, source, targetTypes, Matcher.AnyOther);
			}

			/// <summary>Choose the target storage within same Datanode if possible.</summary>
			internal virtual bool ChooseTargetInSameNode(Dispatcher.DBlock db, Dispatcher.Source
				 source, IList<StorageType> targetTypes)
			{
				foreach (StorageType t in targetTypes)
				{
					Dispatcher.DDatanode.StorageGroup target = this._enclosing.storages.GetTarget(source
						.GetDatanodeInfo().GetDatanodeUuid(), t);
					if (target == null)
					{
						continue;
					}
					Dispatcher.PendingMove pm = source.AddPendingMove(db, target);
					if (pm != null)
					{
						this._enclosing.dispatcher.ExecutePendingMove(pm);
						return true;
					}
				}
				return false;
			}

			internal virtual bool ChooseTarget(Dispatcher.DBlock db, Dispatcher.Source source
				, IList<StorageType> targetTypes, Matcher matcher)
			{
				NetworkTopology cluster = this._enclosing.dispatcher.GetCluster();
				foreach (StorageType t in targetTypes)
				{
					foreach (Dispatcher.DDatanode.StorageGroup target in this._enclosing.storages.GetTargetStorages
						(t))
					{
						if (matcher.Match(cluster, source.GetDatanodeInfo(), target.GetDatanodeInfo()))
						{
							Dispatcher.PendingMove pm = source.AddPendingMove(db, target);
							if (pm != null)
							{
								this._enclosing.dispatcher.ExecutePendingMove(pm);
								return true;
							}
						}
					}
				}
				return false;
			}

			private readonly Mover _enclosing;
		}

		internal class MLocation
		{
			internal readonly DatanodeInfo datanode;

			internal readonly StorageType storageType;

			internal readonly long size;

			internal MLocation(DatanodeInfo datanode, StorageType storageType, long size)
			{
				this.datanode = datanode;
				this.storageType = storageType;
				this.size = size;
			}

			internal static IList<Mover.MLocation> ToLocations(LocatedBlock lb)
			{
				DatanodeInfo[] datanodeInfos = lb.GetLocations();
				StorageType[] storageTypes = lb.GetStorageTypes();
				long size = lb.GetBlockSize();
				IList<Mover.MLocation> locations = new List<Mover.MLocation>();
				for (int i = 0; i < datanodeInfos.Length; i++)
				{
					locations.AddItem(new Mover.MLocation(datanodeInfos[i], storageTypes[i], size));
				}
				return locations;
			}
		}

		internal class StorageTypeDiff
		{
			internal readonly IList<StorageType> expected;

			internal readonly IList<StorageType> existing;

			internal StorageTypeDiff(IList<StorageType> expected, StorageType[] existing)
			{
				this.expected = new List<StorageType>(expected);
				this.existing = new List<StorageType>(Arrays.AsList(existing));
			}

			/// <summary>Remove the overlap between the expected types and the existing types.</summary>
			/// <param name="ignoreNonMovable">
			/// ignore non-movable storage types
			/// by removing them from both expected and existing storage type list
			/// to prevent non-movable storage from being moved.
			/// </param>
			/// <returns>
			/// if the existing types or the expected types is empty after
			/// removing the overlap.
			/// </returns>
			internal virtual bool RemoveOverlap(bool ignoreNonMovable)
			{
				for (IEnumerator<StorageType> i = existing.GetEnumerator(); i.HasNext(); )
				{
					StorageType t = i.Next();
					if (expected.Remove(t))
					{
						i.Remove();
					}
				}
				if (ignoreNonMovable)
				{
					RemoveNonMovable(existing);
					RemoveNonMovable(expected);
				}
				return expected.IsEmpty() || existing.IsEmpty();
			}

			internal virtual void RemoveNonMovable(IList<StorageType> types)
			{
				for (IEnumerator<StorageType> i = types.GetEnumerator(); i.HasNext(); )
				{
					StorageType t = i.Next();
					if (!t.IsMovable())
					{
						i.Remove();
					}
				}
			}

			public override string ToString()
			{
				return GetType().Name + "{expected=" + expected + ", existing=" + existing + "}";
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static int Run(IDictionary<URI, IList<Path>> namenodes, Configuration conf
			)
		{
			long sleeptime = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DFSConfigKeys
				.DfsHeartbeatIntervalDefault) * 2000 + conf.GetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey
				, DFSConfigKeys.DfsNamenodeReplicationIntervalDefault) * 1000;
			AtomicInteger retryCount = new AtomicInteger(0);
			Log.Info("namenodes = " + namenodes);
			IList<NameNodeConnector> connectors = Sharpen.Collections.EmptyList();
			try
			{
				connectors = NameNodeConnector.NewNameNodeConnectors(namenodes, typeof(Org.Apache.Hadoop.Hdfs.Server.Mover.Mover
					).Name, MoverIdPath, conf, NameNodeConnector.DefaultMaxIdleIterations);
				while (connectors.Count > 0)
				{
					Sharpen.Collections.Shuffle(connectors);
					IEnumerator<NameNodeConnector> iter = connectors.GetEnumerator();
					while (iter.HasNext())
					{
						NameNodeConnector nnc = iter.Next();
						Org.Apache.Hadoop.Hdfs.Server.Mover.Mover m = new Org.Apache.Hadoop.Hdfs.Server.Mover.Mover
							(nnc, conf, retryCount);
						ExitStatus r = m.Run();
						if (r == ExitStatus.Success)
						{
							IOUtils.Cleanup(Log, nnc);
							iter.Remove();
						}
						else
						{
							if (r != ExitStatus.InProgress)
							{
								// must be an error statue, return
								return r.GetExitCode();
							}
						}
					}
					Sharpen.Thread.Sleep(sleeptime);
				}
				return ExitStatus.Success.GetExitCode();
			}
			finally
			{
				foreach (NameNodeConnector nnc in connectors)
				{
					IOUtils.Cleanup(Log, nnc);
				}
			}
		}

		internal class Cli : Configured, Tool
		{
			private const string Usage = "Usage: hdfs mover " + "[-p <files/dirs> | -f <local file>]"
				 + "\n\t-p <files/dirs>\ta space separated list of HDFS files/dirs to migrate." 
				+ "\n\t-f <local file>\ta local file containing a list of HDFS files/dirs to migrate.";

			private static Options BuildCliOptions()
			{
				Options opts = new Options();
				Option file = OptionBuilder.Create("f");
				Option paths = OptionBuilder.Create("p");
				OptionGroup group = new OptionGroup();
				group.AddOption(file);
				group.AddOption(paths);
				opts.AddOptionGroup(group);
				return opts;
			}

			/// <exception cref="System.IO.IOException"/>
			private static string[] ReadPathFile(string file)
			{
				IList<string> list = Lists.NewArrayList();
				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream
					(file), "UTF-8"));
				try
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						if (!line.Trim().IsEmpty())
						{
							list.AddItem(line);
						}
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, reader);
				}
				return Sharpen.Collections.ToArray(list, new string[list.Count]);
			}

			/// <exception cref="System.Exception"/>
			private static IDictionary<URI, IList<Path>> GetNameNodePaths(CommandLine line, Configuration
				 conf)
			{
				IDictionary<URI, IList<Path>> map = Maps.NewHashMap();
				string[] paths = null;
				if (line.HasOption("f"))
				{
					paths = ReadPathFile(line.GetOptionValue("f"));
				}
				else
				{
					if (line.HasOption("p"))
					{
						paths = line.GetOptionValues("p");
					}
				}
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				if (paths == null || paths.Length == 0)
				{
					foreach (URI namenode in namenodes)
					{
						map[namenode] = null;
					}
					return map;
				}
				URI singleNs = namenodes.Count == 1 ? namenodes.GetEnumerator().Next() : null;
				foreach (string path in paths)
				{
					Path target = new Path(path);
					if (!target.IsUriPathAbsolute())
					{
						throw new ArgumentException("The path " + target + " is not absolute");
					}
					URI targetUri = target.ToUri();
					if ((targetUri.GetAuthority() == null || targetUri.GetScheme() == null) && singleNs
						 == null)
					{
						// each path must contains both scheme and authority information
						// unless there is only one name service specified in the
						// configuration
						throw new ArgumentException("The path " + target + " does not contain scheme and authority thus cannot identify"
							 + " its name service");
					}
					URI key = singleNs;
					if (singleNs == null)
					{
						key = new URI(targetUri.GetScheme(), targetUri.GetAuthority(), null, null, null);
						if (!namenodes.Contains(key))
						{
							throw new ArgumentException("Cannot resolve the path " + target + ". The namenode services specified in the "
								 + "configuration: " + namenodes);
						}
					}
					IList<Path> targets = map[key];
					if (targets == null)
					{
						targets = Lists.NewArrayList();
						map[key] = targets;
					}
					targets.AddItem(Path.GetPathWithoutSchemeAndAuthority(target));
				}
				return map;
			}

			/// <exception cref="System.Exception"/>
			[VisibleForTesting]
			internal static IDictionary<URI, IList<Path>> GetNameNodePathsToMove(Configuration
				 conf, params string[] args)
			{
				Options opts = BuildCliOptions();
				CommandLineParser parser = new GnuParser();
				CommandLine commandLine = parser.Parse(opts, args, true);
				return GetNameNodePaths(commandLine, conf);
			}

			/// <exception cref="System.Exception"/>
			public virtual int Run(string[] args)
			{
				long startTime = Time.MonotonicNow();
				Configuration conf = GetConf();
				try
				{
					IDictionary<URI, IList<Path>> map = GetNameNodePathsToMove(conf, args);
					return Org.Apache.Hadoop.Hdfs.Server.Mover.Mover.Run(map, conf);
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
				catch (ParseException e)
				{
					System.Console.Out.WriteLine(e + ".  Exiting ...");
					return ExitStatus.IllegalArguments.GetExitCode();
				}
				catch (ArgumentException e)
				{
					System.Console.Out.WriteLine(e + ".  Exiting ...");
					return ExitStatus.IllegalArguments.GetExitCode();
				}
				finally
				{
					System.Console.Out.Format("%-24s ", DateFormat.GetDateTimeInstance().Format(new DateTime
						()));
					System.Console.Out.WriteLine("Mover took " + StringUtils.FormatTime(Time.MonotonicNow
						() - startTime));
				}
			}
		}

		/// <summary>Run a Mover in command line.</summary>
		/// <param name="args">Command line arguments</param>
		public static void Main(string[] args)
		{
			if (DFSUtil.ParseHelpArgument(args, Mover.Cli.Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			try
			{
				System.Environment.Exit(ToolRunner.Run(new HdfsConfiguration(), new Mover.Cli(), 
					args));
			}
			catch (Exception e)
			{
				Log.Error("Exiting " + typeof(Org.Apache.Hadoop.Hdfs.Server.Mover.Mover).Name + " due to an exception"
					, e);
				System.Environment.Exit(-1);
			}
		}
	}
}
