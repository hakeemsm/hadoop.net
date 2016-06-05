using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The Cache Manager handles caching on DataNodes.</summary>
	/// <remarks>
	/// The Cache Manager handles caching on DataNodes.
	/// This class is instantiated by the FSNamesystem.
	/// It maintains the mapping of cached blocks to datanodes via processing
	/// datanode cache reports. Based on these reports and addition and removal of
	/// caching directives, we will schedule caching and uncaching work.
	/// </remarks>
	public sealed class CacheManager
	{
		public static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.CacheManager
			));

		private const float MinCachedBlocksPercent = 0.001f;

		/// <summary>The FSNamesystem that contains this CacheManager.</summary>
		private readonly FSNamesystem namesystem;

		/// <summary>The BlockManager associated with the FSN that owns this CacheManager.</summary>
		private readonly BlockManager blockManager;

		/// <summary>Cache directives, sorted by ID.</summary>
		/// <remarks>
		/// Cache directives, sorted by ID.
		/// listCacheDirectives relies on the ordering of elements in this map
		/// to track what has already been listed by the client.
		/// </remarks>
		private readonly SortedDictionary<long, CacheDirective> directivesById = new SortedDictionary
			<long, CacheDirective>();

		/// <summary>The directive ID to use for a new directive.</summary>
		/// <remarks>
		/// The directive ID to use for a new directive.  IDs always increase, and are
		/// never reused.
		/// </remarks>
		private long nextDirectiveId;

		/// <summary>Cache directives, sorted by path</summary>
		private readonly SortedDictionary<string, IList<CacheDirective>> directivesByPath
			 = new SortedDictionary<string, IList<CacheDirective>>();

		/// <summary>Cache pools, sorted by name.</summary>
		private readonly SortedDictionary<string, CachePool> cachePools = new SortedDictionary
			<string, CachePool>();

		/// <summary>Maximum number of cache pools to list in one operation.</summary>
		private readonly int maxListCachePoolsResponses;

		/// <summary>Maximum number of cache pool directives to list in one operation.</summary>
		private readonly int maxListCacheDirectivesNumResponses;

		/// <summary>Interval between scans in milliseconds.</summary>
		private readonly long scanIntervalMs;

		/// <summary>All cached blocks.</summary>
		private readonly GSet<CachedBlock, CachedBlock> cachedBlocks;

		/// <summary>Lock which protects the CacheReplicationMonitor.</summary>
		private readonly ReentrantLock crmLock = new ReentrantLock();

		private readonly CacheManager.SerializerCompat serializerCompat;

		/// <summary>The CacheReplicationMonitor.</summary>
		private CacheReplicationMonitor monitor;

		public sealed class PersistState
		{
			public readonly FsImageProto.CacheManagerSection section;

			public readonly IList<ClientNamenodeProtocolProtos.CachePoolInfoProto> pools;

			public readonly IList<ClientNamenodeProtocolProtos.CacheDirectiveInfoProto> directives;

			public PersistState(FsImageProto.CacheManagerSection section, IList<ClientNamenodeProtocolProtos.CachePoolInfoProto
				> pools, IList<ClientNamenodeProtocolProtos.CacheDirectiveInfoProto> directives)
			{
				// TODO: add pending / underCached / schedule cached blocks stats.
				this.section = section;
				this.pools = pools;
				this.directives = directives;
			}
		}

		internal CacheManager(FSNamesystem namesystem, Configuration conf, BlockManager blockManager
			)
		{
			serializerCompat = new CacheManager.SerializerCompat(this);
			this.namesystem = namesystem;
			this.blockManager = blockManager;
			this.nextDirectiveId = 1;
			this.maxListCachePoolsResponses = conf.GetInt(DFSConfigKeys.DfsNamenodeListCachePoolsNumResponses
				, DFSConfigKeys.DfsNamenodeListCachePoolsNumResponsesDefault);
			this.maxListCacheDirectivesNumResponses = conf.GetInt(DFSConfigKeys.DfsNamenodeListCacheDirectivesNumResponses
				, DFSConfigKeys.DfsNamenodeListCacheDirectivesNumResponsesDefault);
			scanIntervalMs = conf.GetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs
				, DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMsDefault);
			float cachedBlocksPercent = conf.GetFloat(DFSConfigKeys.DfsNamenodePathBasedCacheBlockMapAllocationPercent
				, DFSConfigKeys.DfsNamenodePathBasedCacheBlockMapAllocationPercentDefault);
			if (cachedBlocksPercent < MinCachedBlocksPercent)
			{
				Log.Info("Using minimum value {} for {}", MinCachedBlocksPercent, DFSConfigKeys.DfsNamenodePathBasedCacheBlockMapAllocationPercent
					);
				cachedBlocksPercent = MinCachedBlocksPercent;
			}
			this.cachedBlocks = new LightWeightGSet<CachedBlock, CachedBlock>(LightWeightGSet
				.ComputeCapacity(cachedBlocksPercent, "cachedBlocks"));
		}

		/// <summary>Resets all tracked directives and pools.</summary>
		/// <remarks>
		/// Resets all tracked directives and pools. Called during 2NN checkpointing to
		/// reset FSNamesystem state. See
		/// <see cref="FSNamesystem.Clear()"/>
		/// .
		/// </remarks>
		internal void Clear()
		{
			directivesById.Clear();
			directivesByPath.Clear();
			cachePools.Clear();
			nextDirectiveId = 1;
		}

		public void StartMonitorThread()
		{
			crmLock.Lock();
			try
			{
				if (this.monitor == null)
				{
					this.monitor = new CacheReplicationMonitor(namesystem, this, scanIntervalMs, crmLock
						);
					this.monitor.Start();
				}
			}
			finally
			{
				crmLock.Unlock();
			}
		}

		public void StopMonitorThread()
		{
			crmLock.Lock();
			try
			{
				if (this.monitor != null)
				{
					CacheReplicationMonitor prevMonitor = this.monitor;
					this.monitor = null;
					IOUtils.CloseQuietly(prevMonitor);
				}
			}
			finally
			{
				crmLock.Unlock();
			}
		}

		public void ClearDirectiveStats()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			foreach (CacheDirective directive in directivesById.Values)
			{
				directive.ResetStatistics();
			}
		}

		/// <returns>Unmodifiable view of the collection of CachePools.</returns>
		public ICollection<CachePool> GetCachePools()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			return Sharpen.Collections.UnmodifiableCollection(cachePools.Values);
		}

		/// <returns>Unmodifiable view of the collection of CacheDirectives.</returns>
		public ICollection<CacheDirective> GetCacheDirectives()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			return Sharpen.Collections.UnmodifiableCollection(directivesById.Values);
		}

		[VisibleForTesting]
		public GSet<CachedBlock, CachedBlock> GetCachedBlocks()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			return cachedBlocks;
		}

		/// <exception cref="System.IO.IOException"/>
		private long GetNextDirectiveId()
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			if (nextDirectiveId >= long.MaxValue - 1)
			{
				throw new IOException("No more available IDs.");
			}
			return nextDirectiveId++;
		}

		// Helper getter / validation methods
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private static void CheckWritePermission(FSPermissionChecker pc, CachePool pool)
		{
			if ((pc != null))
			{
				pc.CheckPermission(pool, FsAction.Write);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private static string ValidatePoolName(CacheDirectiveInfo directive)
		{
			string pool = directive.GetPool();
			if (pool == null)
			{
				throw new InvalidRequestException("No pool specified.");
			}
			if (pool.IsEmpty())
			{
				throw new InvalidRequestException("Invalid empty pool name.");
			}
			return pool;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private static string ValidatePath(CacheDirectiveInfo directive)
		{
			if (directive.GetPath() == null)
			{
				throw new InvalidRequestException("No path specified.");
			}
			string path = directive.GetPath().ToUri().GetPath();
			if (!DFSUtil.IsValidName(path))
			{
				throw new InvalidRequestException("Invalid path '" + path + "'.");
			}
			return path;
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private static short ValidateReplication(CacheDirectiveInfo directive, short defaultValue
			)
		{
			short repl = (directive.GetReplication() != null) ? directive.GetReplication() : 
				defaultValue;
			if (repl <= 0)
			{
				throw new InvalidRequestException("Invalid replication factor " + repl + " <= 0");
			}
			return repl;
		}

		/// <summary>
		/// Calculates the absolute expiry time of the directive from the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.CacheDirectiveInfo.Expiration"/>
		/// . This converts a relative Expiration
		/// into an absolute time based on the local clock.
		/// </summary>
		/// <param name="info">to validate.</param>
		/// <param name="maxRelativeExpiryTime">of the info's pool.</param>
		/// <returns>
		/// the expiration time, or the pool's max absolute expiration if the
		/// info's expiration was not set.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException">if the info's Expiration is invalid.
		/// 	</exception>
		private static long ValidateExpiryTime(CacheDirectiveInfo info, long maxRelativeExpiryTime
			)
		{
			Log.Trace("Validating directive {} pool maxRelativeExpiryTime {}", info, maxRelativeExpiryTime
				);
			long now = new DateTime().GetTime();
			long maxAbsoluteExpiryTime = now + maxRelativeExpiryTime;
			if (info == null || info.GetExpiration() == null)
			{
				return maxAbsoluteExpiryTime;
			}
			CacheDirectiveInfo.Expiration expiry = info.GetExpiration();
			if (expiry.GetMillis() < 0l)
			{
				throw new InvalidRequestException("Cannot set a negative expiration: " + expiry.GetMillis
					());
			}
			long relExpiryTime;
			long absExpiryTime;
			if (expiry.IsRelative())
			{
				relExpiryTime = expiry.GetMillis();
				absExpiryTime = now + relExpiryTime;
			}
			else
			{
				absExpiryTime = expiry.GetMillis();
				relExpiryTime = absExpiryTime - now;
			}
			// Need to cap the expiry so we don't overflow a long when doing math
			if (relExpiryTime > CacheDirectiveInfo.Expiration.MaxRelativeExpiryMs)
			{
				throw new InvalidRequestException("Expiration " + expiry.ToString() + " is too far in the future!"
					);
			}
			// Fail if the requested expiry is greater than the max
			if (relExpiryTime > maxRelativeExpiryTime)
			{
				throw new InvalidRequestException("Expiration " + expiry.ToString() + " exceeds the max relative expiration time of "
					 + maxRelativeExpiryTime + " ms.");
			}
			return absExpiryTime;
		}

		/// <summary>
		/// Throws an exception if the CachePool does not have enough capacity to
		/// cache the given path at the replication factor.
		/// </summary>
		/// <param name="pool">CachePool where the path is being cached</param>
		/// <param name="path">Path that is being cached</param>
		/// <param name="replication">Replication factor of the path</param>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException">if the pool does not have enough capacity
		/// 	</exception>
		private void CheckLimit(CachePool pool, string path, short replication)
		{
			CacheDirectiveStats stats = ComputeNeeded(path, replication);
			if (pool.GetLimit() == CachePoolInfo.LimitUnlimited)
			{
				return;
			}
			if (pool.GetBytesNeeded() + (stats.GetBytesNeeded() * replication) > pool.GetLimit
				())
			{
				throw new InvalidRequestException("Caching path " + path + " of size " + stats.GetBytesNeeded
					() / replication + " bytes at replication " + replication + " would exceed pool "
					 + pool.GetPoolName() + "'s remaining capacity of " + (pool.GetLimit() - pool.GetBytesNeeded
					()) + " bytes.");
			}
		}

		/// <summary>Computes the needed number of bytes and files for a path.</summary>
		/// <returns>CacheDirectiveStats describing the needed stats for this path</returns>
		private CacheDirectiveStats ComputeNeeded(string path, short replication)
		{
			FSDirectory fsDir = namesystem.GetFSDirectory();
			INode node;
			long requestedBytes = 0;
			long requestedFiles = 0;
			CacheDirectiveStats.Builder builder = new CacheDirectiveStats.Builder();
			try
			{
				node = fsDir.GetINode(path);
			}
			catch (UnresolvedLinkException)
			{
				// We don't cache through symlinks
				return builder.Build();
			}
			if (node == null)
			{
				return builder.Build();
			}
			if (node.IsFile())
			{
				requestedFiles = 1;
				INodeFile file = node.AsFile();
				requestedBytes = file.ComputeFileSize();
			}
			else
			{
				if (node.IsDirectory())
				{
					INodeDirectory dir = node.AsDirectory();
					ReadOnlyList<INode> children = dir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
					requestedFiles = children.Size();
					foreach (INode child in children)
					{
						if (child.IsFile())
						{
							requestedBytes += child.AsFile().ComputeFileSize();
						}
					}
				}
			}
			return new CacheDirectiveStats.Builder().SetBytesNeeded(requestedBytes).SetFilesCached
				(requestedFiles).Build();
		}

		/// <summary>
		/// Get a CacheDirective by ID, validating the ID and that the directive
		/// exists.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private CacheDirective GetById(long id)
		{
			// Check for invalid IDs.
			if (id <= 0)
			{
				throw new InvalidRequestException("Invalid negative ID.");
			}
			// Find the directive.
			CacheDirective directive = directivesById[id];
			if (directive == null)
			{
				throw new InvalidRequestException("No directive with ID " + id + " found.");
			}
			return directive;
		}

		/// <summary>Get a CachePool by name, validating that it exists.</summary>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private CachePool GetCachePool(string poolName)
		{
			CachePool pool = cachePools[poolName];
			if (pool == null)
			{
				throw new InvalidRequestException("Unknown pool " + poolName);
			}
			return pool;
		}

		// RPC handlers
		private void AddInternal(CacheDirective directive, CachePool pool)
		{
			bool addedDirective = pool.GetDirectiveList().AddItem(directive);
			System.Diagnostics.Debug.Assert(addedDirective);
			directivesById[directive.GetId()] = directive;
			string path = directive.GetPath();
			IList<CacheDirective> directives = directivesByPath[path];
			if (directives == null)
			{
				directives = new AList<CacheDirective>(1);
				directivesByPath[path] = directives;
			}
			directives.AddItem(directive);
			// Fix up pool stats
			CacheDirectiveStats stats = ComputeNeeded(directive.GetPath(), directive.GetReplication
				());
			directive.AddBytesNeeded(stats.GetBytesNeeded());
			directive.AddFilesNeeded(directive.GetFilesNeeded());
			SetNeedsRescan();
		}

		/// <summary>Adds a directive, skipping most error checking.</summary>
		/// <remarks>
		/// Adds a directive, skipping most error checking. This should only be called
		/// internally in special scenarios like edit log replay.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		internal CacheDirectiveInfo AddDirectiveFromEditLog(CacheDirectiveInfo directive)
		{
			long id = directive.GetId();
			CacheDirective entry = new CacheDirective(directive);
			CachePool pool = cachePools[directive.GetPool()];
			AddInternal(entry, pool);
			if (nextDirectiveId <= id)
			{
				nextDirectiveId = id + 1;
			}
			return entry.ToInfo();
		}

		/// <exception cref="System.IO.IOException"/>
		public CacheDirectiveInfo AddDirective(CacheDirectiveInfo info, FSPermissionChecker
			 pc, EnumSet<CacheFlag> flags)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			CacheDirective directive;
			try
			{
				CachePool pool = GetCachePool(ValidatePoolName(info));
				CheckWritePermission(pc, pool);
				string path = ValidatePath(info);
				short replication = ValidateReplication(info, (short)1);
				long expiryTime = ValidateExpiryTime(info, pool.GetMaxRelativeExpiryMs());
				// Do quota validation if required
				if (!flags.Contains(CacheFlag.Force))
				{
					CheckLimit(pool, path, replication);
				}
				// All validation passed
				// Add a new entry with the next available ID.
				long id = GetNextDirectiveId();
				directive = new CacheDirective(id, path, replication, expiryTime);
				AddInternal(directive, pool);
			}
			catch (IOException e)
			{
				Log.Warn("addDirective of " + info + " failed: ", e);
				throw;
			}
			Log.Info("addDirective of {} successful.", info);
			return directive.ToInfo();
		}

		/// <summary>
		/// Factory method that makes a new CacheDirectiveInfo by applying fields in a
		/// CacheDirectiveInfo to an existing CacheDirective.
		/// </summary>
		/// <param name="info">with some or all fields set.</param>
		/// <param name="defaults">
		/// directive providing default values for unset fields in
		/// info.
		/// </param>
		/// <returns>new CacheDirectiveInfo of the info applied to the defaults.</returns>
		private static CacheDirectiveInfo CreateFromInfoAndDefaults(CacheDirectiveInfo info
			, CacheDirective defaults)
		{
			// Initialize the builder with the default values
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(defaults.ToInfo
				());
			// Replace default with new value if present
			if (info.GetPath() != null)
			{
				builder.SetPath(info.GetPath());
			}
			if (info.GetReplication() != null)
			{
				builder.SetReplication(info.GetReplication());
			}
			if (info.GetPool() != null)
			{
				builder.SetPool(info.GetPool());
			}
			if (info.GetExpiration() != null)
			{
				builder.SetExpiration(info.GetExpiration());
			}
			return builder.Build();
		}

		/// <summary>Modifies a directive, skipping most error checking.</summary>
		/// <remarks>
		/// Modifies a directive, skipping most error checking. This is for careful
		/// internal use only. modifyDirective can be non-deterministic since its error
		/// checking depends on current system time, which poses a problem for edit log
		/// replay.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		internal void ModifyDirectiveFromEditLog(CacheDirectiveInfo info)
		{
			// Check for invalid IDs.
			long id = info.GetId();
			if (id == null)
			{
				throw new InvalidRequestException("Must supply an ID.");
			}
			CacheDirective prevEntry = GetById(id);
			CacheDirectiveInfo newInfo = CreateFromInfoAndDefaults(info, prevEntry);
			RemoveInternal(prevEntry);
			AddInternal(new CacheDirective(newInfo), GetCachePool(newInfo.GetPool()));
		}

		/// <exception cref="System.IO.IOException"/>
		public void ModifyDirective(CacheDirectiveInfo info, FSPermissionChecker pc, EnumSet
			<CacheFlag> flags)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			string idString = (info.GetId() == null) ? "(null)" : info.GetId().ToString();
			try
			{
				// Check for invalid IDs.
				long id = info.GetId();
				if (id == null)
				{
					throw new InvalidRequestException("Must supply an ID.");
				}
				CacheDirective prevEntry = GetById(id);
				CheckWritePermission(pc, prevEntry.GetPool());
				// Fill in defaults
				CacheDirectiveInfo infoWithDefaults = CreateFromInfoAndDefaults(info, prevEntry);
				CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(infoWithDefaults
					);
				// Do validation
				ValidatePath(infoWithDefaults);
				ValidateReplication(infoWithDefaults, (short)-1);
				// Need to test the pool being set here to avoid rejecting a modify for a
				// directive that's already been forced into a pool
				CachePool srcPool = prevEntry.GetPool();
				CachePool destPool = GetCachePool(ValidatePoolName(infoWithDefaults));
				if (!srcPool.GetPoolName().Equals(destPool.GetPoolName()))
				{
					CheckWritePermission(pc, destPool);
					if (!flags.Contains(CacheFlag.Force))
					{
						CheckLimit(destPool, infoWithDefaults.GetPath().ToUri().GetPath(), infoWithDefaults
							.GetReplication());
					}
				}
				// Verify the expiration against the destination pool
				ValidateExpiryTime(infoWithDefaults, destPool.GetMaxRelativeExpiryMs());
				// Indicate changes to the CRM
				SetNeedsRescan();
				// Validation passed
				RemoveInternal(prevEntry);
				AddInternal(new CacheDirective(builder.Build()), destPool);
			}
			catch (IOException e)
			{
				Log.Warn("modifyDirective of " + idString + " failed: ", e);
				throw;
			}
			Log.Info("modifyDirective of {} successfully applied {}.", idString, info);
		}

		/// <exception cref="Org.Apache.Hadoop.FS.InvalidRequestException"/>
		private void RemoveInternal(CacheDirective directive)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			// Remove the corresponding entry in directivesByPath.
			string path = directive.GetPath();
			IList<CacheDirective> directives = directivesByPath[path];
			if (directives == null || !directives.Remove(directive))
			{
				throw new InvalidRequestException("Failed to locate entry " + directive.GetId() +
					 " by path " + directive.GetPath());
			}
			if (directives.Count == 0)
			{
				Sharpen.Collections.Remove(directivesByPath, path);
			}
			// Fix up the stats from removing the pool
			CachePool pool = directive.GetPool();
			directive.AddBytesNeeded(-directive.GetBytesNeeded());
			directive.AddFilesNeeded(-directive.GetFilesNeeded());
			Sharpen.Collections.Remove(directivesById, directive.GetId());
			pool.GetDirectiveList().Remove(directive);
			System.Diagnostics.Debug.Assert(directive.GetPool() == null);
			SetNeedsRescan();
		}

		/// <exception cref="System.IO.IOException"/>
		public void RemoveDirective(long id, FSPermissionChecker pc)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			try
			{
				CacheDirective directive = GetById(id);
				CheckWritePermission(pc, directive.GetPool());
				RemoveInternal(directive);
			}
			catch (IOException e)
			{
				Log.Warn("removeDirective of " + id + " failed: ", e);
				throw;
			}
			Log.Info("removeDirective of " + id + " successful.");
		}

		/// <exception cref="System.IO.IOException"/>
		public BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry> ListCacheDirectives
			(long prevId, CacheDirectiveInfo filter, FSPermissionChecker pc)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			int NumPreAllocatedEntries = 16;
			string filterPath = null;
			if (filter.GetPath() != null)
			{
				filterPath = ValidatePath(filter);
			}
			if (filter.GetReplication() != null)
			{
				throw new InvalidRequestException("Filtering by replication is unsupported.");
			}
			// Querying for a single ID
			long id = filter.GetId();
			if (id != null)
			{
				if (!directivesById.Contains(id))
				{
					throw new InvalidRequestException("Did not find requested id " + id);
				}
				// Since we use a tailMap on directivesById, setting prev to id-1 gets
				// us the directive with the id (if present)
				prevId = id - 1;
			}
			AList<CacheDirectiveEntry> replies = new AList<CacheDirectiveEntry>(NumPreAllocatedEntries
				);
			int numReplies = 0;
			SortedDictionary<long, CacheDirective> tailMap = directivesById.TailMap(prevId + 
				1);
			foreach (KeyValuePair<long, CacheDirective> cur in tailMap)
			{
				if (numReplies >= maxListCacheDirectivesNumResponses)
				{
					return new BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry>(replies, 
						true);
				}
				CacheDirective curDirective = cur.Value;
				CacheDirectiveInfo info = cur.Value.ToInfo();
				// If the requested ID is present, it should be the first item.
				// Hitting this case means the ID is not present, or we're on the second
				// item and should break out.
				if (id != null && !(info.GetId().Equals(id)))
				{
					break;
				}
				if (filter.GetPool() != null && !info.GetPool().Equals(filter.GetPool()))
				{
					continue;
				}
				if (filterPath != null && !info.GetPath().ToUri().GetPath().Equals(filterPath))
				{
					continue;
				}
				bool hasPermission = true;
				if (pc != null)
				{
					try
					{
						pc.CheckPermission(curDirective.GetPool(), FsAction.Read);
					}
					catch (AccessControlException)
					{
						hasPermission = false;
					}
				}
				if (hasPermission)
				{
					replies.AddItem(new CacheDirectiveEntry(info, cur.Value.ToStats()));
					numReplies++;
				}
			}
			return new BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry>(replies, 
				false);
		}

		/// <summary>Create a cache pool.</summary>
		/// <remarks>
		/// Create a cache pool.
		/// Only the superuser should be able to call this function.
		/// </remarks>
		/// <param name="info">The info for the cache pool to create.</param>
		/// <returns>Information about the cache pool we created.</returns>
		/// <exception cref="System.IO.IOException"/>
		public CachePoolInfo AddCachePool(CachePoolInfo info)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			CachePool pool;
			try
			{
				CachePoolInfo.Validate(info);
				string poolName = info.GetPoolName();
				pool = cachePools[poolName];
				if (pool != null)
				{
					throw new InvalidRequestException("Cache pool " + poolName + " already exists.");
				}
				pool = CachePool.CreateFromInfoAndDefaults(info);
				cachePools[pool.GetPoolName()] = pool;
			}
			catch (IOException e)
			{
				Log.Info("addCachePool of " + info + " failed: ", e);
				throw;
			}
			Log.Info("addCachePool of {} successful.", info);
			return pool.GetInfo(true);
		}

		/// <summary>Modify a cache pool.</summary>
		/// <remarks>
		/// Modify a cache pool.
		/// Only the superuser should be able to call this function.
		/// </remarks>
		/// <param name="info">The info for the cache pool to modify.</param>
		/// <exception cref="System.IO.IOException"/>
		public void ModifyCachePool(CachePoolInfo info)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			StringBuilder bld = new StringBuilder();
			try
			{
				CachePoolInfo.Validate(info);
				string poolName = info.GetPoolName();
				CachePool pool = cachePools[poolName];
				if (pool == null)
				{
					throw new InvalidRequestException("Cache pool " + poolName + " does not exist.");
				}
				string prefix = string.Empty;
				if (info.GetOwnerName() != null)
				{
					pool.SetOwnerName(info.GetOwnerName());
					bld.Append(prefix).Append("set owner to ").Append(info.GetOwnerName());
					prefix = "; ";
				}
				if (info.GetGroupName() != null)
				{
					pool.SetGroupName(info.GetGroupName());
					bld.Append(prefix).Append("set group to ").Append(info.GetGroupName());
					prefix = "; ";
				}
				if (info.GetMode() != null)
				{
					pool.SetMode(info.GetMode());
					bld.Append(prefix).Append("set mode to " + info.GetMode());
					prefix = "; ";
				}
				if (info.GetLimit() != null)
				{
					pool.SetLimit(info.GetLimit());
					bld.Append(prefix).Append("set limit to " + info.GetLimit());
					prefix = "; ";
					// New limit changes stats, need to set needs refresh
					SetNeedsRescan();
				}
				if (info.GetMaxRelativeExpiryMs() != null)
				{
					long maxRelativeExpiry = info.GetMaxRelativeExpiryMs();
					pool.SetMaxRelativeExpiryMs(maxRelativeExpiry);
					bld.Append(prefix).Append("set maxRelativeExpiry to " + maxRelativeExpiry);
					prefix = "; ";
				}
				if (prefix.IsEmpty())
				{
					bld.Append("no changes.");
				}
			}
			catch (IOException e)
			{
				Log.Info("modifyCachePool of " + info + " failed: ", e);
				throw;
			}
			Log.Info("modifyCachePool of {} successful; {}", info.GetPoolName(), bld.ToString
				());
		}

		/// <summary>Remove a cache pool.</summary>
		/// <remarks>
		/// Remove a cache pool.
		/// Only the superuser should be able to call this function.
		/// </remarks>
		/// <param name="poolName">The name for the cache pool to remove.</param>
		/// <exception cref="System.IO.IOException"/>
		public void RemoveCachePool(string poolName)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasWriteLock());
			try
			{
				CachePoolInfo.ValidateName(poolName);
				CachePool pool = Sharpen.Collections.Remove(cachePools, poolName);
				if (pool == null)
				{
					throw new InvalidRequestException("Cannot remove non-existent cache pool " + poolName
						);
				}
				// Remove all directives in this pool.
				IEnumerator<CacheDirective> iter = pool.GetDirectiveList().GetEnumerator();
				while (iter.HasNext())
				{
					CacheDirective directive = iter.Next();
					Sharpen.Collections.Remove(directivesByPath, directive.GetPath());
					Sharpen.Collections.Remove(directivesById, directive.GetId());
					iter.Remove();
				}
				SetNeedsRescan();
			}
			catch (IOException e)
			{
				Log.Info("removeCachePool of " + poolName + " failed: ", e);
				throw;
			}
			Log.Info("removeCachePool of " + poolName + " successful.");
		}

		public BatchedRemoteIterator.BatchedListEntries<CachePoolEntry> ListCachePools(FSPermissionChecker
			 pc, string prevKey)
		{
			System.Diagnostics.Debug.Assert(namesystem.HasReadLock());
			int NumPreAllocatedEntries = 16;
			AList<CachePoolEntry> results = new AList<CachePoolEntry>(NumPreAllocatedEntries);
			SortedDictionary<string, CachePool> tailMap = cachePools.TailMap(prevKey, false);
			int numListed = 0;
			foreach (KeyValuePair<string, CachePool> cur in tailMap)
			{
				if (numListed++ >= maxListCachePoolsResponses)
				{
					return new BatchedRemoteIterator.BatchedListEntries<CachePoolEntry>(results, true
						);
				}
				results.AddItem(cur.Value.GetEntry(pc));
			}
			return new BatchedRemoteIterator.BatchedListEntries<CachePoolEntry>(results, false
				);
		}

		public void SetCachedLocations(LocatedBlock block)
		{
			CachedBlock cachedBlock = new CachedBlock(block.GetBlock().GetBlockId(), (short)0
				, false);
			cachedBlock = cachedBlocks.Get(cachedBlock);
			if (cachedBlock == null)
			{
				return;
			}
			IList<DatanodeDescriptor> datanodes = cachedBlock.GetDatanodes(DatanodeDescriptor.CachedBlocksList.Type
				.Cached);
			foreach (DatanodeDescriptor datanode in datanodes)
			{
				block.AddCachedLoc(datanode);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public void ProcessCacheReport(DatanodeID datanodeID, IList<long> blockIds)
		{
			namesystem.WriteLock();
			long startTime = Time.MonotonicNow();
			long endTime;
			try
			{
				DatanodeDescriptor datanode = blockManager.GetDatanodeManager().GetDatanode(datanodeID
					);
				if (datanode == null || !datanode.isAlive)
				{
					throw new IOException("processCacheReport from dead or unregistered datanode: " +
						 datanode);
				}
				ProcessCacheReportImpl(datanode, blockIds);
			}
			finally
			{
				endTime = Time.MonotonicNow();
				namesystem.WriteUnlock();
			}
			// Log the block report processing stats from Namenode perspective
			NameNodeMetrics metrics = NameNode.GetNameNodeMetrics();
			if (metrics != null)
			{
				metrics.AddCacheBlockReport((int)(endTime - startTime));
			}
			Log.Debug("Processed cache report from {}, blocks: {}, " + "processing time: {} msecs"
				, datanodeID, blockIds.Count, (endTime - startTime));
		}

		private void ProcessCacheReportImpl(DatanodeDescriptor datanode, IList<long> blockIds
			)
		{
			DatanodeDescriptor.CachedBlocksList cached = datanode.GetCached();
			cached.Clear();
			DatanodeDescriptor.CachedBlocksList cachedList = datanode.GetCached();
			DatanodeDescriptor.CachedBlocksList pendingCachedList = datanode.GetPendingCached
				();
			for (IEnumerator<long> iter = blockIds.GetEnumerator(); iter.HasNext(); )
			{
				long blockId = iter.Next();
				Log.Trace("Cache report from datanode {} has block {}", datanode, blockId);
				CachedBlock cachedBlock = new CachedBlock(blockId, (short)0, false);
				CachedBlock prevCachedBlock = cachedBlocks.Get(cachedBlock);
				// Add the block ID from the cache report to the cachedBlocks map
				// if it's not already there.
				if (prevCachedBlock != null)
				{
					cachedBlock = prevCachedBlock;
				}
				else
				{
					cachedBlocks.Put(cachedBlock);
					Log.Trace("Added block {}  to cachedBlocks", cachedBlock);
				}
				// Add the block to the datanode's implicit cached block list
				// if it's not already there.  Similarly, remove it from the pending
				// cached block list if it exists there.
				if (!cachedBlock.IsPresent(cachedList))
				{
					cachedList.AddItem(cachedBlock);
					Log.Trace("Added block {} to CACHED list.", cachedBlock);
				}
				if (cachedBlock.IsPresent(pendingCachedList))
				{
					pendingCachedList.Remove(cachedBlock);
					Log.Trace("Removed block {} from PENDING_CACHED list.", cachedBlock);
				}
			}
		}

		/// <summary>Saves the current state of the CacheManager to the DataOutput.</summary>
		/// <remarks>
		/// Saves the current state of the CacheManager to the DataOutput. Used
		/// to persist CacheManager state in the FSImage.
		/// </remarks>
		/// <param name="out">DataOutput to persist state</param>
		/// <param name="sdPath">path of the storage directory</param>
		/// <exception cref="System.IO.IOException"/>
		public void SaveStateCompat(DataOutputStream @out, string sdPath)
		{
			serializerCompat.Save(@out, sdPath);
		}

		/// <exception cref="System.IO.IOException"/>
		public CacheManager.PersistState SaveState()
		{
			AList<ClientNamenodeProtocolProtos.CachePoolInfoProto> pools = Lists.NewArrayListWithCapacity
				(cachePools.Count);
			AList<ClientNamenodeProtocolProtos.CacheDirectiveInfoProto> directives = Lists.NewArrayListWithCapacity
				(directivesById.Count);
			foreach (CachePool pool in cachePools.Values)
			{
				CachePoolInfo p = pool.GetInfo(true);
				ClientNamenodeProtocolProtos.CachePoolInfoProto.Builder b = ClientNamenodeProtocolProtos.CachePoolInfoProto
					.NewBuilder().SetPoolName(p.GetPoolName());
				if (p.GetOwnerName() != null)
				{
					b.SetOwnerName(p.GetOwnerName());
				}
				if (p.GetGroupName() != null)
				{
					b.SetGroupName(p.GetGroupName());
				}
				if (p.GetMode() != null)
				{
					b.SetMode(p.GetMode().ToShort());
				}
				if (p.GetLimit() != null)
				{
					b.SetLimit(p.GetLimit());
				}
				pools.AddItem(((ClientNamenodeProtocolProtos.CachePoolInfoProto)b.Build()));
			}
			foreach (CacheDirective directive in directivesById.Values)
			{
				CacheDirectiveInfo info = directive.ToInfo();
				ClientNamenodeProtocolProtos.CacheDirectiveInfoProto.Builder b = ClientNamenodeProtocolProtos.CacheDirectiveInfoProto
					.NewBuilder().SetId(info.GetId());
				if (info.GetPath() != null)
				{
					b.SetPath(info.GetPath().ToUri().GetPath());
				}
				if (info.GetReplication() != null)
				{
					b.SetReplication(info.GetReplication());
				}
				if (info.GetPool() != null)
				{
					b.SetPool(info.GetPool());
				}
				CacheDirectiveInfo.Expiration expiry = info.GetExpiration();
				if (expiry != null)
				{
					System.Diagnostics.Debug.Assert((!expiry.IsRelative()));
					b.SetExpiration(PBHelper.Convert(expiry));
				}
				directives.AddItem(((ClientNamenodeProtocolProtos.CacheDirectiveInfoProto)b.Build
					()));
			}
			FsImageProto.CacheManagerSection s = ((FsImageProto.CacheManagerSection)FsImageProto.CacheManagerSection
				.NewBuilder().SetNextDirectiveId(nextDirectiveId).SetNumPools(pools.Count).SetNumDirectives
				(directives.Count).Build());
			return new CacheManager.PersistState(s, pools, directives);
		}

		/// <summary>Reloads CacheManager state from the passed DataInput.</summary>
		/// <remarks>
		/// Reloads CacheManager state from the passed DataInput. Used during namenode
		/// startup to restore CacheManager state from an FSImage.
		/// </remarks>
		/// <param name="in">DataInput from which to restore state</param>
		/// <exception cref="System.IO.IOException"/>
		public void LoadStateCompat(DataInput @in)
		{
			serializerCompat.Load(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public void LoadState(CacheManager.PersistState s)
		{
			nextDirectiveId = s.section.GetNextDirectiveId();
			foreach (ClientNamenodeProtocolProtos.CachePoolInfoProto p in s.pools)
			{
				CachePoolInfo info = new CachePoolInfo(p.GetPoolName());
				if (p.HasOwnerName())
				{
					info.SetOwnerName(p.GetOwnerName());
				}
				if (p.HasGroupName())
				{
					info.SetGroupName(p.GetGroupName());
				}
				if (p.HasMode())
				{
					info.SetMode(new FsPermission((short)p.GetMode()));
				}
				if (p.HasLimit())
				{
					info.SetLimit(p.GetLimit());
				}
				AddCachePool(info);
			}
			foreach (ClientNamenodeProtocolProtos.CacheDirectiveInfoProto p_1 in s.directives)
			{
				// Get pool reference by looking it up in the map
				string poolName = p_1.GetPool();
				CacheDirective directive = new CacheDirective(p_1.GetId(), new Path(p_1.GetPath()
					).ToUri().GetPath(), (short)p_1.GetReplication(), p_1.GetExpiration().GetMillis(
					));
				AddCacheDirective(poolName, directive);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddCacheDirective(string poolName, CacheDirective directive)
		{
			CachePool pool = cachePools[poolName];
			if (pool == null)
			{
				throw new IOException("Directive refers to pool " + poolName + ", which does not exist."
					);
			}
			bool addedDirective = pool.GetDirectiveList().AddItem(directive);
			System.Diagnostics.Debug.Assert(addedDirective);
			if (directivesById[directive.GetId()] = directive != null)
			{
				throw new IOException("A directive with ID " + directive.GetId() + " already exists"
					);
			}
			IList<CacheDirective> directives = directivesByPath[directive.GetPath()];
			if (directives == null)
			{
				directives = new List<CacheDirective>();
				directivesByPath[directive.GetPath()] = directives;
			}
			directives.AddItem(directive);
		}

		private sealed class SerializerCompat
		{
			/// <exception cref="System.IO.IOException"/>
			private void Save(DataOutputStream @out, string sdPath)
			{
				@out.WriteLong(this._enclosing.nextDirectiveId);
				this.SavePools(@out, sdPath);
				this.SaveDirectives(@out, sdPath);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Load(DataInput @in)
			{
				this._enclosing.nextDirectiveId = @in.ReadLong();
				// pools need to be loaded first since directives point to their parent pool
				this.LoadPools(@in);
				this.LoadDirectives(@in);
			}

			/// <summary>Save cache pools to fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			private void SavePools(DataOutputStream @out, string sdPath)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				Step step = new Step(StepType.CachePools, sdPath);
				prog.BeginStep(Phase.SavingCheckpoint, step);
				prog.SetTotal(Phase.SavingCheckpoint, step, this._enclosing.cachePools.Count);
				StartupProgress.Counter counter = prog.GetCounter(Phase.SavingCheckpoint, step);
				@out.WriteInt(this._enclosing.cachePools.Count);
				foreach (CachePool pool in this._enclosing.cachePools.Values)
				{
					FSImageSerialization.WriteCachePoolInfo(@out, pool.GetInfo(true));
					counter.Increment();
				}
				prog.EndStep(Phase.SavingCheckpoint, step);
			}

			/*
			* Save cache entries to fsimage
			*/
			/// <exception cref="System.IO.IOException"/>
			private void SaveDirectives(DataOutputStream @out, string sdPath)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				Step step = new Step(StepType.CacheEntries, sdPath);
				prog.BeginStep(Phase.SavingCheckpoint, step);
				prog.SetTotal(Phase.SavingCheckpoint, step, this._enclosing.directivesById.Count);
				StartupProgress.Counter counter = prog.GetCounter(Phase.SavingCheckpoint, step);
				@out.WriteInt(this._enclosing.directivesById.Count);
				foreach (CacheDirective directive in this._enclosing.directivesById.Values)
				{
					FSImageSerialization.WriteCacheDirectiveInfo(@out, directive.ToInfo());
					counter.Increment();
				}
				prog.EndStep(Phase.SavingCheckpoint, step);
			}

			/// <summary>Load cache pools from fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			private void LoadPools(DataInput @in)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				Step step = new Step(StepType.CachePools);
				prog.BeginStep(Phase.LoadingFsimage, step);
				int numberOfPools = @in.ReadInt();
				prog.SetTotal(Phase.LoadingFsimage, step, numberOfPools);
				StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingFsimage, step);
				for (int i = 0; i < numberOfPools; i++)
				{
					this._enclosing.AddCachePool(FSImageSerialization.ReadCachePoolInfo(@in));
					counter.Increment();
				}
				prog.EndStep(Phase.LoadingFsimage, step);
			}

			/// <summary>Load cache directives from the fsimage</summary>
			/// <exception cref="System.IO.IOException"/>
			private void LoadDirectives(DataInput @in)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				Step step = new Step(StepType.CacheEntries);
				prog.BeginStep(Phase.LoadingFsimage, step);
				int numDirectives = @in.ReadInt();
				prog.SetTotal(Phase.LoadingFsimage, step, numDirectives);
				StartupProgress.Counter counter = prog.GetCounter(Phase.LoadingFsimage, step);
				for (int i = 0; i < numDirectives; i++)
				{
					CacheDirectiveInfo info = FSImageSerialization.ReadCacheDirectiveInfo(@in);
					// Get pool reference by looking it up in the map
					string poolName = info.GetPool();
					CacheDirective directive = new CacheDirective(info.GetId(), info.GetPath().ToUri(
						).GetPath(), info.GetReplication(), info.GetExpiration().GetAbsoluteMillis());
					this._enclosing.AddCacheDirective(poolName, directive);
					counter.Increment();
				}
				prog.EndStep(Phase.LoadingFsimage, step);
			}

			internal SerializerCompat(CacheManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CacheManager _enclosing;
		}

		public void WaitForRescanIfNeeded()
		{
			crmLock.Lock();
			try
			{
				if (monitor != null)
				{
					monitor.WaitForRescanIfNeeded();
				}
			}
			finally
			{
				crmLock.Unlock();
			}
		}

		private void SetNeedsRescan()
		{
			crmLock.Lock();
			try
			{
				if (monitor != null)
				{
					monitor.SetNeedsRescan();
				}
			}
			finally
			{
				crmLock.Unlock();
			}
		}

		[VisibleForTesting]
		public Sharpen.Thread GetCacheReplicationMonitor()
		{
			crmLock.Lock();
			try
			{
				return monitor;
			}
			finally
			{
				crmLock.Unlock();
			}
		}
	}
}
