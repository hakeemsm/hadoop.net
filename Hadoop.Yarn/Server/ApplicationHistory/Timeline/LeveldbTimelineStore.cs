using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Collections.Map;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Util;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Fusesource.Leveldbjni;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>
	/// <p>An implementation of an application timeline store backed by leveldb.</p>
	/// <p>There are three sections of the db, the start time section,
	/// the entity section, and the indexed entity section.</p>
	/// <p>The start time section is used to retrieve the unique start time for
	/// a given entity.
	/// </summary>
	/// <remarks>
	/// <p>An implementation of an application timeline store backed by leveldb.</p>
	/// <p>There are three sections of the db, the start time section,
	/// the entity section, and the indexed entity section.</p>
	/// <p>The start time section is used to retrieve the unique start time for
	/// a given entity. Its values each contain a start time while its keys are of
	/// the form:</p>
	/// <pre>
	/// START_TIME_LOOKUP_PREFIX + entity type + entity id</pre>
	/// <p>The entity section is ordered by entity type, then entity start time
	/// descending, then entity ID. There are four sub-sections of the entity
	/// section: events, primary filters, related entities,
	/// and other info. The event entries have event info serialized into their
	/// values. The other info entries have values corresponding to the values of
	/// the other info name/value map for the entry (note the names are contained
	/// in the key). All other entries have empty values. The key structure is as
	/// follows:</p>
	/// <pre>
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// EVENTS_COLUMN + reveventtimestamp + eventtype
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// PRIMARY_FILTERS_COLUMN + name + value
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// OTHER_INFO_COLUMN + name
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// RELATED_ENTITIES_COLUMN + relatedentity type + relatedentity id
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// DOMAIN_ID_COLUMN
	/// ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
	/// INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN + relatedentity type +
	/// relatedentity id</pre>
	/// <p>The indexed entity section contains a primary filter name and primary
	/// filter value as the prefix. Within a given name/value, entire entity
	/// entries are stored in the same format as described in the entity section
	/// above (below, "key" represents any one of the possible entity entry keys
	/// described above).</p>
	/// <pre>
	/// INDEXED_ENTRY_PREFIX + primaryfilter name + primaryfilter value +
	/// key</pre>
	/// </remarks>
	public class LeveldbTimelineStore : AbstractService, TimelineStore
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.LeveldbTimelineStore
			));

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal const string Filename = "leveldb-timeline-store.ldb";

		private static readonly byte[] StartTimeLookupPrefix = Sharpen.Runtime.GetBytesForString
			("k", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] EntityEntryPrefix = Sharpen.Runtime.GetBytesForString
			("e", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] IndexedEntryPrefix = Sharpen.Runtime.GetBytesForString
			("i", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] EventsColumn = Sharpen.Runtime.GetBytesForString("e"
			, Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] PrimaryFiltersColumn = Sharpen.Runtime.GetBytesForString
			("f", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] OtherInfoColumn = Sharpen.Runtime.GetBytesForString
			("i", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] RelatedEntitiesColumn = Sharpen.Runtime.GetBytesForString
			("r", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] InvisibleReverseRelatedEntitiesColumn = Sharpen.Runtime.GetBytesForString
			("z", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] DomainIdColumn = Sharpen.Runtime.GetBytesForString
			("d", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] DomainEntryPrefix = Sharpen.Runtime.GetBytesForString
			("d", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] OwnerLookupPrefix = Sharpen.Runtime.GetBytesForString
			("o", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] DescriptionColumn = Sharpen.Runtime.GetBytesForString
			("d", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] OwnerColumn = Sharpen.Runtime.GetBytesForString("o"
			, Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] ReaderColumn = Sharpen.Runtime.GetBytesForString("r"
			, Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] WriterColumn = Sharpen.Runtime.GetBytesForString("w"
			, Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] TimestampColumn = Sharpen.Runtime.GetBytesForString
			("t", Sharpen.Extensions.GetEncoding("UTF-8"));

		private static readonly byte[] EmptyBytes = new byte[0];

		private const string TimelineStoreVersionKey = "timeline-store-version";

		private static readonly Version CurrentVersionInfo = Version.NewInstance(1, 0);

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal static readonly FsPermission LeveldbDirUmask = FsPermission.CreateImmutable
			((short)0x1c0);

		private IDictionary<EntityIdentifier, LeveldbTimelineStore.StartAndInsertTime> startTimeWriteCache;

		private IDictionary<EntityIdentifier, long> startTimeReadCache;

		/// <summary>Per-entity locks are obtained when writing.</summary>
		private readonly LeveldbTimelineStore.LockMap<EntityIdentifier> writeLocks = new 
			LeveldbTimelineStore.LockMap<EntityIdentifier>();

		private readonly ReentrantReadWriteLock deleteLock = new ReentrantReadWriteLock();

		private DB db;

		private Sharpen.Thread deletionThread;

		public LeveldbTimelineStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.LeveldbTimelineStore).FullName
				)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceTtlMs, 
				YarnConfiguration.DefaultTimelineServiceTtlMs) > 0, "%s property value should be greater than zero"
				, YarnConfiguration.TimelineServiceTtlMs);
			Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs
				, YarnConfiguration.DefaultTimelineServiceLeveldbTtlIntervalMs) > 0, "%s property value should be greater than zero"
				, YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs);
			Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceLeveldbReadCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbReadCacheSize) >= 0, "%s property value should be greater than or equal to zero"
				, YarnConfiguration.TimelineServiceLeveldbReadCacheSize);
			Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbStartTimeReadCacheSize) > 0, " %s property value should be greater than zero"
				, YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize);
			Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbStartTimeWriteCacheSize) > 0, "%s property value should be greater than zero"
				, YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize);
			Options options = new Options();
			options.CreateIfMissing(true);
			options.CacheSize(conf.GetLong(YarnConfiguration.TimelineServiceLeveldbReadCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbReadCacheSize));
			JniDBFactory factory = new JniDBFactory();
			Path dbPath = new Path(conf.Get(YarnConfiguration.TimelineServiceLeveldbPath), Filename
				);
			FileSystem localFS = null;
			try
			{
				localFS = FileSystem.GetLocal(conf);
				if (!localFS.Exists(dbPath))
				{
					if (!localFS.Mkdirs(dbPath))
					{
						throw new IOException("Couldn't create directory for leveldb " + "timeline store "
							 + dbPath);
					}
					localFS.SetPermission(dbPath, LeveldbDirUmask);
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, localFS);
			}
			Log.Info("Using leveldb path " + dbPath);
			db = factory.Open(new FilePath(dbPath.ToString()), options);
			CheckVersion();
			startTimeWriteCache = Sharpen.Collections.SynchronizedMap(new LRUMap(GetStartTimeWriteCacheSize
				(conf)));
			startTimeReadCache = Sharpen.Collections.SynchronizedMap(new LRUMap(GetStartTimeReadCacheSize
				(conf)));
			if (conf.GetBoolean(YarnConfiguration.TimelineServiceTtlEnable, true))
			{
				deletionThread = new LeveldbTimelineStore.EntityDeletionThread(this, conf);
				deletionThread.Start();
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (deletionThread != null)
			{
				deletionThread.Interrupt();
				Log.Info("Waiting for deletion thread to complete its current action");
				try
				{
					deletionThread.Join();
				}
				catch (Exception e)
				{
					Log.Warn("Interrupted while waiting for deletion thread to complete," + " closing db now"
						, e);
				}
			}
			IOUtils.Cleanup(Log, db);
			base.ServiceStop();
		}

		private class StartAndInsertTime
		{
			internal readonly long startTime;

			internal readonly long insertTime;

			public StartAndInsertTime(long startTime, long insertTime)
			{
				this.startTime = startTime;
				this.insertTime = insertTime;
			}
		}

		private class EntityDeletionThread : Sharpen.Thread
		{
			private readonly long ttl;

			private readonly long ttlInterval;

			public EntityDeletionThread(LeveldbTimelineStore _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.ttl = conf.GetLong(YarnConfiguration.TimelineServiceTtlMs, YarnConfiguration
					.DefaultTimelineServiceTtlMs);
				this.ttlInterval = conf.GetLong(YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs
					, YarnConfiguration.DefaultTimelineServiceLeveldbTtlIntervalMs);
				LeveldbTimelineStore.Log.Info("Starting deletion thread with ttl " + this.ttl + " and cycle "
					 + "interval " + this.ttlInterval);
			}

			public override void Run()
			{
				while (true)
				{
					long timestamp = Runtime.CurrentTimeMillis() - this.ttl;
					try
					{
						this._enclosing.DiscardOldEntities(timestamp);
						Sharpen.Thread.Sleep(this.ttlInterval);
					}
					catch (IOException e)
					{
						LeveldbTimelineStore.Log.Error(e);
					}
					catch (Exception)
					{
						LeveldbTimelineStore.Log.Info("Deletion thread received interrupt, exiting");
						break;
					}
				}
			}

			private readonly LeveldbTimelineStore _enclosing;
		}

		private class LockMap<K>
		{
			[System.Serializable]
			private class CountingReentrantLock<K> : ReentrantLock
			{
				private const long serialVersionUID = 1L;

				private int count;

				private K key;

				internal CountingReentrantLock(K key)
					: base()
				{
					this.count = 0;
					this.key = key;
				}
			}

			private IDictionary<K, LeveldbTimelineStore.LockMap.CountingReentrantLock<K>> locks
				 = new Dictionary<K, LeveldbTimelineStore.LockMap.CountingReentrantLock<K>>();

			internal virtual LeveldbTimelineStore.LockMap.CountingReentrantLock<K> GetLock(K 
				key)
			{
				lock (this)
				{
					LeveldbTimelineStore.LockMap.CountingReentrantLock<K> Lock = locks[key];
					if (Lock == null)
					{
						Lock = new LeveldbTimelineStore.LockMap.CountingReentrantLock<K>(key);
						locks[key] = Lock;
					}
					Lock.count++;
					return Lock;
				}
			}

			internal virtual void ReturnLock(LeveldbTimelineStore.LockMap.CountingReentrantLock
				<K> Lock)
			{
				lock (this)
				{
					if (Lock.count == 0)
					{
						throw new InvalidOperationException("Returned lock more times than it " + "was retrieved"
							);
					}
					Lock.count--;
					if (Lock.count == 0)
					{
						Sharpen.Collections.Remove(locks, Lock.key);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEntity GetEntity(string entityId, string entityType, EnumSet
			<TimelineReader.Field> fields)
		{
			long revStartTime = GetStartTimeLong(entityId, entityType);
			if (revStartTime == null)
			{
				return null;
			}
			byte[] prefix = LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(
				entityType).Add(GenericObjectMapper.WriteReverseOrderedLong(revStartTime)).Add(entityId
				).GetBytesForLookup();
			LeveldbIterator iterator = null;
			try
			{
				iterator = new LeveldbIterator(db);
				iterator.Seek(prefix);
				return GetEntity(entityId, entityType, revStartTime, fields, iterator, prefix, prefix
					.Length);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
		}

		/// <summary>Read entity from a db iterator.</summary>
		/// <remarks>
		/// Read entity from a db iterator.  If no information is found in the
		/// specified fields for this entity, return null.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static TimelineEntity GetEntity(string entityId, string entityType, long 
			startTime, EnumSet<TimelineReader.Field> fields, LeveldbIterator iterator, byte[]
			 prefix, int prefixlen)
		{
			if (fields == null)
			{
				fields = EnumSet.AllOf<TimelineReader.Field>();
			}
			TimelineEntity entity = new TimelineEntity();
			bool events = false;
			bool lastEvent = false;
			if (fields.Contains(TimelineReader.Field.Events))
			{
				events = true;
			}
			else
			{
				if (fields.Contains(TimelineReader.Field.LastEventOnly))
				{
					lastEvent = true;
				}
				else
				{
					entity.SetEvents(null);
				}
			}
			bool relatedEntities = false;
			if (fields.Contains(TimelineReader.Field.RelatedEntities))
			{
				relatedEntities = true;
			}
			else
			{
				entity.SetRelatedEntities(null);
			}
			bool primaryFilters = false;
			if (fields.Contains(TimelineReader.Field.PrimaryFilters))
			{
				primaryFilters = true;
			}
			else
			{
				entity.SetPrimaryFilters(null);
			}
			bool otherInfo = false;
			if (fields.Contains(TimelineReader.Field.OtherInfo))
			{
				otherInfo = true;
			}
			else
			{
				entity.SetOtherInfo(null);
			}
			// iterate through the entity's entry, parsing information if it is part
			// of a requested field
			for (; iterator.HasNext(); iterator.Next())
			{
				byte[] key = iterator.PeekNext().Key;
				if (!LeveldbUtils.PrefixMatches(prefix, prefixlen, key))
				{
					break;
				}
				if (key.Length == prefixlen)
				{
					continue;
				}
				if (key[prefixlen] == PrimaryFiltersColumn[0])
				{
					if (primaryFilters)
					{
						AddPrimaryFilter(entity, key, prefixlen + PrimaryFiltersColumn.Length);
					}
				}
				else
				{
					if (key[prefixlen] == OtherInfoColumn[0])
					{
						if (otherInfo)
						{
							entity.AddOtherInfo(ParseRemainingKey(key, prefixlen + OtherInfoColumn.Length), GenericObjectMapper
								.Read(iterator.PeekNext().Value));
						}
					}
					else
					{
						if (key[prefixlen] == RelatedEntitiesColumn[0])
						{
							if (relatedEntities)
							{
								AddRelatedEntity(entity, key, prefixlen + RelatedEntitiesColumn.Length);
							}
						}
						else
						{
							if (key[prefixlen] == EventsColumn[0])
							{
								if (events || (lastEvent && entity.GetEvents().Count == 0))
								{
									TimelineEvent @event = GetEntityEvent(null, key, prefixlen + EventsColumn.Length, 
										iterator.PeekNext().Value);
									if (@event != null)
									{
										entity.AddEvent(@event);
									}
								}
							}
							else
							{
								if (key[prefixlen] == DomainIdColumn[0])
								{
									byte[] v = iterator.PeekNext().Value;
									string domainId = new string(v, Sharpen.Extensions.GetEncoding("UTF-8"));
									entity.SetDomainId(domainId);
								}
								else
								{
									if (key[prefixlen] != InvisibleReverseRelatedEntitiesColumn[0])
									{
										Log.Warn(string.Format("Found unexpected column for entity %s of " + "type %s (0x%02x)"
											, entityId, entityType, key[prefixlen]));
									}
								}
							}
						}
					}
				}
			}
			entity.SetEntityId(entityId);
			entity.SetEntityType(entityType);
			entity.SetStartTime(startTime);
			return entity;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEvents GetEntityTimelines(string entityType, ICollection<string
			> entityIds, long limit, long windowStart, long windowEnd, ICollection<string> eventType
			)
		{
			TimelineEvents events = new TimelineEvents();
			if (entityIds == null || entityIds.IsEmpty())
			{
				return events;
			}
			// create a lexicographically-ordered map from start time to entities
			IDictionary<byte[], IList<EntityIdentifier>> startTimeMap = new SortedDictionary<
				byte[], IList<EntityIdentifier>>(new _IComparer_474());
			LeveldbIterator iterator = null;
			try
			{
				// look up start times for the specified entities
				// skip entities with no start time
				foreach (string entityId in entityIds)
				{
					byte[] startTime = GetStartTime(entityId, entityType);
					if (startTime != null)
					{
						IList<EntityIdentifier> entities = startTimeMap[startTime];
						if (entities == null)
						{
							entities = new AList<EntityIdentifier>();
							startTimeMap[startTime] = entities;
						}
						entities.AddItem(new EntityIdentifier(entityId, entityType));
					}
				}
				foreach (KeyValuePair<byte[], IList<EntityIdentifier>> entry in startTimeMap)
				{
					// look up the events matching the given parameters (limit,
					// start time, end time, event types) for entities whose start times
					// were found and add the entities to the return list
					byte[] revStartTime = entry.Key;
					foreach (EntityIdentifier entityIdentifier in entry.Value)
					{
						TimelineEvents.EventsOfOneEntity entity = new TimelineEvents.EventsOfOneEntity();
						entity.SetEntityId(entityIdentifier.GetId());
						entity.SetEntityType(entityType);
						events.AddEvent(entity);
						LeveldbUtils.KeyBuilder kb = LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix
							).Add(entityType).Add(revStartTime).Add(entityIdentifier.GetId()).Add(EventsColumn
							);
						byte[] prefix = kb.GetBytesForLookup();
						if (windowEnd == null)
						{
							windowEnd = long.MaxValue;
						}
						byte[] revts = GenericObjectMapper.WriteReverseOrderedLong(windowEnd);
						kb.Add(revts);
						byte[] first = kb.GetBytesForLookup();
						byte[] last = null;
						if (windowStart != null)
						{
							last = LeveldbUtils.KeyBuilder.NewInstance().Add(prefix).Add(GenericObjectMapper.WriteReverseOrderedLong
								(windowStart)).GetBytesForLookup();
						}
						if (limit == null)
						{
							limit = DefaultLimit;
						}
						iterator = new LeveldbIterator(db);
						for (iterator.Seek(first); entity.GetEvents().Count < limit && iterator.HasNext()
							; iterator.Next())
						{
							byte[] key = iterator.PeekNext().Key;
							if (!LeveldbUtils.PrefixMatches(prefix, prefix.Length, key) || (last != null && WritableComparator
								.CompareBytes(key, 0, key.Length, last, 0, last.Length) > 0))
							{
								break;
							}
							TimelineEvent @event = GetEntityEvent(eventType, key, prefix.Length, iterator.PeekNext
								().Value);
							if (@event != null)
							{
								entity.AddEvent(@event);
							}
						}
					}
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
			return events;
		}

		private sealed class _IComparer_474 : IComparer<byte[]>
		{
			public _IComparer_474()
			{
			}

			public int Compare(byte[] o1, byte[] o2)
			{
				return WritableComparator.CompareBytes(o1, 0, o1.Length, o2, 0, o2.Length);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEntities GetEntities(string entityType, long limit, long windowStart
			, long windowEnd, string fromId, long fromTs, NameValuePair primaryFilter, ICollection
			<NameValuePair> secondaryFilters, EnumSet<TimelineReader.Field> fields, TimelineDataManager.CheckAcl
			 checkAcl)
		{
			if (primaryFilter == null)
			{
				// if no primary filter is specified, prefix the lookup with
				// ENTITY_ENTRY_PREFIX
				return GetEntityByTime(EntityEntryPrefix, entityType, limit, windowStart, windowEnd
					, fromId, fromTs, secondaryFilters, fields, checkAcl);
			}
			else
			{
				// if a primary filter is specified, prefix the lookup with
				// INDEXED_ENTRY_PREFIX + primaryFilterName + primaryFilterValue +
				// ENTITY_ENTRY_PREFIX
				byte[] @base = LeveldbUtils.KeyBuilder.NewInstance().Add(IndexedEntryPrefix).Add(
					primaryFilter.GetName()).Add(GenericObjectMapper.Write(primaryFilter.GetValue())
					, true).Add(EntityEntryPrefix).GetBytesForLookup();
				return GetEntityByTime(@base, entityType, limit, windowStart, windowEnd, fromId, 
					fromTs, secondaryFilters, fields, checkAcl);
			}
		}

		/// <summary>Retrieves a list of entities satisfying given parameters.</summary>
		/// <param name="base">A byte array prefix for the lookup</param>
		/// <param name="entityType">The type of the entity</param>
		/// <param name="limit">A limit on the number of entities to return</param>
		/// <param name="starttime">The earliest entity start time to retrieve (exclusive)</param>
		/// <param name="endtime">The latest entity start time to retrieve (inclusive)</param>
		/// <param name="fromId">Retrieve entities starting with this entity</param>
		/// <param name="fromTs">Ignore entities with insert timestamp later than this ts</param>
		/// <param name="secondaryFilters">Filter pairs that the entities should match</param>
		/// <param name="fields">The set of fields to retrieve</param>
		/// <returns>A list of entities</returns>
		/// <exception cref="System.IO.IOException"/>
		private TimelineEntities GetEntityByTime(byte[] @base, string entityType, long limit
			, long starttime, long endtime, string fromId, long fromTs, ICollection<NameValuePair
			> secondaryFilters, EnumSet<TimelineReader.Field> fields, TimelineDataManager.CheckAcl
			 checkAcl)
		{
			LeveldbIterator iterator = null;
			try
			{
				LeveldbUtils.KeyBuilder kb = LeveldbUtils.KeyBuilder.NewInstance().Add(@base).Add
					(entityType);
				// only db keys matching the prefix (base + entity type) will be parsed
				byte[] prefix = kb.GetBytesForLookup();
				if (endtime == null)
				{
					// if end time is null, place no restriction on end time
					endtime = long.MaxValue;
				}
				// construct a first key that will be seeked to using end time or fromId
				byte[] first = null;
				if (fromId != null)
				{
					long fromIdStartTime = GetStartTimeLong(fromId, entityType);
					if (fromIdStartTime == null)
					{
						// no start time for provided id, so return empty entities
						return new TimelineEntities();
					}
					if (fromIdStartTime <= endtime)
					{
						// if provided id's start time falls before the end of the window,
						// use it to construct the seek key
						first = kb.Add(GenericObjectMapper.WriteReverseOrderedLong(fromIdStartTime)).Add(
							fromId).GetBytesForLookup();
					}
				}
				// if seek key wasn't constructed using fromId, construct it using end ts
				if (first == null)
				{
					first = kb.Add(GenericObjectMapper.WriteReverseOrderedLong(endtime)).GetBytesForLookup
						();
				}
				byte[] last = null;
				if (starttime != null)
				{
					// if start time is not null, set a last key that will not be
					// iterated past
					last = LeveldbUtils.KeyBuilder.NewInstance().Add(@base).Add(entityType).Add(GenericObjectMapper.WriteReverseOrderedLong
						(starttime)).GetBytesForLookup();
				}
				if (limit == null)
				{
					// if limit is not specified, use the default
					limit = DefaultLimit;
				}
				TimelineEntities entities = new TimelineEntities();
				iterator = new LeveldbIterator(db);
				iterator.Seek(first);
				// iterate until one of the following conditions is met: limit is
				// reached, there are no more keys, the key prefix no longer matches,
				// or a start time has been specified and reached/exceeded
				while (entities.GetEntities().Count < limit && iterator.HasNext())
				{
					byte[] key = iterator.PeekNext().Key;
					if (!LeveldbUtils.PrefixMatches(prefix, prefix.Length, key) || (last != null && WritableComparator
						.CompareBytes(key, 0, key.Length, last, 0, last.Length) > 0))
					{
						break;
					}
					// read the start time and entity id from the current key
					LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, prefix.Length);
					long startTime = kp.GetNextLong();
					string entityId = kp.GetNextString();
					if (fromTs != null)
					{
						long insertTime = GenericObjectMapper.ReadReverseOrderedLong(iterator.PeekNext().
							Value, 0);
						if (insertTime > fromTs)
						{
							byte[] firstKey = key;
							while (iterator.HasNext() && LeveldbUtils.PrefixMatches(firstKey, kp.GetOffset(), 
								key))
							{
								iterator.Next();
								key = iterator.PeekNext().Key;
							}
							continue;
						}
					}
					// parse the entity that owns this key, iterating over all keys for
					// the entity
					TimelineEntity entity = GetEntity(entityId, entityType, startTime, fields, iterator
						, key, kp.GetOffset());
					// determine if the retrieved entity matches the provided secondary
					// filters, and if so add it to the list of entities to return
					bool filterPassed = true;
					if (secondaryFilters != null)
					{
						foreach (NameValuePair filter in secondaryFilters)
						{
							object v = entity.GetOtherInfo()[filter.GetName()];
							if (v == null)
							{
								ICollection<object> vs = entity.GetPrimaryFilters()[filter.GetName()];
								if (vs == null || !vs.Contains(filter.GetValue()))
								{
									filterPassed = false;
									break;
								}
							}
							else
							{
								if (!v.Equals(filter.GetValue()))
								{
									filterPassed = false;
									break;
								}
							}
						}
					}
					if (filterPassed)
					{
						if (entity.GetDomainId() == null)
						{
							entity.SetDomainId(TimelineDataManager.DefaultDomainId);
						}
						if (checkAcl == null || checkAcl.Check(entity))
						{
							entities.AddEntity(entity);
						}
					}
				}
				return entities;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
		}

		/// <summary>Handle error and set it in response.</summary>
		private static void HandleError(TimelineEntity entity, TimelinePutResponse response
			, int errorCode)
		{
			TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
				();
			error.SetEntityId(entity.GetEntityId());
			error.SetEntityType(entity.GetEntityType());
			error.SetErrorCode(errorCode);
			response.AddError(error);
		}

		/// <summary>Put a single entity.</summary>
		/// <remarks>
		/// Put a single entity.  If there is an error, add a TimelinePutError to the
		/// given response.
		/// </remarks>
		private void Put(TimelineEntity entity, TimelinePutResponse response, bool allowEmptyDomainId
			)
		{
			LeveldbTimelineStore.LockMap.CountingReentrantLock<EntityIdentifier> Lock = writeLocks
				.GetLock(new EntityIdentifier(entity.GetEntityId(), entity.GetEntityType()));
			Lock.Lock();
			WriteBatch writeBatch = null;
			IList<EntityIdentifier> relatedEntitiesWithoutStartTimes = new AList<EntityIdentifier
				>();
			byte[] revStartTime = null;
			IDictionary<string, ICollection<object>> primaryFilters = null;
			try
			{
				writeBatch = db.CreateWriteBatch();
				IList<TimelineEvent> events = entity.GetEvents();
				// look up the start time for the entity
				LeveldbTimelineStore.StartAndInsertTime startAndInsertTime = GetAndSetStartTime(entity
					.GetEntityId(), entity.GetEntityType(), entity.GetStartTime(), events);
				if (startAndInsertTime == null)
				{
					// if no start time is found, add an error and return
					HandleError(entity, response, TimelinePutResponse.TimelinePutError.NoStartTime);
					return;
				}
				revStartTime = GenericObjectMapper.WriteReverseOrderedLong(startAndInsertTime.startTime
					);
				primaryFilters = entity.GetPrimaryFilters();
				// write entity marker
				byte[] markerKey = CreateEntityMarkerKey(entity.GetEntityId(), entity.GetEntityType
					(), revStartTime);
				byte[] markerValue = GenericObjectMapper.WriteReverseOrderedLong(startAndInsertTime
					.insertTime);
				writeBatch.Put(markerKey, markerValue);
				WritePrimaryFilterEntries(writeBatch, primaryFilters, markerKey, markerValue);
				// write event entries
				if (events != null && !events.IsEmpty())
				{
					foreach (TimelineEvent @event in events)
					{
						byte[] revts = GenericObjectMapper.WriteReverseOrderedLong(@event.GetTimestamp());
						byte[] key = CreateEntityEventKey(entity.GetEntityId(), entity.GetEntityType(), revStartTime
							, revts, @event.GetEventType());
						byte[] value = GenericObjectMapper.Write(@event.GetEventInfo());
						writeBatch.Put(key, value);
						WritePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
					}
				}
				// write related entity entries
				IDictionary<string, ICollection<string>> relatedEntities = entity.GetRelatedEntities
					();
				if (relatedEntities != null && !relatedEntities.IsEmpty())
				{
					foreach (KeyValuePair<string, ICollection<string>> relatedEntityList in relatedEntities)
					{
						string relatedEntityType = relatedEntityList.Key;
						foreach (string relatedEntityId in relatedEntityList.Value)
						{
							// invisible "reverse" entries (entity -> related entity)
							byte[] key = CreateReverseRelatedEntityKey(entity.GetEntityId(), entity.GetEntityType
								(), revStartTime, relatedEntityId, relatedEntityType);
							writeBatch.Put(key, EmptyBytes);
							// look up start time of related entity
							byte[] relatedEntityStartTime = GetStartTime(relatedEntityId, relatedEntityType);
							// delay writing the related entity if no start time is found
							if (relatedEntityStartTime == null)
							{
								relatedEntitiesWithoutStartTimes.AddItem(new EntityIdentifier(relatedEntityId, relatedEntityType
									));
								continue;
							}
							else
							{
								// This is the existing entity
								byte[] domainIdBytes = db.Get(CreateDomainIdKey(relatedEntityId, relatedEntityType
									, relatedEntityStartTime));
								// The timeline data created by the server before 2.6 won't have
								// the domain field. We assume this timeline data is in the
								// default timeline domain.
								string domainId = null;
								if (domainIdBytes == null)
								{
									domainId = TimelineDataManager.DefaultDomainId;
								}
								else
								{
									domainId = new string(domainIdBytes, Sharpen.Extensions.GetEncoding("UTF-8"));
								}
								if (!domainId.Equals(entity.GetDomainId()))
								{
									// in this case the entity will be put, but the relation will be
									// ignored
									HandleError(entity, response, TimelinePutResponse.TimelinePutError.ForbiddenRelation
										);
									continue;
								}
							}
							// write "forward" entry (related entity -> entity)
							key = CreateRelatedEntityKey(relatedEntityId, relatedEntityType, relatedEntityStartTime
								, entity.GetEntityId(), entity.GetEntityType());
							writeBatch.Put(key, EmptyBytes);
						}
					}
				}
				// write primary filter entries
				if (primaryFilters != null && !primaryFilters.IsEmpty())
				{
					foreach (KeyValuePair<string, ICollection<object>> primaryFilter in primaryFilters)
					{
						foreach (object primaryFilterValue in primaryFilter.Value)
						{
							byte[] key = CreatePrimaryFilterKey(entity.GetEntityId(), entity.GetEntityType(), 
								revStartTime, primaryFilter.Key, primaryFilterValue);
							writeBatch.Put(key, EmptyBytes);
							WritePrimaryFilterEntries(writeBatch, primaryFilters, key, EmptyBytes);
						}
					}
				}
				// write other info entries
				IDictionary<string, object> otherInfo = entity.GetOtherInfo();
				if (otherInfo != null && !otherInfo.IsEmpty())
				{
					foreach (KeyValuePair<string, object> i in otherInfo)
					{
						byte[] key = CreateOtherInfoKey(entity.GetEntityId(), entity.GetEntityType(), revStartTime
							, i.Key);
						byte[] value = GenericObjectMapper.Write(i.Value);
						writeBatch.Put(key, value);
						WritePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
					}
				}
				// write domain id entry
				byte[] key_1 = CreateDomainIdKey(entity.GetEntityId(), entity.GetEntityType(), revStartTime
					);
				if (entity.GetDomainId() == null || entity.GetDomainId().Length == 0)
				{
					if (!allowEmptyDomainId)
					{
						HandleError(entity, response, TimelinePutResponse.TimelinePutError.NoDomain);
						return;
					}
				}
				else
				{
					writeBatch.Put(key_1, Sharpen.Runtime.GetBytesForString(entity.GetDomainId(), Sharpen.Extensions.GetEncoding
						("UTF-8")));
					WritePrimaryFilterEntries(writeBatch, primaryFilters, key_1, Sharpen.Runtime.GetBytesForString
						(entity.GetDomainId(), Sharpen.Extensions.GetEncoding("UTF-8")));
				}
				db.Write(writeBatch);
			}
			catch (DBException de)
			{
				Log.Error("Error putting entity " + entity.GetEntityId() + " of type " + entity.GetEntityType
					(), de);
				HandleError(entity, response, TimelinePutResponse.TimelinePutError.IoException);
			}
			catch (IOException e)
			{
				Log.Error("Error putting entity " + entity.GetEntityId() + " of type " + entity.GetEntityType
					(), e);
				HandleError(entity, response, TimelinePutResponse.TimelinePutError.IoException);
			}
			finally
			{
				Lock.Unlock();
				writeLocks.ReturnLock(Lock);
				IOUtils.Cleanup(Log, writeBatch);
			}
			foreach (EntityIdentifier relatedEntity in relatedEntitiesWithoutStartTimes)
			{
				Lock = writeLocks.GetLock(relatedEntity);
				Lock.Lock();
				try
				{
					LeveldbTimelineStore.StartAndInsertTime relatedEntityStartAndInsertTime = GetAndSetStartTime
						(relatedEntity.GetId(), relatedEntity.GetType(), GenericObjectMapper.ReadReverseOrderedLong
						(revStartTime, 0), null);
					if (relatedEntityStartAndInsertTime == null)
					{
						throw new IOException("Error setting start time for related entity");
					}
					byte[] relatedEntityStartTime = GenericObjectMapper.WriteReverseOrderedLong(relatedEntityStartAndInsertTime
						.startTime);
					// This is the new entity, the domain should be the same
					byte[] key = CreateDomainIdKey(relatedEntity.GetId(), relatedEntity.GetType(), relatedEntityStartTime
						);
					db.Put(key, Sharpen.Runtime.GetBytesForString(entity.GetDomainId(), Sharpen.Extensions.GetEncoding
						("UTF-8")));
					db.Put(CreateRelatedEntityKey(relatedEntity.GetId(), relatedEntity.GetType(), relatedEntityStartTime
						, entity.GetEntityId(), entity.GetEntityType()), EmptyBytes);
					db.Put(CreateEntityMarkerKey(relatedEntity.GetId(), relatedEntity.GetType(), relatedEntityStartTime
						), GenericObjectMapper.WriteReverseOrderedLong(relatedEntityStartAndInsertTime.insertTime
						));
				}
				catch (DBException de)
				{
					Log.Error("Error putting related entity " + relatedEntity.GetId() + " of type " +
						 relatedEntity.GetType() + " for entity " + entity.GetEntityId() + " of type " +
						 entity.GetEntityType(), de);
					HandleError(entity, response, TimelinePutResponse.TimelinePutError.IoException);
				}
				catch (IOException e)
				{
					Log.Error("Error putting related entity " + relatedEntity.GetId() + " of type " +
						 relatedEntity.GetType() + " for entity " + entity.GetEntityId() + " of type " +
						 entity.GetEntityType(), e);
					HandleError(entity, response, TimelinePutResponse.TimelinePutError.IoException);
				}
				finally
				{
					Lock.Unlock();
					writeLocks.ReturnLock(Lock);
				}
			}
		}

		/// <summary>
		/// For a given key / value pair that has been written to the db,
		/// write additional entries to the db for each primary filter.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void WritePrimaryFilterEntries(WriteBatch writeBatch, IDictionary<
			string, ICollection<object>> primaryFilters, byte[] key, byte[] value)
		{
			if (primaryFilters != null && !primaryFilters.IsEmpty())
			{
				foreach (KeyValuePair<string, ICollection<object>> pf in primaryFilters)
				{
					foreach (object pfval in pf.Value)
					{
						writeBatch.Put(AddPrimaryFilterToKey(pf.Key, pfval, key), value);
					}
				}
			}
		}

		public virtual TimelinePutResponse Put(TimelineEntities entities)
		{
			try
			{
				deleteLock.ReadLock().Lock();
				TimelinePutResponse response = new TimelinePutResponse();
				foreach (TimelineEntity entity in entities.GetEntities())
				{
					Put(entity, response, false);
				}
				return response;
			}
			finally
			{
				deleteLock.ReadLock().Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual TimelinePutResponse PutWithNoDomainId(TimelineEntities entities)
		{
			try
			{
				deleteLock.ReadLock().Lock();
				TimelinePutResponse response = new TimelinePutResponse();
				foreach (TimelineEntity entity in entities.GetEntities())
				{
					Put(entity, response, true);
				}
				return response;
			}
			finally
			{
				deleteLock.ReadLock().Unlock();
			}
		}

		/// <summary>
		/// Get the unique start time for a given entity as a byte array that sorts
		/// the timestamps in reverse order (see
		/// <see cref="GenericObjectMapper.WriteReverseOrderedLong(long)"/>
		/// ).
		/// </summary>
		/// <param name="entityId">The id of the entity</param>
		/// <param name="entityType">The type of the entity</param>
		/// <returns>A byte array, null if not found</returns>
		/// <exception cref="System.IO.IOException"/>
		private byte[] GetStartTime(string entityId, string entityType)
		{
			long l = GetStartTimeLong(entityId, entityType);
			return l == null ? null : GenericObjectMapper.WriteReverseOrderedLong(l);
		}

		/// <summary>Get the unique start time for a given entity as a Long.</summary>
		/// <param name="entityId">The id of the entity</param>
		/// <param name="entityType">The type of the entity</param>
		/// <returns>A Long, null if not found</returns>
		/// <exception cref="System.IO.IOException"/>
		private long GetStartTimeLong(string entityId, string entityType)
		{
			EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
			try
			{
				// start time is not provided, so try to look it up
				if (startTimeReadCache.Contains(entity))
				{
					// found the start time in the cache
					return startTimeReadCache[entity];
				}
				else
				{
					// try to look up the start time in the db
					byte[] b = CreateStartTimeLookupKey(entity.GetId(), entity.GetType());
					byte[] v = db.Get(b);
					if (v == null)
					{
						// did not find the start time in the db
						return null;
					}
					else
					{
						// found the start time in the db
						long l = GenericObjectMapper.ReadReverseOrderedLong(v, 0);
						startTimeReadCache[entity] = l;
						return l;
					}
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <summary>
		/// Get the unique start time for a given entity as a byte array that sorts
		/// the timestamps in reverse order (see
		/// <see cref="GenericObjectMapper.WriteReverseOrderedLong(long)"/>
		/// ). If the start time
		/// doesn't exist, set it based on the information provided. Should only be
		/// called when a lock has been obtained on the entity.
		/// </summary>
		/// <param name="entityId">The id of the entity</param>
		/// <param name="entityType">The type of the entity</param>
		/// <param name="startTime">The start time of the entity, or null</param>
		/// <param name="events">A list of events for the entity, or null</param>
		/// <returns>A StartAndInsertTime</returns>
		/// <exception cref="System.IO.IOException"/>
		private LeveldbTimelineStore.StartAndInsertTime GetAndSetStartTime(string entityId
			, string entityType, long startTime, IList<TimelineEvent> events)
		{
			EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
			if (startTime == null)
			{
				// start time is not provided, so try to look it up
				if (startTimeWriteCache.Contains(entity))
				{
					// found the start time in the cache
					return startTimeWriteCache[entity];
				}
				else
				{
					if (events != null)
					{
						// prepare a start time from events in case it is needed
						long min = long.MaxValue;
						foreach (TimelineEvent e in events)
						{
							if (min > e.GetTimestamp())
							{
								min = e.GetTimestamp();
							}
						}
						startTime = min;
					}
					return CheckStartTimeInDb(entity, startTime);
				}
			}
			else
			{
				// start time is provided
				if (startTimeWriteCache.Contains(entity))
				{
					// always use start time from cache if it exists
					return startTimeWriteCache[entity];
				}
				else
				{
					// check the provided start time matches the db
					return CheckStartTimeInDb(entity, startTime);
				}
			}
		}

		/// <summary>Checks db for start time and returns it if it exists.</summary>
		/// <remarks>
		/// Checks db for start time and returns it if it exists.  If it doesn't
		/// exist, writes the suggested start time (if it is not null).  This is
		/// only called when the start time is not found in the cache,
		/// so it adds it back into the cache if it is found. Should only be called
		/// when a lock has been obtained on the entity.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private LeveldbTimelineStore.StartAndInsertTime CheckStartTimeInDb(EntityIdentifier
			 entity, long suggestedStartTime)
		{
			LeveldbTimelineStore.StartAndInsertTime startAndInsertTime = null;
			// create lookup key for start time
			byte[] b = CreateStartTimeLookupKey(entity.GetId(), entity.GetType());
			try
			{
				// retrieve value for key
				byte[] v = db.Get(b);
				if (v == null)
				{
					// start time doesn't exist in db
					if (suggestedStartTime == null)
					{
						return null;
					}
					startAndInsertTime = new LeveldbTimelineStore.StartAndInsertTime(suggestedStartTime
						, Runtime.CurrentTimeMillis());
					// write suggested start time
					v = new byte[16];
					GenericObjectMapper.WriteReverseOrderedLong(suggestedStartTime, v, 0);
					GenericObjectMapper.WriteReverseOrderedLong(startAndInsertTime.insertTime, v, 8);
					WriteOptions writeOptions = new WriteOptions();
					writeOptions.Sync(true);
					db.Put(b, v, writeOptions);
				}
				else
				{
					// found start time in db, so ignore suggested start time
					startAndInsertTime = new LeveldbTimelineStore.StartAndInsertTime(GenericObjectMapper.ReadReverseOrderedLong
						(v, 0), GenericObjectMapper.ReadReverseOrderedLong(v, 8));
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			startTimeWriteCache[entity] = startAndInsertTime;
			startTimeReadCache[entity] = startAndInsertTime.startTime;
			return startAndInsertTime;
		}

		/// <summary>
		/// Creates a key for looking up the start time of a given entity,
		/// of the form START_TIME_LOOKUP_PREFIX + entity type + entity id.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateStartTimeLookupKey(string entityId, string entityType
			)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(StartTimeLookupPrefix).Add(entityType
				).Add(entityId).GetBytes();
		}

		/// <summary>
		/// Creates an entity marker, serializing ENTITY_ENTRY_PREFIX + entity type +
		/// revstarttime + entity id.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateEntityMarkerKey(string entityId, string entityType, byte
			[] revStartTime)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).GetBytesForLookup();
		}

		/// <summary>
		/// Creates an index entry for the given key of the form
		/// INDEXED_ENTRY_PREFIX + primaryfiltername + primaryfiltervalue + key.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] AddPrimaryFilterToKey(string primaryFilterName, object primaryFilterValue
			, byte[] key)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(IndexedEntryPrefix).Add(primaryFilterName
				).Add(GenericObjectMapper.Write(primaryFilterValue), true).Add(key).GetBytes();
		}

		/// <summary>
		/// Creates an event key, serializing ENTITY_ENTRY_PREFIX + entity type +
		/// revstarttime + entity id + EVENTS_COLUMN + reveventtimestamp + event type.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateEntityEventKey(string entityId, string entityType, byte
			[] revStartTime, byte[] revEventTimestamp, string eventType)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(EventsColumn).Add(revEventTimestamp).Add(eventType
				).GetBytes();
		}

		/// <summary>Creates an event object from the given key, offset, and value.</summary>
		/// <remarks>
		/// Creates an event object from the given key, offset, and value.  If the
		/// event type is not contained in the specified set of event types,
		/// returns null.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static TimelineEvent GetEntityEvent(ICollection<string> eventTypes, byte[]
			 key, int offset, byte[] value)
		{
			LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, offset);
			long ts = kp.GetNextLong();
			string tstype = kp.GetNextString();
			if (eventTypes == null || eventTypes.Contains(tstype))
			{
				TimelineEvent @event = new TimelineEvent();
				@event.SetTimestamp(ts);
				@event.SetEventType(tstype);
				object o = GenericObjectMapper.Read(value);
				if (o == null)
				{
					@event.SetEventInfo(null);
				}
				else
				{
					if (o is IDictionary)
					{
						IDictionary<string, object> m = (IDictionary<string, object>)o;
						@event.SetEventInfo(m);
					}
					else
					{
						throw new IOException("Couldn't deserialize event info map");
					}
				}
				return @event;
			}
			return null;
		}

		/// <summary>
		/// Creates a primary filter key, serializing ENTITY_ENTRY_PREFIX +
		/// entity type + revstarttime + entity id + PRIMARY_FILTERS_COLUMN + name +
		/// value.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreatePrimaryFilterKey(string entityId, string entityType, 
			byte[] revStartTime, string name, object value)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(PrimaryFiltersColumn).Add(name).Add(GenericObjectMapper
				.Write(value)).GetBytes();
		}

		/// <summary>
		/// Parses the primary filter from the given key at the given offset and
		/// adds it to the given entity.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void AddPrimaryFilter(TimelineEntity entity, byte[] key, int offset
			)
		{
			LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, offset);
			string name = kp.GetNextString();
			object value = GenericObjectMapper.Read(key, kp.GetOffset());
			entity.AddPrimaryFilter(name, value);
		}

		/// <summary>
		/// Creates an other info key, serializing ENTITY_ENTRY_PREFIX + entity type +
		/// revstarttime + entity id + OTHER_INFO_COLUMN + name.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateOtherInfoKey(string entityId, string entityType, byte
			[] revStartTime, string name)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(OtherInfoColumn).Add(name).GetBytes();
		}

		/// <summary>
		/// Creates a string representation of the byte array from the given offset
		/// to the end of the array (for parsing other info keys).
		/// </summary>
		private static string ParseRemainingKey(byte[] b, int offset)
		{
			return new string(b, offset, b.Length - offset, Sharpen.Extensions.GetEncoding("UTF-8"
				));
		}

		/// <summary>
		/// Creates a related entity key, serializing ENTITY_ENTRY_PREFIX +
		/// entity type + revstarttime + entity id + RELATED_ENTITIES_COLUMN +
		/// relatedentity type + relatedentity id.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateRelatedEntityKey(string entityId, string entityType, 
			byte[] revStartTime, string relatedEntityId, string relatedEntityType)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(RelatedEntitiesColumn).Add(relatedEntityType
				).Add(relatedEntityId).GetBytes();
		}

		/// <summary>
		/// Parses the related entity from the given key at the given offset and
		/// adds it to the given entity.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void AddRelatedEntity(TimelineEntity entity, byte[] key, int offset
			)
		{
			LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, offset);
			string type = kp.GetNextString();
			string id = kp.GetNextString();
			entity.AddRelatedEntity(type, id);
		}

		/// <summary>
		/// Creates a reverse related entity key, serializing ENTITY_ENTRY_PREFIX +
		/// entity type + revstarttime + entity id +
		/// INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN +
		/// relatedentity type + relatedentity id.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateReverseRelatedEntityKey(string entityId, string entityType
			, byte[] revStartTime, string relatedEntityId, string relatedEntityType)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(InvisibleReverseRelatedEntitiesColumn).Add
				(relatedEntityType).Add(relatedEntityId).GetBytes();
		}

		/// <summary>
		/// Creates a domain id key, serializing ENTITY_ENTRY_PREFIX +
		/// entity type + revstarttime + entity id + DOMAIN_ID_COLUMN.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateDomainIdKey(string entityId, string entityType, byte[]
			 revStartTime)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add(entityType
				).Add(revStartTime).Add(entityId).Add(DomainIdColumn).GetBytes();
		}

		/// <summary>
		/// Clears the cache to test reloading start times from leveldb (only for
		/// testing).
		/// </summary>
		[VisibleForTesting]
		internal virtual void ClearStartTimeCache()
		{
			startTimeWriteCache.Clear();
			startTimeReadCache.Clear();
		}

		[VisibleForTesting]
		internal static int GetStartTimeReadCacheSize(Configuration conf)
		{
			return conf.GetInt(YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbStartTimeReadCacheSize);
		}

		[VisibleForTesting]
		internal static int GetStartTimeWriteCacheSize(Configuration conf)
		{
			return conf.GetInt(YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize
				, YarnConfiguration.DefaultTimelineServiceLeveldbStartTimeWriteCacheSize);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual IList<string> GetEntityTypes()
		{
			LeveldbIterator iterator = null;
			try
			{
				iterator = GetDbIterator(false);
				IList<string> entityTypes = new AList<string>();
				iterator.Seek(EntityEntryPrefix);
				while (iterator.HasNext())
				{
					byte[] key = iterator.PeekNext().Key;
					if (key[0] != EntityEntryPrefix[0])
					{
						break;
					}
					LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, EntityEntryPrefix.Length
						);
					string entityType = kp.GetNextString();
					entityTypes.AddItem(entityType);
					byte[] lookupKey = LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix).Add
						(entityType).GetBytesForLookup();
					if (lookupKey[lookupKey.Length - 1] != unchecked((int)(0x0)))
					{
						throw new IOException("Found unexpected end byte in lookup key");
					}
					lookupKey[lookupKey.Length - 1] = unchecked((int)(0x1));
					iterator.Seek(lookupKey);
				}
				return entityTypes;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
		}

		/// <summary>
		/// Finds all keys in the db that have a given prefix and deletes them on
		/// the given write batch.
		/// </summary>
		private void DeleteKeysWithPrefix(WriteBatch writeBatch, byte[] prefix, LeveldbIterator
			 iterator)
		{
			for (iterator.Seek(prefix); iterator.HasNext(); iterator.Next())
			{
				byte[] key = iterator.PeekNext().Key;
				if (!LeveldbUtils.PrefixMatches(prefix, prefix.Length, key))
				{
					break;
				}
				writeBatch.Delete(key);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual bool DeleteNextEntity(string entityType, byte[] reverseTimestamp
			, LeveldbIterator iterator, LeveldbIterator pfIterator, bool seeked)
		{
			WriteBatch writeBatch = null;
			try
			{
				LeveldbUtils.KeyBuilder kb = LeveldbUtils.KeyBuilder.NewInstance().Add(EntityEntryPrefix
					).Add(entityType);
				byte[] typePrefix = kb.GetBytesForLookup();
				kb.Add(reverseTimestamp);
				if (!seeked)
				{
					iterator.Seek(kb.GetBytesForLookup());
				}
				if (!iterator.HasNext())
				{
					return false;
				}
				byte[] entityKey = iterator.PeekNext().Key;
				if (!LeveldbUtils.PrefixMatches(typePrefix, typePrefix.Length, entityKey))
				{
					return false;
				}
				// read the start time and entity id from the current key
				LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(entityKey, typePrefix.Length
					 + 8);
				string entityId = kp.GetNextString();
				int prefixlen = kp.GetOffset();
				byte[] deletePrefix = new byte[prefixlen];
				System.Array.Copy(entityKey, 0, deletePrefix, 0, prefixlen);
				writeBatch = db.CreateWriteBatch();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Deleting entity type:" + entityType + " id:" + entityId);
				}
				// remove start time from cache and db
				writeBatch.Delete(CreateStartTimeLookupKey(entityId, entityType));
				EntityIdentifier entityIdentifier = new EntityIdentifier(entityId, entityType);
				Sharpen.Collections.Remove(startTimeReadCache, entityIdentifier);
				Sharpen.Collections.Remove(startTimeWriteCache, entityIdentifier);
				// delete current entity
				for (; iterator.HasNext(); iterator.Next())
				{
					byte[] key = iterator.PeekNext().Key;
					if (!LeveldbUtils.PrefixMatches(entityKey, prefixlen, key))
					{
						break;
					}
					writeBatch.Delete(key);
					if (key.Length == prefixlen)
					{
						continue;
					}
					if (key[prefixlen] == PrimaryFiltersColumn[0])
					{
						kp = new LeveldbUtils.KeyParser(key, prefixlen + PrimaryFiltersColumn.Length);
						string name = kp.GetNextString();
						object value = GenericObjectMapper.Read(key, kp.GetOffset());
						DeleteKeysWithPrefix(writeBatch, AddPrimaryFilterToKey(name, value, deletePrefix)
							, pfIterator);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Deleting entity type:" + entityType + " id:" + entityId + " primary filter entry "
								 + name + " " + value);
						}
					}
					else
					{
						if (key[prefixlen] == RelatedEntitiesColumn[0])
						{
							kp = new LeveldbUtils.KeyParser(key, prefixlen + RelatedEntitiesColumn.Length);
							string type = kp.GetNextString();
							string id = kp.GetNextString();
							byte[] relatedEntityStartTime = GetStartTime(id, type);
							if (relatedEntityStartTime == null)
							{
								Log.Warn("Found no start time for " + "related entity " + id + " of type " + type
									 + " while " + "deleting " + entityId + " of type " + entityType);
								continue;
							}
							writeBatch.Delete(CreateReverseRelatedEntityKey(id, type, relatedEntityStartTime, 
								entityId, entityType));
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Deleting entity type:" + entityType + " id:" + entityId + " from invisible reverse related entity "
									 + "entry of type:" + type + " id:" + id);
							}
						}
						else
						{
							if (key[prefixlen] == InvisibleReverseRelatedEntitiesColumn[0])
							{
								kp = new LeveldbUtils.KeyParser(key, prefixlen + InvisibleReverseRelatedEntitiesColumn
									.Length);
								string type = kp.GetNextString();
								string id = kp.GetNextString();
								byte[] relatedEntityStartTime = GetStartTime(id, type);
								if (relatedEntityStartTime == null)
								{
									Log.Warn("Found no start time for reverse " + "related entity " + id + " of type "
										 + type + " while " + "deleting " + entityId + " of type " + entityType);
									continue;
								}
								writeBatch.Delete(CreateRelatedEntityKey(id, type, relatedEntityStartTime, entityId
									, entityType));
								if (Log.IsDebugEnabled())
								{
									Log.Debug("Deleting entity type:" + entityType + " id:" + entityId + " from related entity entry of type:"
										 + type + " id:" + id);
								}
							}
						}
					}
				}
				WriteOptions writeOptions = new WriteOptions();
				writeOptions.Sync(true);
				db.Write(writeBatch, writeOptions);
				return true;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, writeBatch);
			}
		}

		/// <summary>
		/// Discards entities with start timestamp less than or equal to the given
		/// timestamp.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void DiscardOldEntities(long timestamp)
		{
			byte[] reverseTimestamp = GenericObjectMapper.WriteReverseOrderedLong(timestamp);
			long totalCount = 0;
			long t1 = Runtime.CurrentTimeMillis();
			try
			{
				IList<string> entityTypes = GetEntityTypes();
				foreach (string entityType in entityTypes)
				{
					LeveldbIterator iterator = null;
					LeveldbIterator pfIterator = null;
					long typeCount = 0;
					try
					{
						deleteLock.WriteLock().Lock();
						iterator = GetDbIterator(false);
						pfIterator = GetDbIterator(false);
						if (deletionThread != null && deletionThread.IsInterrupted())
						{
							throw new Exception();
						}
						bool seeked = false;
						while (DeleteNextEntity(entityType, reverseTimestamp, iterator, pfIterator, seeked
							))
						{
							typeCount++;
							totalCount++;
							seeked = true;
							if (deletionThread != null && deletionThread.IsInterrupted())
							{
								throw new Exception();
							}
						}
					}
					catch (IOException e)
					{
						Log.Error("Got IOException while deleting entities for type " + entityType + ", continuing to next type"
							, e);
					}
					finally
					{
						IOUtils.Cleanup(Log, iterator, pfIterator);
						deleteLock.WriteLock().Unlock();
						if (typeCount > 0)
						{
							Log.Info("Deleted " + typeCount + " entities of type " + entityType);
						}
					}
				}
			}
			finally
			{
				long t2 = Runtime.CurrentTimeMillis();
				Log.Info("Discarded " + totalCount + " entities for timestamp " + timestamp + " and earlier in "
					 + (t2 - t1) / 1000.0 + " seconds");
			}
		}

		[VisibleForTesting]
		internal virtual LeveldbIterator GetDbIterator(bool fillCache)
		{
			ReadOptions readOptions = new ReadOptions();
			readOptions.FillCache(fillCache);
			return new LeveldbIterator(db, readOptions);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Version LoadVersion()
		{
			try
			{
				byte[] data = db.Get(JniDBFactory.Bytes(TimelineStoreVersionKey));
				// if version is not stored previously, treat it as CURRENT_VERSION_INFO.
				if (data == null || data.Length == 0)
				{
					return GetCurrentVersion();
				}
				Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
					(data));
				return version;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		// Only used for test
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void StoreVersion(Version state)
		{
			DbStoreVersion(state);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DbStoreVersion(Version state)
		{
			string key = TimelineStoreVersionKey;
			byte[] data = ((VersionPBImpl)state).GetProto().ToByteArray();
			try
			{
				db.Put(JniDBFactory.Bytes(key), data);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		internal virtual Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <summary>1) Versioning timeline store: major.minor.</summary>
		/// <remarks>
		/// 1) Versioning timeline store: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
		/// 2) Any incompatible change of TS-store is a major upgrade, and any
		/// compatible change of TS-store is a minor upgrade.
		/// 3) Within a minor upgrade, say 1.1 to 1.2:
		/// overwrite the version info and proceed as normal.
		/// 4) Within a major upgrade, say 1.2 to 2.0:
		/// throw exception and indicate user to use a separate upgrade tool to
		/// upgrade timeline store or remove incompatible old state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded timeline store version info " + loadedVersion);
			if (loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing timeline store version info " + GetCurrentVersion());
				DbStoreVersion(CurrentVersionInfo);
			}
			else
			{
				string incompatibleMessage = "Incompatible version for timeline store: expecting version "
					 + GetCurrentVersion() + ", but loading version " + loadedVersion;
				Log.Fatal(incompatibleMessage);
				throw new IOException(incompatibleMessage);
			}
		}

		//TODO: make data retention work with the domain data as well
		/// <exception cref="System.IO.IOException"/>
		public virtual void Put(TimelineDomain domain)
		{
			WriteBatch writeBatch = null;
			try
			{
				writeBatch = db.CreateWriteBatch();
				if (domain.GetId() == null || domain.GetId().Length == 0)
				{
					throw new ArgumentException("Domain doesn't have an ID");
				}
				if (domain.GetOwner() == null || domain.GetOwner().Length == 0)
				{
					throw new ArgumentException("Domain doesn't have an owner.");
				}
				// Write description
				byte[] domainEntryKey = CreateDomainEntryKey(domain.GetId(), DescriptionColumn);
				byte[] ownerLookupEntryKey = CreateOwnerLookupKey(domain.GetOwner(), domain.GetId
					(), DescriptionColumn);
				if (domain.GetDescription() != null)
				{
					writeBatch.Put(domainEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetDescription
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
					writeBatch.Put(ownerLookupEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetDescription
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
				}
				else
				{
					writeBatch.Put(domainEntryKey, EmptyBytes);
					writeBatch.Put(ownerLookupEntryKey, EmptyBytes);
				}
				// Write owner
				domainEntryKey = CreateDomainEntryKey(domain.GetId(), OwnerColumn);
				ownerLookupEntryKey = CreateOwnerLookupKey(domain.GetOwner(), domain.GetId(), OwnerColumn
					);
				// Null check for owner is done before
				writeBatch.Put(domainEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetOwner(
					), Sharpen.Extensions.GetEncoding("UTF-8")));
				writeBatch.Put(ownerLookupEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetOwner
					(), Sharpen.Extensions.GetEncoding("UTF-8")));
				// Write readers
				domainEntryKey = CreateDomainEntryKey(domain.GetId(), ReaderColumn);
				ownerLookupEntryKey = CreateOwnerLookupKey(domain.GetOwner(), domain.GetId(), ReaderColumn
					);
				if (domain.GetReaders() != null && domain.GetReaders().Length > 0)
				{
					writeBatch.Put(domainEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetReaders
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
					writeBatch.Put(ownerLookupEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetReaders
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
				}
				else
				{
					writeBatch.Put(domainEntryKey, EmptyBytes);
					writeBatch.Put(ownerLookupEntryKey, EmptyBytes);
				}
				// Write writers
				domainEntryKey = CreateDomainEntryKey(domain.GetId(), WriterColumn);
				ownerLookupEntryKey = CreateOwnerLookupKey(domain.GetOwner(), domain.GetId(), WriterColumn
					);
				if (domain.GetWriters() != null && domain.GetWriters().Length > 0)
				{
					writeBatch.Put(domainEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetWriters
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
					writeBatch.Put(ownerLookupEntryKey, Sharpen.Runtime.GetBytesForString(domain.GetWriters
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
				}
				else
				{
					writeBatch.Put(domainEntryKey, EmptyBytes);
					writeBatch.Put(ownerLookupEntryKey, EmptyBytes);
				}
				// Write creation time and modification time
				// We put both timestamps together because they are always retrieved
				// together, and store them in the same way as we did for the entity's
				// start time and insert time.
				domainEntryKey = CreateDomainEntryKey(domain.GetId(), TimestampColumn);
				ownerLookupEntryKey = CreateOwnerLookupKey(domain.GetOwner(), domain.GetId(), TimestampColumn
					);
				long currentTimestamp = Runtime.CurrentTimeMillis();
				byte[] timestamps = db.Get(domainEntryKey);
				if (timestamps == null)
				{
					timestamps = new byte[16];
					GenericObjectMapper.WriteReverseOrderedLong(currentTimestamp, timestamps, 0);
					GenericObjectMapper.WriteReverseOrderedLong(currentTimestamp, timestamps, 8);
				}
				else
				{
					GenericObjectMapper.WriteReverseOrderedLong(currentTimestamp, timestamps, 8);
				}
				writeBatch.Put(domainEntryKey, timestamps);
				writeBatch.Put(ownerLookupEntryKey, timestamps);
				db.Write(writeBatch);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, writeBatch);
			}
		}

		/// <summary>
		/// Creates a domain entity key with column name suffix,
		/// of the form DOMAIN_ENTRY_PREFIX + domain id + column name.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateDomainEntryKey(string domainId, byte[] columnName)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(DomainEntryPrefix).Add(domainId)
				.Add(columnName).GetBytes();
		}

		/// <summary>
		/// Creates an owner lookup key with column name suffix,
		/// of the form OWNER_LOOKUP_PREFIX + owner + domain id + column name.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateOwnerLookupKey(string owner, string domainId, byte[] 
			columnName)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(OwnerLookupPrefix).Add(owner).Add
				(domainId).Add(columnName).GetBytes();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomain GetDomain(string domainId)
		{
			LeveldbIterator iterator = null;
			try
			{
				byte[] prefix = LeveldbUtils.KeyBuilder.NewInstance().Add(DomainEntryPrefix).Add(
					domainId).GetBytesForLookup();
				iterator = new LeveldbIterator(db);
				iterator.Seek(prefix);
				return GetTimelineDomain(iterator, domainId, prefix);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomains GetDomains(string owner)
		{
			LeveldbIterator iterator = null;
			try
			{
				byte[] prefix = LeveldbUtils.KeyBuilder.NewInstance().Add(OwnerLookupPrefix).Add(
					owner).GetBytesForLookup();
				IList<TimelineDomain> domains = new AList<TimelineDomain>();
				for (iterator = new LeveldbIterator(db), iterator.Seek(prefix); iterator.HasNext(
					); )
				{
					byte[] key = iterator.PeekNext().Key;
					if (!LeveldbUtils.PrefixMatches(prefix, prefix.Length, key))
					{
						break;
					}
					// Iterator to parse the rows of an individual domain
					LeveldbUtils.KeyParser kp = new LeveldbUtils.KeyParser(key, prefix.Length);
					string domainId = kp.GetNextString();
					byte[] prefixExt = LeveldbUtils.KeyBuilder.NewInstance().Add(OwnerLookupPrefix).Add
						(owner).Add(domainId).GetBytesForLookup();
					TimelineDomain domainToReturn = GetTimelineDomain(iterator, domainId, prefixExt);
					if (domainToReturn != null)
					{
						domains.AddItem(domainToReturn);
					}
				}
				// Sort the domains to return
				domains.Sort(new _IComparer_1733());
				TimelineDomains domainsToReturn = new TimelineDomains();
				domainsToReturn.AddDomains(domains);
				return domainsToReturn;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
		}

		private sealed class _IComparer_1733 : IComparer<TimelineDomain>
		{
			public _IComparer_1733()
			{
			}

			public int Compare(TimelineDomain domain1, TimelineDomain domain2)
			{
				int result = domain2.GetCreatedTime().CompareTo(domain1.GetCreatedTime());
				if (result == 0)
				{
					return domain2.GetModifiedTime().CompareTo(domain1.GetModifiedTime());
				}
				else
				{
					return result;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static TimelineDomain GetTimelineDomain(LeveldbIterator iterator, string 
			domainId, byte[] prefix)
		{
			// Iterate over all the rows whose key starts with prefix to retrieve the
			// domain information.
			TimelineDomain domain = new TimelineDomain();
			domain.SetId(domainId);
			bool noRows = true;
			for (; iterator.HasNext(); iterator.Next())
			{
				byte[] key = iterator.PeekNext().Key;
				if (!LeveldbUtils.PrefixMatches(prefix, prefix.Length, key))
				{
					break;
				}
				if (noRows)
				{
					noRows = false;
				}
				byte[] value = iterator.PeekNext().Value;
				if (value != null && value.Length > 0)
				{
					if (key[prefix.Length] == DescriptionColumn[0])
					{
						domain.SetDescription(new string(value, Sharpen.Extensions.GetEncoding("UTF-8")));
					}
					else
					{
						if (key[prefix.Length] == OwnerColumn[0])
						{
							domain.SetOwner(new string(value, Sharpen.Extensions.GetEncoding("UTF-8")));
						}
						else
						{
							if (key[prefix.Length] == ReaderColumn[0])
							{
								domain.SetReaders(new string(value, Sharpen.Extensions.GetEncoding("UTF-8")));
							}
							else
							{
								if (key[prefix.Length] == WriterColumn[0])
								{
									domain.SetWriters(new string(value, Sharpen.Extensions.GetEncoding("UTF-8")));
								}
								else
								{
									if (key[prefix.Length] == TimestampColumn[0])
									{
										domain.SetCreatedTime(GenericObjectMapper.ReadReverseOrderedLong(value, 0));
										domain.SetModifiedTime(GenericObjectMapper.ReadReverseOrderedLong(value, 8));
									}
									else
									{
										Log.Error("Unrecognized domain column: " + key[prefix.Length]);
									}
								}
							}
						}
					}
				}
			}
			if (noRows)
			{
				return null;
			}
			else
			{
				return domain;
			}
		}
	}
}
