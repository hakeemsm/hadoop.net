using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Server.Tasktracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	internal class IndexCache
	{
		private readonly JobConf conf;

		private readonly int totalMemoryAllowed;

		private AtomicInteger totalMemoryUsed = new AtomicInteger();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.IndexCache
			));

		private readonly ConcurrentHashMap<string, IndexCache.IndexInformation> cache = new 
			ConcurrentHashMap<string, IndexCache.IndexInformation>();

		private readonly LinkedBlockingQueue<string> queue = new LinkedBlockingQueue<string
			>();

		public IndexCache(JobConf conf)
		{
			this.conf = conf;
			totalMemoryAllowed = conf.GetInt(TTConfig.TtIndexCache, 10) * 1024 * 1024;
			Log.Info("IndexCache created with max memory = " + totalMemoryAllowed);
		}

		/// <summary>This method gets the index information for the given mapId and reduce.</summary>
		/// <remarks>
		/// This method gets the index information for the given mapId and reduce.
		/// It reads the index file into cache if it is not already present.
		/// </remarks>
		/// <param name="mapId"/>
		/// <param name="reduce"/>
		/// <param name="fileName">
		/// The file to read the index information from if it is not
		/// already present in the cache
		/// </param>
		/// <param name="expectedIndexOwner">The expected owner of the index file</param>
		/// <returns>The Index Information</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IndexRecord GetIndexInformation(string mapId, int reduce, Path fileName
			, string expectedIndexOwner)
		{
			IndexCache.IndexInformation info = cache[mapId];
			if (info == null)
			{
				info = ReadIndexFileToCache(fileName, mapId, expectedIndexOwner);
			}
			else
			{
				lock (info)
				{
					while (IsUnderConstruction(info))
					{
						try
						{
							Sharpen.Runtime.Wait(info);
						}
						catch (Exception e)
						{
							throw new IOException("Interrupted waiting for construction", e);
						}
					}
				}
				Log.Debug("IndexCache HIT: MapId " + mapId + " found");
			}
			if (info.mapSpillRecord.Size() == 0 || info.mapSpillRecord.Size() <= reduce)
			{
				throw new IOException("Invalid request " + " Map Id = " + mapId + " Reducer = " +
					 reduce + " Index Info Length = " + info.mapSpillRecord.Size());
			}
			return info.mapSpillRecord.GetIndex(reduce);
		}

		private bool IsUnderConstruction(IndexCache.IndexInformation info)
		{
			lock (info)
			{
				return (null == info.mapSpillRecord);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private IndexCache.IndexInformation ReadIndexFileToCache(Path indexFileName, string
			 mapId, string expectedIndexOwner)
		{
			IndexCache.IndexInformation info;
			IndexCache.IndexInformation newInd = new IndexCache.IndexInformation();
			if ((info = cache.PutIfAbsent(mapId, newInd)) != null)
			{
				lock (info)
				{
					while (IsUnderConstruction(info))
					{
						try
						{
							Sharpen.Runtime.Wait(info);
						}
						catch (Exception e)
						{
							throw new IOException("Interrupted waiting for construction", e);
						}
					}
				}
				Log.Debug("IndexCache HIT: MapId " + mapId + " found");
				return info;
			}
			Log.Debug("IndexCache MISS: MapId " + mapId + " not found");
			SpillRecord tmp = null;
			try
			{
				tmp = new SpillRecord(indexFileName, conf, expectedIndexOwner);
			}
			catch (Exception e)
			{
				tmp = new SpillRecord(0);
				Sharpen.Collections.Remove(cache, mapId);
				throw new IOException("Error Reading IndexFile", e);
			}
			finally
			{
				lock (newInd)
				{
					newInd.mapSpillRecord = tmp;
					Sharpen.Runtime.NotifyAll(newInd);
				}
			}
			queue.AddItem(mapId);
			if (totalMemoryUsed.AddAndGet(newInd.GetSize()) > totalMemoryAllowed)
			{
				FreeIndexInformation();
			}
			return newInd;
		}

		/// <summary>
		/// This method removes the map from the cache if index information for this
		/// map is loaded(size&gt;0), index information entry in cache will not be
		/// removed if it is in the loading phrase(size=0), this prevents corruption
		/// of totalMemoryUsed.
		/// </summary>
		/// <remarks>
		/// This method removes the map from the cache if index information for this
		/// map is loaded(size&gt;0), index information entry in cache will not be
		/// removed if it is in the loading phrase(size=0), this prevents corruption
		/// of totalMemoryUsed. It should be called when a map output on this tracker
		/// is discarded.
		/// </remarks>
		/// <param name="mapId">The taskID of this map.</param>
		public virtual void RemoveMap(string mapId)
		{
			IndexCache.IndexInformation info = cache[mapId];
			if (info == null || IsUnderConstruction(info))
			{
				return;
			}
			info = Sharpen.Collections.Remove(cache, mapId);
			if (info != null)
			{
				totalMemoryUsed.AddAndGet(-info.GetSize());
				if (!queue.Remove(mapId))
				{
					Log.Warn("Map ID" + mapId + " not found in queue!!");
				}
			}
			else
			{
				Log.Info("Map ID " + mapId + " not found in cache");
			}
		}

		/// <summary>This method checks if cache and totolMemoryUsed is consistent.</summary>
		/// <remarks>
		/// This method checks if cache and totolMemoryUsed is consistent.
		/// It is only used for unit test.
		/// </remarks>
		/// <returns>True if cache and totolMemoryUsed is consistent</returns>
		internal virtual bool CheckTotalMemoryUsed()
		{
			int totalSize = 0;
			foreach (IndexCache.IndexInformation info in cache.Values)
			{
				totalSize += info.GetSize();
			}
			return totalSize == totalMemoryUsed.Get();
		}

		/// <summary>Bring memory usage below totalMemoryAllowed.</summary>
		private void FreeIndexInformation()
		{
			lock (this)
			{
				while (totalMemoryUsed.Get() > totalMemoryAllowed)
				{
					string s = queue.Remove();
					IndexCache.IndexInformation info = Sharpen.Collections.Remove(cache, s);
					if (info != null)
					{
						totalMemoryUsed.AddAndGet(-info.GetSize());
					}
				}
			}
		}

		private class IndexInformation
		{
			internal SpillRecord mapSpillRecord;

			internal virtual int GetSize()
			{
				return mapSpillRecord == null ? 0 : mapSpillRecord.Size() * MapTask.MapOutputIndexRecordLength;
			}
		}
	}
}
