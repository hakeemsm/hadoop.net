using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Client
{
	/// <summary>The public API for performing administrative functions on HDFS.</summary>
	/// <remarks>
	/// The public API for performing administrative functions on HDFS. Those writing
	/// applications against HDFS should prefer this interface to directly accessing
	/// functionality in DistributedFileSystem or DFSClient.
	/// Note that this is distinct from the similarly-named
	/// <see cref="Org.Apache.Hadoop.Hdfs.Tools.DFSAdmin"/>
	/// , which
	/// is a class that provides the functionality for the CLI `hdfs dfsadmin ...'
	/// commands.
	/// </remarks>
	public class HdfsAdmin
	{
		private DistributedFileSystem dfs;

		/// <summary>Create a new HdfsAdmin client.</summary>
		/// <param name="uri">the unique URI of the HDFS file system to administer</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException">in the event the file system could not be created
		/// 	</exception>
		public HdfsAdmin(URI uri, Configuration conf)
		{
			FileSystem fs = FileSystem.Get(uri, conf);
			if (!(fs is DistributedFileSystem))
			{
				throw new ArgumentException("'" + uri + "' is not an HDFS URI.");
			}
			else
			{
				dfs = (DistributedFileSystem)fs;
			}
		}

		/// <summary>
		/// Set the namespace quota (count of files, directories, and sym links) for a
		/// directory.
		/// </summary>
		/// <param name="src">the path to set the quota for</param>
		/// <param name="quota">the value to set for the quota</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void SetQuota(Path src, long quota)
		{
			dfs.SetQuota(src, quota, HdfsConstants.QuotaDontSet);
		}

		/// <summary>
		/// Clear the namespace quota (count of files, directories and sym links) for a
		/// directory.
		/// </summary>
		/// <param name="src">the path to clear the quota of</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void ClearQuota(Path src)
		{
			dfs.SetQuota(src, HdfsConstants.QuotaReset, HdfsConstants.QuotaDontSet);
		}

		/// <summary>Set the storage space quota (size of files) for a directory.</summary>
		/// <remarks>
		/// Set the storage space quota (size of files) for a directory. Note that
		/// directories and sym links do not occupy storage space.
		/// </remarks>
		/// <param name="src">the path to set the space quota of</param>
		/// <param name="spaceQuota">the value to set for the space quota</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void SetSpaceQuota(Path src, long spaceQuota)
		{
			dfs.SetQuota(src, HdfsConstants.QuotaDontSet, spaceQuota);
		}

		/// <summary>Clear the storage space quota (size of files) for a directory.</summary>
		/// <remarks>
		/// Clear the storage space quota (size of files) for a directory. Note that
		/// directories and sym links do not occupy storage space.
		/// </remarks>
		/// <param name="src">the path to clear the space quota of</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void ClearSpaceQuota(Path src)
		{
			dfs.SetQuota(src, HdfsConstants.QuotaDontSet, HdfsConstants.QuotaReset);
		}

		/// <summary>Set the quota by storage type for a directory.</summary>
		/// <remarks>
		/// Set the quota by storage type for a directory. Note that
		/// directories and sym links do not occupy storage type quota.
		/// </remarks>
		/// <param name="src">the target directory to set the quota by storage type</param>
		/// <param name="type">the storage type to set for quota by storage type</param>
		/// <param name="quota">the value to set for quota by storage type</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void SetQuotaByStorageType(Path src, StorageType type, long quota)
		{
			dfs.SetQuotaByStorageType(src, type, quota);
		}

		/// <summary>Clear the space quota by storage type for a directory.</summary>
		/// <remarks>
		/// Clear the space quota by storage type for a directory. Note that
		/// directories and sym links do not occupy storage type quota.
		/// </remarks>
		/// <param name="src">the target directory to clear the quota by storage type</param>
		/// <param name="type">the storage type to clear for quota by storage type</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public virtual void ClearQuotaByStorageType(Path src, StorageType type)
		{
			dfs.SetQuotaByStorageType(src, type, HdfsConstants.QuotaReset);
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <param name="path">The path of the directory where snapshots will be taken.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AllowSnapshot(Path path)
		{
			dfs.AllowSnapshot(path);
		}

		/// <summary>Disallow snapshot on a directory.</summary>
		/// <param name="path">The path of the snapshottable directory.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void DisallowSnapshot(Path path)
		{
			dfs.DisallowSnapshot(path);
		}

		/// <summary>Add a new CacheDirectiveInfo.</summary>
		/// <param name="info">Information about a directive to add.</param>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <returns>the ID of the directive that was created.</returns>
		/// <exception cref="System.IO.IOException">if the directive could not be added</exception>
		public virtual long AddCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag>
			 flags)
		{
			return dfs.AddCacheDirective(info, flags);
		}

		/// <summary>Modify a CacheDirective.</summary>
		/// <param name="info">
		/// Information about the directive to modify. You must set the ID
		/// to indicate which CacheDirective you want to modify.
		/// </param>
		/// <param name="flags">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.CacheFlag"/>
		/// s to use for this operation.
		/// </param>
		/// <exception cref="System.IO.IOException">if the directive could not be modified</exception>
		public virtual void ModifyCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag
			> flags)
		{
			dfs.ModifyCacheDirective(info, flags);
		}

		/// <summary>Remove a CacheDirective.</summary>
		/// <param name="id">identifier of the CacheDirectiveInfo to remove</param>
		/// <exception cref="System.IO.IOException">if the directive could not be removed</exception>
		public virtual void RemoveCacheDirective(long id)
		{
			dfs.RemoveCacheDirective(id);
		}

		/// <summary>List cache directives.</summary>
		/// <remarks>List cache directives. Incrementally fetches results from the server.</remarks>
		/// <param name="filter">
		/// Filter parameters to use when listing the directives, null to
		/// list all directives visible to us.
		/// </param>
		/// <returns>A RemoteIterator which returns CacheDirectiveInfo objects.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<CacheDirectiveEntry> ListCacheDirectives(CacheDirectiveInfo
			 filter)
		{
			return dfs.ListCacheDirectives(filter);
		}

		/// <summary>Add a cache pool.</summary>
		/// <param name="info">The request to add a cache pool.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If the request could not be completed.
		/// </exception>
		public virtual void AddCachePool(CachePoolInfo info)
		{
			dfs.AddCachePool(info);
		}

		/// <summary>Modify an existing cache pool.</summary>
		/// <param name="info">The request to modify a cache pool.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// If the request could not be completed.
		/// </exception>
		public virtual void ModifyCachePool(CachePoolInfo info)
		{
			dfs.ModifyCachePool(info);
		}

		/// <summary>Remove a cache pool.</summary>
		/// <param name="poolName">Name of the cache pool to remove.</param>
		/// <exception cref="System.IO.IOException">
		/// 
		/// if the cache pool did not exist, or could not be removed.
		/// </exception>
		public virtual void RemoveCachePool(string poolName)
		{
			dfs.RemoveCachePool(poolName);
		}

		/// <summary>List all cache pools.</summary>
		/// <returns>
		/// A remote iterator from which you can get CachePoolEntry objects.
		/// Requests will be made as needed.
		/// </returns>
		/// <exception cref="System.IO.IOException">If there was an error listing cache pools.
		/// 	</exception>
		public virtual RemoteIterator<CachePoolEntry> ListCachePools()
		{
			return dfs.ListCachePools();
		}

		/// <summary>
		/// Create an encryption zone rooted at an empty existing directory, using the
		/// specified encryption key.
		/// </summary>
		/// <remarks>
		/// Create an encryption zone rooted at an empty existing directory, using the
		/// specified encryption key. An encryption zone has an associated encryption
		/// key used when reading and writing files within the zone.
		/// </remarks>
		/// <param name="path">
		/// The path of the root of the encryption zone. Must refer to
		/// an empty, existing directory.
		/// </param>
		/// <param name="keyName">Name of key available at the KeyProvider.</param>
		/// <exception cref="System.IO.IOException">if there was a general IO exception</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the caller does not have access to path
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if the path does not exist</exception>
		public virtual void CreateEncryptionZone(Path path, string keyName)
		{
			dfs.CreateEncryptionZone(path, keyName);
		}

		/// <summary>Get the path of the encryption zone for a given file or directory.</summary>
		/// <param name="path">The path to get the ez for.</param>
		/// <returns>The EncryptionZone of the ez, or null if path is not in an ez.</returns>
		/// <exception cref="System.IO.IOException">if there was a general IO exception</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the caller does not have access to path
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if the path does not exist</exception>
		public virtual EncryptionZone GetEncryptionZoneForPath(Path path)
		{
			return dfs.GetEZForPath(path);
		}

		/// <summary>
		/// Returns a RemoteIterator which can be used to list the encryption zones
		/// in HDFS.
		/// </summary>
		/// <remarks>
		/// Returns a RemoteIterator which can be used to list the encryption zones
		/// in HDFS. For large numbers of encryption zones, the iterator will fetch
		/// the list of zones in a number of small batches.
		/// <p/>
		/// Since the list is fetched in batches, it does not represent a
		/// consistent snapshot of the entire list of encryption zones.
		/// <p/>
		/// This method can only be called by HDFS superusers.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual RemoteIterator<EncryptionZone> ListEncryptionZones()
		{
			return dfs.ListEncryptionZones();
		}

		/// <summary>Exposes a stream of namesystem events.</summary>
		/// <remarks>
		/// Exposes a stream of namesystem events. Only events occurring after the
		/// stream is created are available.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSInotifyEventInputStream"/>
		/// for information on stream usage.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Inotify.Event"/>
		/// for information on the available events.
		/// <p/>
		/// Inotify users may want to tune the following HDFS parameters to
		/// ensure that enough extra HDFS edits are saved to support inotify clients
		/// that fall behind the current state of the namespace while reading events.
		/// The default parameter values should generally be reasonable. If edits are
		/// deleted before their corresponding events can be read, clients will see a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		/// on
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSInotifyEventInputStream"/>
		/// method calls.
		/// It should generally be sufficient to tune these parameters:
		/// dfs.namenode.num.extra.edits.retained
		/// dfs.namenode.max.extra.edits.segments.retained
		/// Parameters that affect the number of created segments and the number of
		/// edits that are considered necessary, i.e. do not count towards the
		/// dfs.namenode.num.extra.edits.retained quota):
		/// dfs.namenode.checkpoint.period
		/// dfs.namenode.checkpoint.txns
		/// dfs.namenode.num.checkpoints.retained
		/// dfs.ha.log-roll.period
		/// <p/>
		/// It is recommended that local journaling be configured
		/// (dfs.namenode.edits.dir) for inotify (in addition to a shared journal)
		/// so that edit transfers from the shared journal can be avoided.
		/// </remarks>
		/// <exception cref="System.IO.IOException">If there was an error obtaining the stream.
		/// 	</exception>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream()
		{
			return dfs.GetInotifyEventStream();
		}

		/// <summary>
		/// A version of
		/// <see cref="GetInotifyEventStream()"/>
		/// meant for advanced
		/// users who are aware of HDFS edits up to lastReadTxid (e.g. because they
		/// have access to an FSImage inclusive of lastReadTxid) and only want to read
		/// events after this point.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DFSInotifyEventInputStream GetInotifyEventStream(long lastReadTxid
			)
		{
			return dfs.GetInotifyEventStream(lastReadTxid);
		}

		/// <summary>Set the source path to the specified storage policy.</summary>
		/// <param name="src">The source path referring to either a directory or a file.</param>
		/// <param name="policyName">The name of the storage policy.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetStoragePolicy(Path src, string policyName)
		{
			dfs.SetStoragePolicy(src, policyName);
		}
	}
}
