using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Filecache
{
	/// <summary>Manages internal configuration of the cache by the client for job submission.
	/// 	</summary>
	public class ClientDistributedCacheManager
	{
		/// <summary>
		/// Determines timestamps of files to be cached, and stores those
		/// in the configuration.
		/// </summary>
		/// <remarks>
		/// Determines timestamps of files to be cached, and stores those
		/// in the configuration. Determines the visibilities of the distributed cache
		/// files and archives. The visibility of a cache path is "public" if the leaf
		/// component has READ permissions for others, and the parent subdirs have
		/// EXECUTE permissions for others.
		/// This is an internal method!
		/// </remarks>
		/// <param name="job"/>
		/// <exception cref="System.IO.IOException"/>
		public static void DetermineTimestampsAndCacheVisibilities(Configuration job)
		{
			IDictionary<URI, FileStatus> statCache = new Dictionary<URI, FileStatus>();
			DetermineTimestamps(job, statCache);
			DetermineCacheVisibilities(job, statCache);
		}

		/// <summary>
		/// Determines timestamps of files to be cached, and stores those
		/// in the configuration.
		/// </summary>
		/// <remarks>
		/// Determines timestamps of files to be cached, and stores those
		/// in the configuration.  This is intended to be used internally by JobClient
		/// after all cache files have been added.
		/// This is an internal method!
		/// </remarks>
		/// <param name="job">Configuration of a job.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void DetermineTimestamps(Configuration job, IDictionary<URI, FileStatus
			> statCache)
		{
			URI[] tarchives = DistributedCache.GetCacheArchives(job);
			if (tarchives != null)
			{
				FileStatus status = GetFileStatus(job, tarchives[0], statCache);
				StringBuilder archiveFileSizes = new StringBuilder(status.GetLen().ToString());
				StringBuilder archiveTimestamps = new StringBuilder(status.GetModificationTime().
					ToString());
				for (int i = 1; i < tarchives.Length; i++)
				{
					status = GetFileStatus(job, tarchives[i], statCache);
					archiveFileSizes.Append(",");
					archiveFileSizes.Append(status.GetLen().ToString());
					archiveTimestamps.Append(",");
					archiveTimestamps.Append(status.GetModificationTime().ToString());
				}
				job.Set(MRJobConfig.CacheArchivesSizes, archiveFileSizes.ToString());
				SetArchiveTimestamps(job, archiveTimestamps.ToString());
			}
			URI[] tfiles = DistributedCache.GetCacheFiles(job);
			if (tfiles != null)
			{
				FileStatus status = GetFileStatus(job, tfiles[0], statCache);
				StringBuilder fileSizes = new StringBuilder(status.GetLen().ToString());
				StringBuilder fileTimestamps = new StringBuilder(status.GetModificationTime().ToString
					());
				for (int i = 1; i < tfiles.Length; i++)
				{
					status = GetFileStatus(job, tfiles[i], statCache);
					fileSizes.Append(",");
					fileSizes.Append(status.GetLen().ToString());
					fileTimestamps.Append(",");
					fileTimestamps.Append(status.GetModificationTime().ToString());
				}
				job.Set(MRJobConfig.CacheFilesSizes, fileSizes.ToString());
				SetFileTimestamps(job, fileTimestamps.ToString());
			}
		}

		/// <summary>For each archive or cache file - get the corresponding delegation token</summary>
		/// <param name="job"/>
		/// <param name="credentials"/>
		/// <exception cref="System.IO.IOException"/>
		public static void GetDelegationTokens(Configuration job, Credentials credentials
			)
		{
			URI[] tarchives = DistributedCache.GetCacheArchives(job);
			URI[] tfiles = DistributedCache.GetCacheFiles(job);
			int size = (tarchives != null ? tarchives.Length : 0) + (tfiles != null ? tfiles.
				Length : 0);
			Path[] ps = new Path[size];
			int i = 0;
			if (tarchives != null)
			{
				for (i = 0; i < tarchives.Length; i++)
				{
					ps[i] = new Path(tarchives[i].ToString());
				}
			}
			if (tfiles != null)
			{
				for (int j = 0; j < tfiles.Length; j++)
				{
					ps[i + j] = new Path(tfiles[j].ToString());
				}
			}
			TokenCache.ObtainTokensForNamenodes(credentials, ps, job);
		}

		/// <summary>
		/// Determines the visibilities of the distributed cache files and
		/// archives.
		/// </summary>
		/// <remarks>
		/// Determines the visibilities of the distributed cache files and
		/// archives. The visibility of a cache path is "public" if the leaf component
		/// has READ permissions for others, and the parent subdirs have
		/// EXECUTE permissions for others
		/// </remarks>
		/// <param name="job"/>
		/// <exception cref="System.IO.IOException"/>
		public static void DetermineCacheVisibilities(Configuration job, IDictionary<URI, 
			FileStatus> statCache)
		{
			URI[] tarchives = DistributedCache.GetCacheArchives(job);
			if (tarchives != null)
			{
				StringBuilder archiveVisibilities = new StringBuilder(IsPublic(job, tarchives[0], 
					statCache).ToString());
				for (int i = 1; i < tarchives.Length; i++)
				{
					archiveVisibilities.Append(",");
					archiveVisibilities.Append(IsPublic(job, tarchives[i], statCache).ToString());
				}
				SetArchiveVisibilities(job, archiveVisibilities.ToString());
			}
			URI[] tfiles = DistributedCache.GetCacheFiles(job);
			if (tfiles != null)
			{
				StringBuilder fileVisibilities = new StringBuilder(IsPublic(job, tfiles[0], statCache
					).ToString());
				for (int i = 1; i < tfiles.Length; i++)
				{
					fileVisibilities.Append(",");
					fileVisibilities.Append(IsPublic(job, tfiles[i], statCache).ToString());
				}
				SetFileVisibilities(job, fileVisibilities.ToString());
			}
		}

		/// <summary>
		/// This is to check the public/private visibility of the archives to be
		/// localized.
		/// </summary>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="booleans">
		/// comma separated list of booleans (true - public)
		/// The order should be the same as the order in which the archives are added.
		/// </param>
		internal static void SetArchiveVisibilities(Configuration conf, string booleans)
		{
			conf.Set(MRJobConfig.CacheArchivesVisibilities, booleans);
		}

		/// <summary>This is to check the public/private visibility of the files to be localized
		/// 	</summary>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="booleans">
		/// comma separated list of booleans (true - public)
		/// The order should be the same as the order in which the files are added.
		/// </param>
		internal static void SetFileVisibilities(Configuration conf, string booleans)
		{
			conf.Set(MRJobConfig.CacheFileVisibilities, booleans);
		}

		/// <summary>This is to check the timestamp of the archives to be localized.</summary>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="timestamps">
		/// comma separated list of timestamps of archives.
		/// The order should be the same as the order in which the archives are added.
		/// </param>
		internal static void SetArchiveTimestamps(Configuration conf, string timestamps)
		{
			conf.Set(MRJobConfig.CacheArchivesTimestamps, timestamps);
		}

		/// <summary>This is to check the timestamp of the files to be localized.</summary>
		/// <param name="conf">Configuration which stores the timestamp's</param>
		/// <param name="timestamps">
		/// comma separated list of timestamps of files.
		/// The order should be the same as the order in which the files are added.
		/// </param>
		internal static void SetFileTimestamps(Configuration conf, string timestamps)
		{
			conf.Set(MRJobConfig.CacheFileTimestamps, timestamps);
		}

		/// <summary>Gets the file status for the given URI.</summary>
		/// <remarks>
		/// Gets the file status for the given URI.  If the URI is in the cache,
		/// returns it.  Otherwise, fetches it and adds it to the cache.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static FileStatus GetFileStatus(Configuration job, URI uri, IDictionary<URI
			, FileStatus> statCache)
		{
			FileSystem fileSystem = FileSystem.Get(uri, job);
			return GetFileStatus(fileSystem, uri, statCache);
		}

		/// <summary>
		/// Returns a boolean to denote whether a cache file is visible to all(public)
		/// or not
		/// </summary>
		/// <param name="conf"/>
		/// <param name="uri"/>
		/// <returns>true if the path in the uri is visible to all, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static bool IsPublic(Configuration conf, URI uri, IDictionary<URI, FileStatus
			> statCache)
		{
			FileSystem fs = FileSystem.Get(uri, conf);
			Path current = new Path(uri.GetPath());
			//the leaf level file should be readable by others
			if (!CheckPermissionOfOther(fs, current, FsAction.Read, statCache))
			{
				return false;
			}
			return AncestorsHaveExecutePermissions(fs, current.GetParent(), statCache);
		}

		/// <summary>
		/// Returns true if all ancestors of the specified path have the 'execute'
		/// permission set for all users (i.e.
		/// </summary>
		/// <remarks>
		/// Returns true if all ancestors of the specified path have the 'execute'
		/// permission set for all users (i.e. that other users can traverse
		/// the directory heirarchy to the given path)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static bool AncestorsHaveExecutePermissions(FileSystem fs, Path path, IDictionary
			<URI, FileStatus> statCache)
		{
			Path current = path;
			while (current != null)
			{
				//the subdirs in the path should have execute permissions for others
				if (!CheckPermissionOfOther(fs, current, FsAction.Execute, statCache))
				{
					return false;
				}
				current = current.GetParent();
			}
			return true;
		}

		/// <summary>
		/// Checks for a given path whether the Other permissions on it
		/// imply the permission in the passed FsAction
		/// </summary>
		/// <param name="fs"/>
		/// <param name="path"/>
		/// <param name="action"/>
		/// <returns>true if the path in the uri is visible to all, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		private static bool CheckPermissionOfOther(FileSystem fs, Path path, FsAction action
			, IDictionary<URI, FileStatus> statCache)
		{
			FileStatus status = GetFileStatus(fs, path.ToUri(), statCache);
			FsPermission perms = status.GetPermission();
			FsAction otherAction = perms.GetOtherAction();
			if (otherAction.Implies(action))
			{
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FileStatus GetFileStatus(FileSystem fs, URI uri, IDictionary<URI, 
			FileStatus> statCache)
		{
			FileStatus stat = statCache[uri];
			if (stat == null)
			{
				stat = fs.GetFileStatus(new Path(uri));
				statCache[uri] = stat;
			}
			return stat;
		}
	}
}
