using System;
using System.Text;
using Com.Google.Common.Base;
using Javax.Annotation;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A CachePool describes a set of cache resources being managed by the NameNode.
	/// 	</summary>
	/// <remarks>
	/// A CachePool describes a set of cache resources being managed by the NameNode.
	/// User caching requests are billed to the cache pool specified in the request.
	/// This is an internal class, only used on the NameNode.  For identifying or
	/// describing a cache pool to clients, please use CachePoolInfo.
	/// CachePools must be accessed under the FSNamesystem lock.
	/// </remarks>
	public sealed class CachePool
	{
		[Nonnull]
		private readonly string poolName;

		[Nonnull]
		private string ownerName;

		[Nonnull]
		private string groupName;

		/// <summary>Cache pool permissions.</summary>
		/// <remarks>
		/// Cache pool permissions.
		/// READ permission means that you can list the cache directives in this pool.
		/// WRITE permission means that you can add, remove, or modify cache directives
		/// in this pool.
		/// EXECUTE permission is unused.
		/// </remarks>
		[Nonnull]
		private FsPermission mode;

		/// <summary>Maximum number of bytes that can be cached in this pool.</summary>
		private long limit;

		/// <summary>
		/// Maximum duration that a CacheDirective in this pool remains valid,
		/// in milliseconds.
		/// </summary>
		private long maxRelativeExpiryMs;

		private long bytesNeeded;

		private long bytesCached;

		private long filesNeeded;

		private long filesCached;

		public sealed class DirectiveList : IntrusiveCollection<CacheDirective>
		{
			private readonly CachePool cachePool;

			private DirectiveList(CachePool cachePool)
			{
				this.cachePool = cachePool;
			}

			public CachePool GetCachePool()
			{
				return cachePool;
			}
		}

		[Nonnull]
		private readonly CachePool.DirectiveList directiveList;

		/// <summary>Create a new cache pool based on a CachePoolInfo object and the defaults.
		/// 	</summary>
		/// <remarks>
		/// Create a new cache pool based on a CachePoolInfo object and the defaults.
		/// We will fill in information that was not supplied according to the
		/// defaults.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static CachePool CreateFromInfoAndDefaults(CachePoolInfo info)
		{
			UserGroupInformation ugi = null;
			string ownerName = info.GetOwnerName();
			if (ownerName == null)
			{
				ugi = NameNode.GetRemoteUser();
				ownerName = ugi.GetShortUserName();
			}
			string groupName = info.GetGroupName();
			if (groupName == null)
			{
				if (ugi == null)
				{
					ugi = NameNode.GetRemoteUser();
				}
				groupName = ugi.GetPrimaryGroupName();
			}
			FsPermission mode = (info.GetMode() == null) ? FsPermission.GetCachePoolDefault()
				 : info.GetMode();
			long limit = info.GetLimit() == null ? CachePoolInfo.DefaultLimit : info.GetLimit
				();
			long maxRelativeExpiry = info.GetMaxRelativeExpiryMs() == null ? CachePoolInfo.DefaultMaxRelativeExpiry
				 : info.GetMaxRelativeExpiryMs();
			return new CachePool(info.GetPoolName(), ownerName, groupName, mode, limit, maxRelativeExpiry
				);
		}

		/// <summary>Create a new cache pool based on a CachePoolInfo object.</summary>
		/// <remarks>
		/// Create a new cache pool based on a CachePoolInfo object.
		/// No fields in the CachePoolInfo can be blank.
		/// </remarks>
		internal static CachePool CreateFromInfo(CachePoolInfo info)
		{
			return new CachePool(info.GetPoolName(), info.GetOwnerName(), info.GetGroupName()
				, info.GetMode(), info.GetLimit(), info.GetMaxRelativeExpiryMs());
		}

		internal CachePool(string poolName, string ownerName, string groupName, FsPermission
			 mode, long limit, long maxRelativeExpiry)
		{
			directiveList = new CachePool.DirectiveList(this);
			Preconditions.CheckNotNull(poolName);
			Preconditions.CheckNotNull(ownerName);
			Preconditions.CheckNotNull(groupName);
			Preconditions.CheckNotNull(mode);
			this.poolName = poolName;
			this.ownerName = ownerName;
			this.groupName = groupName;
			this.mode = new FsPermission(mode);
			this.limit = limit;
			this.maxRelativeExpiryMs = maxRelativeExpiry;
		}

		public string GetPoolName()
		{
			return poolName;
		}

		public string GetOwnerName()
		{
			return ownerName;
		}

		public CachePool SetOwnerName(string ownerName)
		{
			this.ownerName = ownerName;
			return this;
		}

		public string GetGroupName()
		{
			return groupName;
		}

		public CachePool SetGroupName(string groupName)
		{
			this.groupName = groupName;
			return this;
		}

		public FsPermission GetMode()
		{
			return mode;
		}

		public CachePool SetMode(FsPermission mode)
		{
			this.mode = new FsPermission(mode);
			return this;
		}

		public long GetLimit()
		{
			return limit;
		}

		public CachePool SetLimit(long bytes)
		{
			this.limit = bytes;
			return this;
		}

		public long GetMaxRelativeExpiryMs()
		{
			return maxRelativeExpiryMs;
		}

		public CachePool SetMaxRelativeExpiryMs(long expiry)
		{
			this.maxRelativeExpiryMs = expiry;
			return this;
		}

		/// <summary>Get either full or partial information about this CachePool.</summary>
		/// <param name="fullInfo">
		/// If true, only the name will be returned (i.e., what you
		/// would get if you didn't have read permission for this pool.)
		/// </param>
		/// <returns>Cache pool information.</returns>
		internal CachePoolInfo GetInfo(bool fullInfo)
		{
			CachePoolInfo info = new CachePoolInfo(poolName);
			if (!fullInfo)
			{
				return info;
			}
			return info.SetOwnerName(ownerName).SetGroupName(groupName).SetMode(new FsPermission
				(mode)).SetLimit(limit).SetMaxRelativeExpiryMs(maxRelativeExpiryMs);
		}

		/// <summary>Resets statistics related to this CachePool</summary>
		public void ResetStatistics()
		{
			bytesNeeded = 0;
			bytesCached = 0;
			filesNeeded = 0;
			filesCached = 0;
		}

		public void AddBytesNeeded(long bytes)
		{
			bytesNeeded += bytes;
		}

		public void AddBytesCached(long bytes)
		{
			bytesCached += bytes;
		}

		public void AddFilesNeeded(long files)
		{
			filesNeeded += files;
		}

		public void AddFilesCached(long files)
		{
			filesCached += files;
		}

		public long GetBytesNeeded()
		{
			return bytesNeeded;
		}

		public long GetBytesCached()
		{
			return bytesCached;
		}

		public long GetBytesOverlimit()
		{
			return Math.Max(bytesNeeded - limit, 0);
		}

		public long GetFilesNeeded()
		{
			return filesNeeded;
		}

		public long GetFilesCached()
		{
			return filesCached;
		}

		/// <summary>Get statistics about this CachePool.</summary>
		/// <returns>Cache pool statistics.</returns>
		private CachePoolStats GetStats()
		{
			return new CachePoolStats.Builder().SetBytesNeeded(bytesNeeded).SetBytesCached(bytesCached
				).SetBytesOverlimit(GetBytesOverlimit()).SetFilesNeeded(filesNeeded).SetFilesCached
				(filesCached).Build();
		}

		/// <summary>
		/// Returns a CachePoolInfo describing this CachePool based on the permissions
		/// of the calling user.
		/// </summary>
		/// <remarks>
		/// Returns a CachePoolInfo describing this CachePool based on the permissions
		/// of the calling user. Unprivileged users will see only minimal descriptive
		/// information about the pool.
		/// </remarks>
		/// <param name="pc">
		/// Permission checker to be used to validate the user's permissions,
		/// or null
		/// </param>
		/// <returns>CachePoolEntry describing this CachePool</returns>
		public CachePoolEntry GetEntry(FSPermissionChecker pc)
		{
			bool hasPermission = true;
			if (pc != null)
			{
				try
				{
					pc.CheckPermission(this, FsAction.Read);
				}
				catch (AccessControlException)
				{
					hasPermission = false;
				}
			}
			return new CachePoolEntry(GetInfo(hasPermission), hasPermission ? GetStats() : new 
				CachePoolStats.Builder().Build());
		}

		public override string ToString()
		{
			return new StringBuilder().Append("{ ").Append("poolName:").Append(poolName).Append
				(", ownerName:").Append(ownerName).Append(", groupName:").Append(groupName).Append
				(", mode:").Append(mode).Append(", limit:").Append(limit).Append(", maxRelativeExpiryMs:"
				).Append(maxRelativeExpiryMs).Append(" }").ToString();
		}

		public CachePool.DirectiveList GetDirectiveList()
		{
			return directiveList;
		}
	}
}
