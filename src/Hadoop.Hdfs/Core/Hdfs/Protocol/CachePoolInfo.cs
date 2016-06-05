using System.IO;
using System.Text;
using Javax.Annotation;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>CachePoolInfo describes a cache pool.</summary>
	/// <remarks>
	/// CachePoolInfo describes a cache pool.
	/// This class is used in RPCs to create and modify cache pools.
	/// It is serializable and can be stored in the edit log.
	/// </remarks>
	public class CachePoolInfo
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo
			));

		/// <summary>Indicates that the pool does not have a maximum relative expiry.</summary>
		public const long RelativeExpiryNever = CacheDirectiveInfo.Expiration.MaxRelativeExpiryMs;

		/// <summary>Default max relative expiry for cache pools.</summary>
		public const long DefaultMaxRelativeExpiry = RelativeExpiryNever;

		public const long LimitUnlimited = long.MaxValue;

		public const long DefaultLimit = LimitUnlimited;

		internal readonly string poolName;

		[Nullable]
		internal string ownerName;

		[Nullable]
		internal string groupName;

		[Nullable]
		internal FsPermission mode;

		[Nullable]
		internal long limit;

		[Nullable]
		internal long maxRelativeExpiryMs;

		public CachePoolInfo(string poolName)
		{
			this.poolName = poolName;
		}

		/// <returns>Name of the pool.</returns>
		public virtual string GetPoolName()
		{
			return poolName;
		}

		/// <returns>
		/// The owner of the pool. Along with the group and mode, determines
		/// who has access to view and modify the pool.
		/// </returns>
		public virtual string GetOwnerName()
		{
			return ownerName;
		}

		public virtual Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo SetOwnerName(string 
			ownerName)
		{
			this.ownerName = ownerName;
			return this;
		}

		/// <returns>
		/// The group of the pool. Along with the owner and mode, determines
		/// who has access to view and modify the pool.
		/// </returns>
		public virtual string GetGroupName()
		{
			return groupName;
		}

		public virtual Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo SetGroupName(string 
			groupName)
		{
			this.groupName = groupName;
			return this;
		}

		/// <returns>
		/// Unix-style permissions of the pool. Along with the owner and group,
		/// determines who has access to view and modify the pool.
		/// </returns>
		public virtual FsPermission GetMode()
		{
			return mode;
		}

		public virtual Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo SetMode(FsPermission
			 mode)
		{
			this.mode = mode;
			return this;
		}

		/// <returns>
		/// The maximum aggregate number of bytes that can be cached by
		/// directives in this pool.
		/// </returns>
		public virtual long GetLimit()
		{
			return limit;
		}

		public virtual Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo SetLimit(long bytes)
		{
			this.limit = bytes;
			return this;
		}

		/// <returns>
		/// The maximum relative expiration of directives of this pool in
		/// milliseconds
		/// </returns>
		public virtual long GetMaxRelativeExpiryMs()
		{
			return maxRelativeExpiryMs;
		}

		/// <summary>
		/// Set the maximum relative expiration of directives of this pool in
		/// milliseconds.
		/// </summary>
		/// <param name="ms">in milliseconds</param>
		/// <returns>This builder, for call chaining.</returns>
		public virtual Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo SetMaxRelativeExpiryMs
			(long ms)
		{
			this.maxRelativeExpiryMs = ms;
			return this;
		}

		public override string ToString()
		{
			return new StringBuilder().Append("{").Append("poolName:").Append(poolName).Append
				(", ownerName:").Append(ownerName).Append(", groupName:").Append(groupName).Append
				(", mode:").Append((mode == null) ? "null" : string.Format("0%03o", mode.ToShort
				())).Append(", limit:").Append(limit).Append(", maxRelativeExpiryMs:").Append(maxRelativeExpiryMs
				).Append("}").ToString();
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (o == this)
			{
				return true;
			}
			if (o.GetType() != GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo other = (Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo
				)o;
			return new EqualsBuilder().Append(poolName, other.poolName).Append(ownerName, other
				.ownerName).Append(groupName, other.groupName).Append(mode, other.mode).Append(limit
				, other.limit).Append(maxRelativeExpiryMs, other.maxRelativeExpiryMs).IsEquals();
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(poolName).Append(ownerName).Append(groupName)
				.Append(mode).Append(limit).Append(maxRelativeExpiryMs).GetHashCode();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Validate(Org.Apache.Hadoop.Hdfs.Protocol.CachePoolInfo info)
		{
			if (info == null)
			{
				throw new InvalidRequestException("CachePoolInfo is null");
			}
			if ((info.GetLimit() != null) && (info.GetLimit() < 0))
			{
				throw new InvalidRequestException("Limit is negative.");
			}
			if (info.GetMaxRelativeExpiryMs() != null)
			{
				long maxRelativeExpiryMs = info.GetMaxRelativeExpiryMs();
				if (maxRelativeExpiryMs < 0l)
				{
					throw new InvalidRequestException("Max relative expiry is negative.");
				}
				if (maxRelativeExpiryMs > CacheDirectiveInfo.Expiration.MaxRelativeExpiryMs)
				{
					throw new InvalidRequestException("Max relative expiry is too big.");
				}
			}
			ValidateName(info.poolName);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void ValidateName(string poolName)
		{
			if (poolName == null || poolName.IsEmpty())
			{
				// Empty pool names are not allowed because they would be highly
				// confusing.  They would also break the ability to list all pools
				// by starting with prevKey = ""
				throw new IOException("invalid empty cache pool name");
			}
		}
	}
}
