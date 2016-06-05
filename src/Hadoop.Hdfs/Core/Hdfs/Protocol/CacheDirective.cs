using System;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Namenode class that tracks state related to a cached path.</summary>
	/// <remarks>
	/// Namenode class that tracks state related to a cached path.
	/// This is an implementation class, not part of the public API.
	/// </remarks>
	public sealed class CacheDirective : IntrusiveCollection.Element
	{
		private readonly long id;

		private readonly string path;

		private readonly short replication;

		private CachePool pool;

		private readonly long expiryTime;

		private long bytesNeeded;

		private long bytesCached;

		private long filesNeeded;

		private long filesCached;

		private IntrusiveCollection.Element prev;

		private IntrusiveCollection.Element next;

		public CacheDirective(CacheDirectiveInfo info)
			: this(info.GetId(), info.GetPath().ToUri().GetPath(), info.GetReplication(), info
				.GetExpiration().GetAbsoluteMillis())
		{
		}

		public CacheDirective(long id, string path, short replication, long expiryTime)
		{
			Preconditions.CheckArgument(id > 0);
			this.id = id;
			this.path = Preconditions.CheckNotNull(path);
			Preconditions.CheckArgument(replication > 0);
			this.replication = replication;
			this.expiryTime = expiryTime;
		}

		public long GetId()
		{
			return id;
		}

		public string GetPath()
		{
			return path;
		}

		public short GetReplication()
		{
			return replication;
		}

		public CachePool GetPool()
		{
			return pool;
		}

		/// <returns>When this directive expires, in milliseconds since Unix epoch</returns>
		public long GetExpiryTime()
		{
			return expiryTime;
		}

		/// <returns>When this directive expires, as an ISO-8601 formatted string.</returns>
		public string GetExpiryTimeString()
		{
			return DFSUtil.DateToIso8601String(Sharpen.Extensions.CreateDate(expiryTime));
		}

		/// <summary>
		/// Returns a
		/// <see cref="CacheDirectiveInfo"/>
		/// based on this CacheDirective.
		/// <p>
		/// This always sets an absolute expiry time, never a relative TTL.
		/// </summary>
		public CacheDirectiveInfo ToInfo()
		{
			return new CacheDirectiveInfo.Builder().SetId(id).SetPath(new Path(path)).SetReplication
				(replication).SetPool(pool.GetPoolName()).SetExpiration(CacheDirectiveInfo.Expiration
				.NewAbsolute(expiryTime)).Build();
		}

		public CacheDirectiveStats ToStats()
		{
			return new CacheDirectiveStats.Builder().SetBytesNeeded(bytesNeeded).SetBytesCached
				(bytesCached).SetFilesNeeded(filesNeeded).SetFilesCached(filesCached).SetHasExpired
				(new DateTime().GetTime() > expiryTime).Build();
		}

		public CacheDirectiveEntry ToEntry()
		{
			return new CacheDirectiveEntry(ToInfo(), ToStats());
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			builder.Append("{ id:").Append(id).Append(", path:").Append(path).Append(", replication:"
				).Append(replication).Append(", pool:").Append(pool).Append(", expiryTime: ").Append
				(GetExpiryTimeString()).Append(", bytesNeeded:").Append(bytesNeeded).Append(", bytesCached:"
				).Append(bytesCached).Append(", filesNeeded:").Append(filesNeeded).Append(", filesCached:"
				).Append(filesCached).Append(" }");
			return builder.ToString();
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
			if (o.GetType() != this.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Protocol.CacheDirective other = (Org.Apache.Hadoop.Hdfs.Protocol.CacheDirective
				)o;
			return id == other.id;
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(id).ToHashCode();
		}

		//
		// Stats related getters and setters
		//
		/// <summary>Resets the byte and file statistics being tracked by this CacheDirective.
		/// 	</summary>
		public void ResetStatistics()
		{
			bytesNeeded = 0;
			bytesCached = 0;
			filesNeeded = 0;
			filesCached = 0;
		}

		public long GetBytesNeeded()
		{
			return bytesNeeded;
		}

		public void AddBytesNeeded(long bytes)
		{
			this.bytesNeeded += bytes;
			pool.AddBytesNeeded(bytes);
		}

		public long GetBytesCached()
		{
			return bytesCached;
		}

		public void AddBytesCached(long bytes)
		{
			this.bytesCached += bytes;
			pool.AddBytesCached(bytes);
		}

		public long GetFilesNeeded()
		{
			return filesNeeded;
		}

		public void AddFilesNeeded(long files)
		{
			this.filesNeeded += files;
			pool.AddFilesNeeded(files);
		}

		public long GetFilesCached()
		{
			return filesCached;
		}

		public void AddFilesCached(long files)
		{
			this.filesCached += files;
			pool.AddFilesCached(files);
		}

		//
		// IntrusiveCollection.Element implementation
		//
		public void InsertInternal<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 prev, IntrusiveCollection.Element next)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			System.Diagnostics.Debug.Assert(this.pool == null);
			this.pool = ((CachePool.DirectiveList)list).GetCachePool();
			this.prev = prev;
			this.next = next;
		}

		public void SetPrev<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 prev)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			System.Diagnostics.Debug.Assert(list == pool.GetDirectiveList());
			this.prev = prev;
		}

		public void SetNext<_T0>(IntrusiveCollection<_T0> list, IntrusiveCollection.Element
			 next)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			System.Diagnostics.Debug.Assert(list == pool.GetDirectiveList());
			this.next = next;
		}

		public void RemoveInternal<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			System.Diagnostics.Debug.Assert(list == pool.GetDirectiveList());
			this.pool = null;
			this.prev = null;
			this.next = null;
		}

		public IntrusiveCollection.Element GetPrev<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			if (list != pool.GetDirectiveList())
			{
				return null;
			}
			return this.prev;
		}

		public IntrusiveCollection.Element GetNext<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			if (list != pool.GetDirectiveList())
			{
				return null;
			}
			return this.next;
		}

		public bool IsInList<_T0>(IntrusiveCollection<_T0> list)
			where _T0 : IntrusiveCollection.Element
		{
			// IntrusiveCollection.Element
			return pool == null ? false : list == pool.GetDirectiveList();
		}
	}
}
