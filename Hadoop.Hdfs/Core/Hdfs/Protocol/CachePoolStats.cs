using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>CachePoolStats describes cache pool statistics.</summary>
	public class CachePoolStats
	{
		public class Builder
		{
			private long bytesNeeded;

			private long bytesCached;

			private long bytesOverlimit;

			private long filesNeeded;

			private long filesCached;

			public Builder()
			{
			}

			public virtual CachePoolStats.Builder SetBytesNeeded(long bytesNeeded)
			{
				this.bytesNeeded = bytesNeeded;
				return this;
			}

			public virtual CachePoolStats.Builder SetBytesCached(long bytesCached)
			{
				this.bytesCached = bytesCached;
				return this;
			}

			public virtual CachePoolStats.Builder SetBytesOverlimit(long bytesOverlimit)
			{
				this.bytesOverlimit = bytesOverlimit;
				return this;
			}

			public virtual CachePoolStats.Builder SetFilesNeeded(long filesNeeded)
			{
				this.filesNeeded = filesNeeded;
				return this;
			}

			public virtual CachePoolStats.Builder SetFilesCached(long filesCached)
			{
				this.filesCached = filesCached;
				return this;
			}

			public virtual CachePoolStats Build()
			{
				return new CachePoolStats(bytesNeeded, bytesCached, bytesOverlimit, filesNeeded, 
					filesCached);
			}
		}

		private readonly long bytesNeeded;

		private readonly long bytesCached;

		private readonly long bytesOverlimit;

		private readonly long filesNeeded;

		private readonly long filesCached;

		private CachePoolStats(long bytesNeeded, long bytesCached, long bytesOverlimit, long
			 filesNeeded, long filesCached)
		{
			this.bytesNeeded = bytesNeeded;
			this.bytesCached = bytesCached;
			this.bytesOverlimit = bytesOverlimit;
			this.filesNeeded = filesNeeded;
			this.filesCached = filesCached;
		}

		public virtual long GetBytesNeeded()
		{
			return bytesNeeded;
		}

		public virtual long GetBytesCached()
		{
			return bytesCached;
		}

		public virtual long GetBytesOverlimit()
		{
			return bytesOverlimit;
		}

		public virtual long GetFilesNeeded()
		{
			return filesNeeded;
		}

		public virtual long GetFilesCached()
		{
			return filesCached;
		}

		public override string ToString()
		{
			return new StringBuilder().Append("{").Append("bytesNeeded:").Append(bytesNeeded)
				.Append(", bytesCached:").Append(bytesCached).Append(", bytesOverlimit:").Append
				(bytesOverlimit).Append(", filesNeeded:").Append(filesNeeded).Append(", filesCached:"
				).Append(filesCached).Append("}").ToString();
		}
	}
}
