using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Describes a path-based cache directive.</summary>
	public class CacheDirectiveStats
	{
		public class Builder
		{
			private long bytesNeeded;

			private long bytesCached;

			private long filesNeeded;

			private long filesCached;

			private bool hasExpired;

			/// <summary>Builds a new CacheDirectiveStats populated with the set properties.</summary>
			/// <returns>New CacheDirectiveStats.</returns>
			public virtual CacheDirectiveStats Build()
			{
				return new CacheDirectiveStats(bytesNeeded, bytesCached, filesNeeded, filesCached
					, hasExpired);
			}

			/// <summary>Creates an empty builder.</summary>
			public Builder()
			{
			}

			/// <summary>Sets the bytes needed by this directive.</summary>
			/// <param name="bytesNeeded">The bytes needed.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveStats.Builder SetBytesNeeded(long bytesNeeded)
			{
				this.bytesNeeded = bytesNeeded;
				return this;
			}

			/// <summary>Sets the bytes cached by this directive.</summary>
			/// <param name="bytesCached">The bytes cached.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveStats.Builder SetBytesCached(long bytesCached)
			{
				this.bytesCached = bytesCached;
				return this;
			}

			/// <summary>Sets the files needed by this directive.</summary>
			/// <param name="filesNeeded">The number of files needed</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveStats.Builder SetFilesNeeded(long filesNeeded)
			{
				this.filesNeeded = filesNeeded;
				return this;
			}

			/// <summary>Sets the files cached by this directive.</summary>
			/// <param name="filesCached">The number of files cached.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveStats.Builder SetFilesCached(long filesCached)
			{
				this.filesCached = filesCached;
				return this;
			}

			/// <summary>Sets whether this directive has expired.</summary>
			/// <param name="hasExpired">if this directive has expired</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveStats.Builder SetHasExpired(bool hasExpired)
			{
				this.hasExpired = hasExpired;
				return this;
			}
		}

		private readonly long bytesNeeded;

		private readonly long bytesCached;

		private readonly long filesNeeded;

		private readonly long filesCached;

		private readonly bool hasExpired;

		private CacheDirectiveStats(long bytesNeeded, long bytesCached, long filesNeeded, 
			long filesCached, bool hasExpired)
		{
			this.bytesNeeded = bytesNeeded;
			this.bytesCached = bytesCached;
			this.filesNeeded = filesNeeded;
			this.filesCached = filesCached;
			this.hasExpired = hasExpired;
		}

		/// <returns>The bytes needed.</returns>
		public virtual long GetBytesNeeded()
		{
			return bytesNeeded;
		}

		/// <returns>The bytes cached.</returns>
		public virtual long GetBytesCached()
		{
			return bytesCached;
		}

		/// <returns>The number of files needed.</returns>
		public virtual long GetFilesNeeded()
		{
			return filesNeeded;
		}

		/// <returns>The number of files cached.</returns>
		public virtual long GetFilesCached()
		{
			return filesCached;
		}

		/// <returns>Whether this directive has expired.</returns>
		public virtual bool HasExpired()
		{
			return hasExpired;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			builder.Append("{");
			builder.Append("bytesNeeded: ").Append(bytesNeeded);
			builder.Append(", ").Append("bytesCached: ").Append(bytesCached);
			builder.Append(", ").Append("filesNeeded: ").Append(filesNeeded);
			builder.Append(", ").Append("filesCached: ").Append(filesCached);
			builder.Append(", ").Append("hasExpired: ").Append(hasExpired);
			builder.Append("}");
			return builder.ToString();
		}
	}
}
