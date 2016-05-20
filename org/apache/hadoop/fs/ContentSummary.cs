using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Store the summary of a content (a directory or a file).</summary>
	public class ContentSummary : org.apache.hadoop.io.Writable
	{
		private long length;

		private long fileCount;

		private long directoryCount;

		private long quota;

		private long spaceConsumed;

		private long spaceQuota;

		private long[] typeConsumed;

		private long[] typeQuota;

		public class Builder
		{
			public Builder()
			{
				this.quota = -1;
				this.spaceQuota = -1;
				typeConsumed = new long[org.apache.hadoop.fs.StorageType.values().Length];
				typeQuota = new long[org.apache.hadoop.fs.StorageType.values().Length];
				for (int i = 0; i < typeQuota.Length; i++)
				{
					typeQuota[i] = -1;
				}
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder length(long length)
			{
				this.length = length;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder fileCount(long fileCount
				)
			{
				this.fileCount = fileCount;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder directoryCount(long directoryCount
				)
			{
				this.directoryCount = directoryCount;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder quota(long quota)
			{
				this.quota = quota;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder spaceConsumed(long spaceConsumed
				)
			{
				this.spaceConsumed = spaceConsumed;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder spaceQuota(long spaceQuota
				)
			{
				this.spaceQuota = spaceQuota;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder typeConsumed(long[] typeConsumed
				)
			{
				for (int i = 0; i < typeConsumed.Length; i++)
				{
					this.typeConsumed[i] = typeConsumed[i];
				}
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder typeQuota(org.apache.hadoop.fs.StorageType
				 type, long quota)
			{
				this.typeQuota[(int)(type)] = quota;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder typeConsumed(org.apache.hadoop.fs.StorageType
				 type, long consumed)
			{
				this.typeConsumed[(int)(type)] = consumed;
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary.Builder typeQuota(long[] typeQuota
				)
			{
				for (int i = 0; i < typeQuota.Length; i++)
				{
					this.typeQuota[i] = typeQuota[i];
				}
				return this;
			}

			public virtual org.apache.hadoop.fs.ContentSummary build()
			{
				return new org.apache.hadoop.fs.ContentSummary(length, fileCount, directoryCount, 
					quota, spaceConsumed, spaceQuota, typeConsumed, typeQuota);
			}

			private long length;

			private long fileCount;

			private long directoryCount;

			private long quota;

			private long spaceConsumed;

			private long spaceQuota;

			private long[] typeConsumed;

			private long[] typeQuota;
		}

		/// <summary>Constructor deprecated by ContentSummary.Builder</summary>
		[System.Obsolete]
		public ContentSummary()
		{
		}

		/// <summary>
		/// Constructor, deprecated by ContentSummary.Builder
		/// This constructor implicitly set spaceConsumed the same as length.
		/// </summary>
		/// <remarks>
		/// Constructor, deprecated by ContentSummary.Builder
		/// This constructor implicitly set spaceConsumed the same as length.
		/// spaceConsumed and length must be set explicitly with
		/// ContentSummary.Builder
		/// </remarks>
		[System.Obsolete]
		public ContentSummary(long length, long fileCount, long directoryCount)
			: this(length, fileCount, directoryCount, -1L, length, -1L)
		{
		}

		/// <summary>Constructor, deprecated by ContentSummary.Builder</summary>
		[System.Obsolete]
		public ContentSummary(long length, long fileCount, long directoryCount, long quota
			, long spaceConsumed, long spaceQuota)
		{
			this.length = length;
			this.fileCount = fileCount;
			this.directoryCount = directoryCount;
			this.quota = quota;
			this.spaceConsumed = spaceConsumed;
			this.spaceQuota = spaceQuota;
		}

		/// <summary>Constructor for ContentSummary.Builder</summary>
		private ContentSummary(long length, long fileCount, long directoryCount, long quota
			, long spaceConsumed, long spaceQuota, long[] typeConsumed, long[] typeQuota)
		{
			this.length = length;
			this.fileCount = fileCount;
			this.directoryCount = directoryCount;
			this.quota = quota;
			this.spaceConsumed = spaceConsumed;
			this.spaceQuota = spaceQuota;
			this.typeConsumed = typeConsumed;
			this.typeQuota = typeQuota;
		}

		/// <returns>the length</returns>
		public virtual long getLength()
		{
			return length;
		}

		/// <returns>the directory count</returns>
		public virtual long getDirectoryCount()
		{
			return directoryCount;
		}

		/// <returns>the file count</returns>
		public virtual long getFileCount()
		{
			return fileCount;
		}

		/// <summary>Return the directory quota</summary>
		public virtual long getQuota()
		{
			return quota;
		}

		/// <summary>Retuns storage space consumed</summary>
		public virtual long getSpaceConsumed()
		{
			return spaceConsumed;
		}

		/// <summary>Returns storage space quota</summary>
		public virtual long getSpaceQuota()
		{
			return spaceQuota;
		}

		/// <summary>Returns storage type quota</summary>
		public virtual long getTypeQuota(org.apache.hadoop.fs.StorageType type)
		{
			return (typeQuota != null) ? typeQuota[(int)(type)] : -1;
		}

		/// <summary>Returns storage type consumed</summary>
		public virtual long getTypeConsumed(org.apache.hadoop.fs.StorageType type)
		{
			return (typeConsumed != null) ? typeConsumed[(int)(type)] : 0;
		}

		/// <summary>Returns true if any storage type quota has been set</summary>
		public virtual bool isTypeQuotaSet()
		{
			if (typeQuota == null)
			{
				return false;
			}
			foreach (org.apache.hadoop.fs.StorageType t in org.apache.hadoop.fs.StorageType.getTypesSupportingQuota
				())
			{
				if (typeQuota[(int)(t)] > 0)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Returns true if any storage type consumption information is available</summary>
		public virtual bool isTypeConsumedAvailable()
		{
			if (typeConsumed == null)
			{
				return false;
			}
			foreach (org.apache.hadoop.fs.StorageType t in org.apache.hadoop.fs.StorageType.getTypesSupportingQuota
				())
			{
				if (typeConsumed[(int)(t)] > 0)
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeLong(length);
			@out.writeLong(fileCount);
			@out.writeLong(directoryCount);
			@out.writeLong(quota);
			@out.writeLong(spaceConsumed);
			@out.writeLong(spaceQuota);
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void readFields(java.io.DataInput @in)
		{
			this.length = @in.readLong();
			this.fileCount = @in.readLong();
			this.directoryCount = @in.readLong();
			this.quota = @in.readLong();
			this.spaceConsumed = @in.readLong();
			this.spaceQuota = @in.readLong();
		}

		/// <summary>
		/// Output format:
		/// <----12----> <----12----> <-------18------->
		/// DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME
		/// </summary>
		private const string STRING_FORMAT = "%12s %12s %18s ";

		/// <summary>
		/// Output format:
		/// <----12----> <----15----> <----15----> <----15----> <----12----> <----12----> <-------18------->
		/// QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM DIR_COUNT   FILE_COUNT   CONTENT_SIZE     FILE_NAME
		/// </summary>
		private const string QUOTA_STRING_FORMAT = "%12s %15s ";

		private const string SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";

		/// <summary>The header string</summary>
		private static readonly string HEADER = string.format(STRING_FORMAT.Replace('d', 
			's'), "directories", "files", "bytes");

		private static readonly string QUOTA_HEADER = string.format(QUOTA_STRING_FORMAT +
			 SPACE_QUOTA_STRING_FORMAT, "name quota", "rem name quota", "space quota", "rem space quota"
			) + HEADER;

		/// <summary>Return the header of the output.</summary>
		/// <remarks>
		/// Return the header of the output.
		/// if qOption is false, output directory count, file count, and content size;
		/// if qOption is true, output quota and remaining quota as well.
		/// </remarks>
		/// <param name="qOption">a flag indicating if quota needs to be printed or not</param>
		/// <returns>the header of the output</returns>
		public static string getHeader(bool qOption)
		{
			return qOption ? QUOTA_HEADER : HEADER;
		}

		public override string ToString()
		{
			return toString(true);
		}

		/// <summary>Return the string representation of the object in the output format.</summary>
		/// <remarks>
		/// Return the string representation of the object in the output format.
		/// if qOption is false, output directory count, file count, and content size;
		/// if qOption is true, output quota and remaining quota as well.
		/// </remarks>
		/// <param name="qOption">a flag indicating if quota needs to be printed or not</param>
		/// <returns>the string representation of the object</returns>
		public virtual string toString(bool qOption)
		{
			return toString(qOption, false);
		}

		/// <summary>Return the string representation of the object in the output format.</summary>
		/// <remarks>
		/// Return the string representation of the object in the output format.
		/// if qOption is false, output directory count, file count, and content size;
		/// if qOption is true, output quota and remaining quota as well.
		/// if hOption is false file sizes are returned in bytes
		/// if hOption is true file sizes are returned in human readable
		/// </remarks>
		/// <param name="qOption">a flag indicating if quota needs to be printed or not</param>
		/// <param name="hOption">a flag indicating if human readable output if to be used</param>
		/// <returns>the string representation of the object</returns>
		public virtual string toString(bool qOption, bool hOption)
		{
			string prefix = string.Empty;
			if (qOption)
			{
				string quotaStr = "none";
				string quotaRem = "inf";
				string spaceQuotaStr = "none";
				string spaceQuotaRem = "inf";
				if (quota > 0)
				{
					quotaStr = formatSize(quota, hOption);
					quotaRem = formatSize(quota - (directoryCount + fileCount), hOption);
				}
				if (spaceQuota > 0)
				{
					spaceQuotaStr = formatSize(spaceQuota, hOption);
					spaceQuotaRem = formatSize(spaceQuota - spaceConsumed, hOption);
				}
				prefix = string.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, quotaStr, 
					quotaRem, spaceQuotaStr, spaceQuotaRem);
			}
			return prefix + string.format(STRING_FORMAT, formatSize(directoryCount, hOption), 
				formatSize(fileCount, hOption), formatSize(length, hOption));
		}

		/// <summary>Formats a size to be human readable or in bytes</summary>
		/// <param name="size">value to be formatted</param>
		/// <param name="humanReadable">flag indicating human readable or not</param>
		/// <returns>String representation of the size</returns>
		private string formatSize(long size, bool humanReadable)
		{
			return humanReadable ? org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix
				.long2String(size, string.Empty, 1) : Sharpen.Runtime.getStringValueOf(size);
		}
	}
}
