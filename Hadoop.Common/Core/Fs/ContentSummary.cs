using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>Store the summary of a content (a directory or a file).</summary>
	public class ContentSummary : IWritable
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
				typeConsumed = new long[StorageType.Values().Length];
				typeQuota = new long[StorageType.Values().Length];
				for (int i = 0; i < typeQuota.Length; i++)
				{
					typeQuota[i] = -1;
				}
			}

			public virtual ContentSummary.Builder Length(long length)
			{
				this.length = length;
				return this;
			}

			public virtual ContentSummary.Builder FileCount(long fileCount)
			{
				this.fileCount = fileCount;
				return this;
			}

			public virtual ContentSummary.Builder DirectoryCount(long directoryCount)
			{
				this.directoryCount = directoryCount;
				return this;
			}

			public virtual ContentSummary.Builder Quota(long quota)
			{
				this.quota = quota;
				return this;
			}

			public virtual ContentSummary.Builder SpaceConsumed(long spaceConsumed)
			{
				this.spaceConsumed = spaceConsumed;
				return this;
			}

			public virtual ContentSummary.Builder SpaceQuota(long spaceQuota)
			{
				this.spaceQuota = spaceQuota;
				return this;
			}

			public virtual ContentSummary.Builder TypeConsumed(long[] typeConsumed)
			{
				for (int i = 0; i < typeConsumed.Length; i++)
				{
					this.typeConsumed[i] = typeConsumed[i];
				}
				return this;
			}

			public virtual ContentSummary.Builder TypeQuota(StorageType type, long quota)
			{
				this.typeQuota[(int)(type)] = quota;
				return this;
			}

			public virtual ContentSummary.Builder TypeConsumed(StorageType type, long consumed
				)
			{
				this.typeConsumed[(int)(type)] = consumed;
				return this;
			}

			public virtual ContentSummary.Builder TypeQuota(long[] typeQuota)
			{
				for (int i = 0; i < typeQuota.Length; i++)
				{
					this.typeQuota[i] = typeQuota[i];
				}
				return this;
			}

			public virtual ContentSummary Build()
			{
				return new ContentSummary(length, fileCount, directoryCount, quota, spaceConsumed
					, spaceQuota, typeConsumed, typeQuota);
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
		[Obsolete]
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
		[Obsolete]
		public ContentSummary(long length, long fileCount, long directoryCount)
			: this(length, fileCount, directoryCount, -1L, length, -1L)
		{
		}

		/// <summary>Constructor, deprecated by ContentSummary.Builder</summary>
		[Obsolete]
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
		public virtual long GetLength()
		{
			return length;
		}

		/// <returns>the directory count</returns>
		public virtual long GetDirectoryCount()
		{
			return directoryCount;
		}

		/// <returns>the file count</returns>
		public virtual long GetFileCount()
		{
			return fileCount;
		}

		/// <summary>Return the directory quota</summary>
		public virtual long GetQuota()
		{
			return quota;
		}

		/// <summary>Retuns storage space consumed</summary>
		public virtual long GetSpaceConsumed()
		{
			return spaceConsumed;
		}

		/// <summary>Returns storage space quota</summary>
		public virtual long GetSpaceQuota()
		{
			return spaceQuota;
		}

		/// <summary>Returns storage type quota</summary>
		public virtual long GetTypeQuota(StorageType type)
		{
			return (typeQuota != null) ? typeQuota[(int)(type)] : -1;
		}

		/// <summary>Returns storage type consumed</summary>
		public virtual long GetTypeConsumed(StorageType type)
		{
			return (typeConsumed != null) ? typeConsumed[(int)(type)] : 0;
		}

		/// <summary>Returns true if any storage type quota has been set</summary>
		public virtual bool IsTypeQuotaSet()
		{
			if (typeQuota == null)
			{
				return false;
			}
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				if (typeQuota[(int)(t)] > 0)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Returns true if any storage type consumption information is available</summary>
		public virtual bool IsTypeConsumedAvailable()
		{
			if (typeConsumed == null)
			{
				return false;
			}
			foreach (StorageType t in StorageType.GetTypesSupportingQuota())
			{
				if (typeConsumed[(int)(t)] > 0)
				{
					return true;
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteLong(length);
			@out.WriteLong(fileCount);
			@out.WriteLong(directoryCount);
			@out.WriteLong(quota);
			@out.WriteLong(spaceConsumed);
			@out.WriteLong(spaceQuota);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void ReadFields(BinaryReader @in)
		{
			this.length = @in.ReadLong();
			this.fileCount = @in.ReadLong();
			this.directoryCount = @in.ReadLong();
			this.quota = @in.ReadLong();
			this.spaceConsumed = @in.ReadLong();
			this.spaceQuota = @in.ReadLong();
		}

		/// <summary>
		/// Output format:
		/// <----12----> <----12----> <-------18------->
		/// DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME
		/// </summary>
		private const string StringFormat = "%12s %12s %18s ";

		/// <summary>
		/// Output format:
		/// <----12----> <----15----> <----15----> <----15----> <----12----> <----12----> <-------18------->
		/// QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM DIR_COUNT   FILE_COUNT   CONTENT_SIZE     FILE_NAME
		/// </summary>
		private const string QuotaStringFormat = "%12s %15s ";

		private const string SpaceQuotaStringFormat = "%15s %15s ";

		/// <summary>The header string</summary>
		private static readonly string Header = string.Format(StringFormat.Replace('d', 's'
			), "directories", "files", "bytes");

		private static readonly string QuotaHeader = string.Format(QuotaStringFormat + SpaceQuotaStringFormat
			, "name quota", "rem name quota", "space quota", "rem space quota") + Header;

		/// <summary>Return the header of the output.</summary>
		/// <remarks>
		/// Return the header of the output.
		/// if qOption is false, output directory count, file count, and content size;
		/// if qOption is true, output quota and remaining quota as well.
		/// </remarks>
		/// <param name="qOption">a flag indicating if quota needs to be printed or not</param>
		/// <returns>the header of the output</returns>
		public static string GetHeader(bool qOption)
		{
			return qOption ? QuotaHeader : Header;
		}

		public override string ToString()
		{
			return ToString(true);
		}

		/// <summary>Return the string representation of the object in the output format.</summary>
		/// <remarks>
		/// Return the string representation of the object in the output format.
		/// if qOption is false, output directory count, file count, and content size;
		/// if qOption is true, output quota and remaining quota as well.
		/// </remarks>
		/// <param name="qOption">a flag indicating if quota needs to be printed or not</param>
		/// <returns>the string representation of the object</returns>
		public virtual string ToString(bool qOption)
		{
			return ToString(qOption, false);
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
		public virtual string ToString(bool qOption, bool hOption)
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
					quotaStr = FormatSize(quota, hOption);
					quotaRem = FormatSize(quota - (directoryCount + fileCount), hOption);
				}
				if (spaceQuota > 0)
				{
					spaceQuotaStr = FormatSize(spaceQuota, hOption);
					spaceQuotaRem = FormatSize(spaceQuota - spaceConsumed, hOption);
				}
				prefix = string.Format(QuotaStringFormat + SpaceQuotaStringFormat, quotaStr, quotaRem
					, spaceQuotaStr, spaceQuotaRem);
			}
			return prefix + string.Format(StringFormat, FormatSize(directoryCount, hOption), 
				FormatSize(fileCount, hOption), FormatSize(length, hOption));
		}

		/// <summary>Formats a size to be human readable or in bytes</summary>
		/// <param name="size">value to be formatted</param>
		/// <param name="humanReadable">flag indicating human readable or not</param>
		/// <returns>String representation of the size</returns>
		private string FormatSize(long size, bool humanReadable)
		{
			return humanReadable ? StringUtils.TraditionalBinaryPrefix.Long2String(size, string.Empty
				, 1) : size.ToString();
		}
	}
}
