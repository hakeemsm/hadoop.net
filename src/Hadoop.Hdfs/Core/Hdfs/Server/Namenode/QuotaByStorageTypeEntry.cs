using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class QuotaByStorageTypeEntry
	{
		private StorageType type;

		private long quota;

		public virtual StorageType GetStorageType()
		{
			return type;
		}

		public virtual long GetQuota()
		{
			return quota;
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (GetType() != o.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.QuotaByStorageTypeEntry other = (Org.Apache.Hadoop.Hdfs.Server.Namenode.QuotaByStorageTypeEntry
				)o;
			return Objects.Equal(type, other.type) && Objects.Equal(quota, other.quota);
		}

		public override int GetHashCode()
		{
			return Objects.HashCode(type, quota);
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			System.Diagnostics.Debug.Assert((type != null));
			sb.Append(StringUtils.ToLowerCase(type.ToString()));
			sb.Append(':');
			sb.Append(quota);
			return sb.ToString();
		}

		public class Builder
		{
			private StorageType type;

			private long quota;

			public virtual QuotaByStorageTypeEntry.Builder SetStorageType(StorageType type)
			{
				this.type = type;
				return this;
			}

			public virtual QuotaByStorageTypeEntry.Builder SetQuota(long quota)
			{
				this.quota = quota;
				return this;
			}

			public virtual QuotaByStorageTypeEntry Build()
			{
				return new QuotaByStorageTypeEntry(type, quota);
			}
		}

		private QuotaByStorageTypeEntry(StorageType type, long quota)
		{
			this.type = type;
			this.quota = quota;
		}
	}
}
