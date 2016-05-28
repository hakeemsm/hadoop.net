using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	[System.Serializable]
	public class QuotaByStorageTypeExceededException : QuotaExceededException
	{
		protected internal const long serialVersionUID = 1L;

		protected internal StorageType type;

		public QuotaByStorageTypeExceededException()
		{
		}

		public QuotaByStorageTypeExceededException(string msg)
			: base(msg)
		{
		}

		public QuotaByStorageTypeExceededException(long quota, long count, StorageType type
			)
			: base(quota, count)
		{
			this.type = type;
		}

		public override string Message
		{
			get
			{
				string msg = base.Message;
				if (msg == null)
				{
					return "Quota by storage type : " + type.ToString() + " on path : " + (pathName ==
						 null ? string.Empty : pathName) + " is exceeded. quota = " + StringUtils.TraditionalBinaryPrefix.Long2String
						(quota, "B", 2) + " but space consumed = " + StringUtils.TraditionalBinaryPrefix.Long2String
						(count, "B", 2);
				}
				else
				{
					return msg;
				}
			}
		}
	}
}
