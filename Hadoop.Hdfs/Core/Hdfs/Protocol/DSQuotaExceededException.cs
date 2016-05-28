using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	[System.Serializable]
	public class DSQuotaExceededException : QuotaExceededException
	{
		protected internal const long serialVersionUID = 1L;

		public DSQuotaExceededException()
		{
		}

		public DSQuotaExceededException(string msg)
			: base(msg)
		{
		}

		public DSQuotaExceededException(long quota, long count)
			: base(quota, count)
		{
		}

		public override string Message
		{
			get
			{
				string msg = base.Message;
				if (msg == null)
				{
					return "The DiskSpace quota" + (pathName == null ? string.Empty : " of " + pathName
						) + " is exceeded: quota = " + quota + " B = " + StringUtils.TraditionalBinaryPrefix.Long2String
						(quota, "B", 2) + " but diskspace consumed = " + count + " B = " + StringUtils.TraditionalBinaryPrefix.Long2String
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
