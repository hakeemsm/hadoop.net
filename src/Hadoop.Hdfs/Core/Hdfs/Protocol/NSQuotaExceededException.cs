using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	[System.Serializable]
	public sealed class NSQuotaExceededException : QuotaExceededException
	{
		protected internal const long serialVersionUID = 1L;

		private string prefix;

		public NSQuotaExceededException()
		{
		}

		public NSQuotaExceededException(string msg)
			: base(msg)
		{
		}

		public NSQuotaExceededException(long quota, long count)
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
					msg = "The NameSpace quota (directories and files)" + (pathName == null ? string.Empty
						 : (" of directory " + pathName)) + " is exceeded: quota=" + quota + " file count="
						 + count;
					if (prefix != null)
					{
						msg = prefix + ": " + msg;
					}
				}
				return msg;
			}
		}

		/// <summary>Set a prefix for the error message.</summary>
		public void SetMessagePrefix(string prefix)
		{
			this.prefix = prefix;
		}
	}
}
