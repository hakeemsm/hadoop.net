using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// This exception is thrown when modification to HDFS results in violation
	/// of a directory quota.
	/// </summary>
	/// <remarks>
	/// This exception is thrown when modification to HDFS results in violation
	/// of a directory quota. A directory quota might be namespace quota (limit
	/// on number of files and directories) or a diskspace quota (limit on space
	/// taken by all the file under the directory tree). <br /> <br />
	/// The message for the exception specifies the directory where the quota
	/// was violated and actual quotas. Specific message is generated in the
	/// corresponding Exception class:
	/// DSQuotaExceededException or
	/// NSQuotaExceededException
	/// </remarks>
	[System.Serializable]
	public class QuotaExceededException : IOException
	{
		protected internal const long serialVersionUID = 1L;

		protected internal string pathName = null;

		protected internal long quota;

		protected internal long count;

		protected internal QuotaExceededException()
		{
		}

		protected internal QuotaExceededException(string msg)
			: base(msg)
		{
		}

		protected internal QuotaExceededException(long quota, long count)
		{
			// quota
			// actual value
			this.quota = quota;
			this.count = count;
		}

		public virtual void SetPathName(string path)
		{
			this.pathName = path;
		}

		public override string Message
		{
			get
			{
				return base.Message;
			}
		}
	}
}
