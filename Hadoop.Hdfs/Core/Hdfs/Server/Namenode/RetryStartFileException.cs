using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	[System.Serializable]
	public class RetryStartFileException : IOException
	{
		private const long serialVersionUID = 1L;

		public RetryStartFileException()
			: base("Preconditions for creating a file failed because of a " + "transient error, retry create later."
				)
		{
		}

		public RetryStartFileException(string s)
			: base(s)
		{
		}
	}
}
