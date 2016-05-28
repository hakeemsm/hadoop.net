using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Exception indicating that a replica is already being recovery.</summary>
	[System.Serializable]
	public class RecoveryInProgressException : IOException
	{
		private const long serialVersionUID = 1L;

		public RecoveryInProgressException(string msg)
			: base(msg)
		{
		}
	}
}
