using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Thrown when an unknown cipher suite is encountered.</summary>
	[System.Serializable]
	public class UnknownCipherSuiteException : IOException
	{
		public UnknownCipherSuiteException(string msg)
			: base(msg)
		{
		}
	}
}
