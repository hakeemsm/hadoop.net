using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This exception is thrown when an operation is not supported.</summary>
	[System.Serializable]
	public class UnsupportedActionException : IOException
	{
		/// <summary>for java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		public UnsupportedActionException(string msg)
			: base(msg)
		{
		}
	}
}
