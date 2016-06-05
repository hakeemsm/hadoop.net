using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This exception is thrown if resolving topology path
	/// for a node fails.
	/// </summary>
	[System.Serializable]
	public class UnresolvedTopologyException : IOException
	{
		/// <summary>for java.io.Serializable</summary>
		private const long serialVersionUID = 1L;

		public UnresolvedTopologyException(string text)
			: base(text)
		{
		}
	}
}
