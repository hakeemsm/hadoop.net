using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>Access token verification failed.</summary>
	[System.Serializable]
	public class InvalidBlockTokenException : IOException
	{
		private const long serialVersionUID = 168L;

		public InvalidBlockTokenException()
			: base()
		{
		}

		public InvalidBlockTokenException(string msg)
			: base(msg)
		{
		}
	}
}
