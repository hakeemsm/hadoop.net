using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	/// <summary>Encryption key verification failed.</summary>
	[System.Serializable]
	public class InvalidEncryptionKeyException : IOException
	{
		private const long serialVersionUID = 0l;

		public InvalidEncryptionKeyException()
			: base()
		{
		}

		public InvalidEncryptionKeyException(string msg)
			: base(msg)
		{
		}
	}
}
