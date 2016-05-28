using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Indicates a failure manipulating an ACL.</summary>
	[System.Serializable]
	public class AclException : IOException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Creates a new AclException.</summary>
		/// <param name="message">String message</param>
		public AclException(string message)
			: base(message)
		{
		}
	}
}
