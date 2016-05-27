using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Thrown when a symbolic link is encountered in a path.</summary>
	[System.Serializable]
	public class UnresolvedLinkException : IOException
	{
		private const long serialVersionUID = 1L;

		public UnresolvedLinkException()
			: base()
		{
		}

		public UnresolvedLinkException(string msg)
			: base(msg)
		{
		}
	}
}
