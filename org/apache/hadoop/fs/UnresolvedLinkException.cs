using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Thrown when a symbolic link is encountered in a path.</summary>
	[System.Serializable]
	public class UnresolvedLinkException : System.IO.IOException
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
