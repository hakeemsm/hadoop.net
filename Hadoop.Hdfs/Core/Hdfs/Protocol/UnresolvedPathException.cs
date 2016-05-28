using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Thrown when a symbolic link is encountered in a path.</summary>
	[System.Serializable]
	public sealed class UnresolvedPathException : UnresolvedLinkException
	{
		private const long serialVersionUID = 1L;

		private string path;

		private string preceding;

		private string remainder;

		private string linkTarget;

		/// <summary>Used by RemoteException to instantiate an UnresolvedPathException.</summary>
		public UnresolvedPathException(string msg)
			: base(msg)
		{
		}

		public UnresolvedPathException(string path, string preceding, string remainder, string
			 linkTarget)
		{
			// The path containing the link
			// The path part preceding the link
			// The path part following the link
			// The link's target
			this.path = path;
			this.preceding = preceding;
			this.remainder = remainder;
			this.linkTarget = linkTarget;
		}

		/// <summary>Return a path with the link resolved with the target.</summary>
		/// <exception cref="System.IO.IOException"/>
		public Path GetResolvedPath()
		{
			// If the path is absolute we cam throw out the preceding part and
			// just append the remainder to the target, otherwise append each
			// piece to resolve the link in path.
			bool noRemainder = (remainder == null || string.Empty.Equals(remainder));
			Path target = new Path(linkTarget);
			if (target.IsUriPathAbsolute())
			{
				return noRemainder ? target : new Path(target, remainder);
			}
			else
			{
				return noRemainder ? new Path(preceding, target) : new Path(new Path(preceding, linkTarget
					), remainder);
			}
		}

		public override string Message
		{
			get
			{
				string msg = base.Message;
				if (msg != null)
				{
					return msg;
				}
				string myMsg = "Unresolved path " + path;
				try
				{
					return GetResolvedPath().ToString();
				}
				catch (IOException)
				{
				}
				// Ignore
				return myMsg;
			}
		}
	}
}
