using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Thrown when the startAfter can't be found when listing a directory.</summary>
	[System.Serializable]
	public class DirectoryListingStartAfterNotFoundException : IOException
	{
		private const long serialVersionUID = 1L;

		public DirectoryListingStartAfterNotFoundException()
			: base()
		{
		}

		public DirectoryListingStartAfterNotFoundException(string msg)
			: base(msg)
		{
		}
	}
}
