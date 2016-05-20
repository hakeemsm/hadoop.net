using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Thrown when the startAfter can't be found when listing a directory.</summary>
	[System.Serializable]
	public class DirectoryListingStartAfterNotFoundException : System.IO.IOException
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
