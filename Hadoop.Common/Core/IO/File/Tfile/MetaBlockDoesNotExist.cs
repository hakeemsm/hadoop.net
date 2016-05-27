using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Exception - No such Meta Block with the given name.</summary>
	[System.Serializable]
	public class MetaBlockDoesNotExist : IOException
	{
		/// <summary>Constructor</summary>
		/// <param name="s">message.</param>
		internal MetaBlockDoesNotExist(string s)
			: base(s)
		{
		}
	}
}
