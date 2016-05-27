using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Exception - Meta Block with the same name already exists.</summary>
	[System.Serializable]
	public class MetaBlockAlreadyExists : IOException
	{
		/// <summary>Constructor</summary>
		/// <param name="s">message.</param>
		internal MetaBlockAlreadyExists(string s)
			: base(s)
		{
		}
	}
}
