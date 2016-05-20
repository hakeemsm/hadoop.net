using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Exception - No such Meta Block with the given name.</summary>
	[System.Serializable]
	public class MetaBlockDoesNotExist : System.IO.IOException
	{
		/// <summary>Constructor</summary>
		/// <param name="s">message.</param>
		internal MetaBlockDoesNotExist(string s)
			: base(s)
		{
		}
	}
}
