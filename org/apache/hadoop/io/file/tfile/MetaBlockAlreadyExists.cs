using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Exception - Meta Block with the same name already exists.</summary>
	[System.Serializable]
	public class MetaBlockAlreadyExists : System.IO.IOException
	{
		/// <summary>Constructor</summary>
		/// <param name="s">message.</param>
		internal MetaBlockAlreadyExists(string s)
			: base(s)
		{
		}
	}
}
