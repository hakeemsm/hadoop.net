using Sharpen;

namespace org.apache.hadoop.fs
{
	public interface PathFilter
	{
		/// <summary>
		/// Tests whether or not the specified abstract pathname should be
		/// included in a pathname list.
		/// </summary>
		/// <param name="path">The abstract pathname to be tested</param>
		/// <returns>
		/// <code>true</code> if and only if <code>pathname</code>
		/// should be included
		/// </returns>
		bool accept(org.apache.hadoop.fs.Path path);
	}
}
