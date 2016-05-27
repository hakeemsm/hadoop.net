using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A wrapper class to maven's ComparableVersion class, to comply
	/// with maven's version name string convention
	/// </summary>
	public abstract class VersionUtil
	{
		/// <summary>Compares two version name strings using maven's ComparableVersion class.
		/// 	</summary>
		/// <param name="version1">the first version to compare</param>
		/// <param name="version2">the second version to compare</param>
		/// <returns>
		/// a negative integer if version1 precedes version2, a positive
		/// integer if version2 precedes version1, and 0 if and only if the two
		/// versions are equal.
		/// </returns>
		public static int CompareVersions(string version1, string version2)
		{
			ComparableVersion v1 = new ComparableVersion(version1);
			ComparableVersion v2 = new ComparableVersion(version2);
			return v1.CompareTo(v2);
		}
	}
}
