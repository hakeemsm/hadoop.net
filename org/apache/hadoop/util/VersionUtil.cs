using Sharpen;

namespace org.apache.hadoop.util
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
		public static int compareVersions(string version1, string version2)
		{
			org.apache.hadoop.util.ComparableVersion v1 = new org.apache.hadoop.util.ComparableVersion
				(version1);
			org.apache.hadoop.util.ComparableVersion v2 = new org.apache.hadoop.util.ComparableVersion
				(version2);
			return v1.compareTo(v2);
		}
	}
}
