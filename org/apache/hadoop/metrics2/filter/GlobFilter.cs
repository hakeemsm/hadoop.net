using Sharpen;

namespace org.apache.hadoop.metrics2.filter
{
	/// <summary>A glob pattern filter for metrics.</summary>
	/// <remarks>
	/// A glob pattern filter for metrics.
	/// The class name is used in metrics config files
	/// </remarks>
	public class GlobFilter : org.apache.hadoop.metrics2.filter.AbstractPatternFilter
	{
		protected internal override java.util.regex.Pattern compile(string s)
		{
			return org.apache.hadoop.fs.GlobPattern.compile(s);
		}
	}
}
