using Sharpen;

namespace org.apache.hadoop.metrics2.filter
{
	/// <summary>A regex pattern filter for metrics</summary>
	public class RegexFilter : org.apache.hadoop.metrics2.filter.AbstractPatternFilter
	{
		protected internal override java.util.regex.Pattern compile(string s)
		{
			return java.util.regex.Pattern.compile(s);
		}
	}
}
