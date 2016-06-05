

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>
	/// Comparator for command line tests that attempts to find a regexp
	/// within the entire text returned by a command.
	/// </summary>
	/// <remarks>
	/// Comparator for command line tests that attempts to find a regexp
	/// within the entire text returned by a command.
	/// This comparator differs from RegexpComparator in that it attempts
	/// to match the pattern within all of the text returned by the command,
	/// rather than matching against each line of the returned text.  This
	/// allows matching against patterns that span multiple lines.
	/// </remarks>
	public class RegexpAcrossOutputComparator : ComparatorBase
	{
		public override bool Compare(string actual, string expected)
		{
			return Pattern.Compile(expected).Matcher(actual).Find();
		}
	}
}
