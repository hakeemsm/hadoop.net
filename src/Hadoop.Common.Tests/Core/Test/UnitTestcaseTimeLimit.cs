using NUnit.Framework;
using NUnit.Framework.Rules;


namespace Org.Apache.Hadoop.Test
{
	/// <summary>
	/// Class for test units to extend in order that their individual tests will
	/// be timed out and fail automatically should they run more than 10 seconds.
	/// </summary>
	/// <remarks>
	/// Class for test units to extend in order that their individual tests will
	/// be timed out and fail automatically should they run more than 10 seconds.
	/// This provides an automatic regression check for tests that begin running
	/// longer than expected.
	/// </remarks>
	public class UnitTestcaseTimeLimit
	{
		public readonly int timeOutSecs = 10;

		[Rule]
		public TestRule globalTimeout = new Timeout(timeOutSecs * 1000);
	}
}
