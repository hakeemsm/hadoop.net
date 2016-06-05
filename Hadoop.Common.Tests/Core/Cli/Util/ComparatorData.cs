

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Class to store CLI Test Comparators Data</summary>
	public class ComparatorData
	{
		private string expectedOutput = null;

		private string actualOutput = null;

		private bool testResult = false;

		private int exitCode = 0;

		private string comparatorType = null;

		public ComparatorData()
		{
		}

		/// <returns>the expectedOutput</returns>
		public virtual string GetExpectedOutput()
		{
			return expectedOutput;
		}

		/// <param name="expectedOutput">the expectedOutput to set</param>
		public virtual void SetExpectedOutput(string expectedOutput)
		{
			this.expectedOutput = expectedOutput;
		}

		/// <returns>the actualOutput</returns>
		public virtual string GetActualOutput()
		{
			return actualOutput;
		}

		/// <param name="actualOutput">the actualOutput to set</param>
		public virtual void SetActualOutput(string actualOutput)
		{
			this.actualOutput = actualOutput;
		}

		/// <returns>the testResult</returns>
		public virtual bool GetTestResult()
		{
			return testResult;
		}

		/// <param name="testResult">the testResult to set</param>
		public virtual void SetTestResult(bool testResult)
		{
			this.testResult = testResult;
		}

		/// <returns>the exitCode</returns>
		public virtual int GetExitCode()
		{
			return exitCode;
		}

		/// <param name="exitCode">the exitCode to set</param>
		public virtual void SetExitCode(int exitCode)
		{
			this.exitCode = exitCode;
		}

		/// <returns>the comparatorType</returns>
		public virtual string GetComparatorType()
		{
			return comparatorType;
		}

		/// <param name="comparatorType">the comparatorType to set</param>
		public virtual void SetComparatorType(string comparatorType)
		{
			this.comparatorType = comparatorType;
		}
	}
}
