using Sharpen;

namespace org.apache.hadoop.cli.util
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
		public virtual string getExpectedOutput()
		{
			return expectedOutput;
		}

		/// <param name="expectedOutput">the expectedOutput to set</param>
		public virtual void setExpectedOutput(string expectedOutput)
		{
			this.expectedOutput = expectedOutput;
		}

		/// <returns>the actualOutput</returns>
		public virtual string getActualOutput()
		{
			return actualOutput;
		}

		/// <param name="actualOutput">the actualOutput to set</param>
		public virtual void setActualOutput(string actualOutput)
		{
			this.actualOutput = actualOutput;
		}

		/// <returns>the testResult</returns>
		public virtual bool getTestResult()
		{
			return testResult;
		}

		/// <param name="testResult">the testResult to set</param>
		public virtual void setTestResult(bool testResult)
		{
			this.testResult = testResult;
		}

		/// <returns>the exitCode</returns>
		public virtual int getExitCode()
		{
			return exitCode;
		}

		/// <param name="exitCode">the exitCode to set</param>
		public virtual void setExitCode(int exitCode)
		{
			this.exitCode = exitCode;
		}

		/// <returns>the comparatorType</returns>
		public virtual string getComparatorType()
		{
			return comparatorType;
		}

		/// <param name="comparatorType">the comparatorType to set</param>
		public virtual void setComparatorType(string comparatorType)
		{
			this.comparatorType = comparatorType;
		}
	}
}
