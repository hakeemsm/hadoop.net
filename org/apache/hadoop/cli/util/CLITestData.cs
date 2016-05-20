using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>Class to store CLI Test Data</summary>
	public class CLITestData
	{
		private string testDesc = null;

		private System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> testCommands
			 = null;

		private System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> cleanupCommands
			 = null;

		private System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData
			> comparatorData = null;

		private bool testResult = false;

		public CLITestData()
		{
		}

		/// <returns>the testDesc</returns>
		public virtual string getTestDesc()
		{
			return testDesc;
		}

		/// <param name="testDesc">the testDesc to set</param>
		public virtual void setTestDesc(string testDesc)
		{
			this.testDesc = testDesc;
		}

		/// <returns>the testCommands</returns>
		public virtual System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
			> getTestCommands()
		{
			return testCommands;
		}

		/// <param name="testCommands">the testCommands to set</param>
		public virtual void setTestCommands(System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
			> testCommands)
		{
			this.testCommands = testCommands;
		}

		/// <returns>the comparatorData</returns>
		public virtual System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData
			> getComparatorData()
		{
			return comparatorData;
		}

		/// <param name="comparatorData">the comparatorData to set</param>
		public virtual void setComparatorData(System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData
			> comparatorData)
		{
			this.comparatorData = comparatorData;
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

		/// <returns>the cleanupCommands</returns>
		public virtual System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
			> getCleanupCommands()
		{
			return cleanupCommands;
		}

		/// <param name="cleanupCommands">the cleanupCommands to set</param>
		public virtual void setCleanupCommands(System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
			> cleanupCommands)
		{
			this.cleanupCommands = cleanupCommands;
		}
	}
}
