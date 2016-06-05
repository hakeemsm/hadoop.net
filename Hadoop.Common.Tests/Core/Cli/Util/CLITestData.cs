

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Class to store CLI Test Data</summary>
	public class CLITestData
	{
		private string testDesc = null;

		private AList<CLICommand> testCommands = null;

		private AList<CLICommand> cleanupCommands = null;

		private AList<ComparatorData> comparatorData = null;

		private bool testResult = false;

		public CLITestData()
		{
		}

		/// <returns>the testDesc</returns>
		public virtual string GetTestDesc()
		{
			return testDesc;
		}

		/// <param name="testDesc">the testDesc to set</param>
		public virtual void SetTestDesc(string testDesc)
		{
			this.testDesc = testDesc;
		}

		/// <returns>the testCommands</returns>
		public virtual AList<CLICommand> GetTestCommands()
		{
			return testCommands;
		}

		/// <param name="testCommands">the testCommands to set</param>
		public virtual void SetTestCommands(AList<CLICommand> testCommands)
		{
			this.testCommands = testCommands;
		}

		/// <returns>the comparatorData</returns>
		public virtual AList<ComparatorData> GetComparatorData()
		{
			return comparatorData;
		}

		/// <param name="comparatorData">the comparatorData to set</param>
		public virtual void SetComparatorData(AList<ComparatorData> comparatorData)
		{
			this.comparatorData = comparatorData;
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

		/// <returns>the cleanupCommands</returns>
		public virtual AList<CLICommand> GetCleanupCommands()
		{
			return cleanupCommands;
		}

		/// <param name="cleanupCommands">the cleanupCommands to set</param>
		public virtual void SetCleanupCommands(AList<CLICommand> cleanupCommands)
		{
			this.cleanupCommands = cleanupCommands;
		}
	}
}
