using System;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test enumerations in TestHdfsServerConstants.</summary>
	public class TestHdfsServerConstants
	{
		/// <summary>Verify that parsing a StartupOption string gives the expected results.</summary>
		/// <remarks>
		/// Verify that parsing a StartupOption string gives the expected results.
		/// If a RollingUpgradeStartupOption is specified than it is also checked.
		/// </remarks>
		/// <param name="value"/>
		/// <param name="expectedOption"/>
		/// <param name="expectedRollupOption">optional, may be null.</param>
		private static void VerifyStartupOptionResult(string value, HdfsServerConstants.StartupOption
			 expectedOption, HdfsServerConstants.RollingUpgradeStartupOption expectedRollupOption
			)
		{
			HdfsServerConstants.StartupOption option = HdfsServerConstants.StartupOption.GetEnum
				(value);
			NUnit.Framework.Assert.AreEqual(expectedOption, option);
			if (expectedRollupOption != null)
			{
				NUnit.Framework.Assert.AreEqual(expectedRollupOption, option.GetRollingUpgradeStartupOption
					());
			}
		}

		/// <summary>
		/// Test that we can parse a StartupOption string without the optional
		/// RollingUpgradeStartupOption.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestStartupOptionParsing()
		{
			VerifyStartupOptionResult("FORMAT", HdfsServerConstants.StartupOption.Format, null
				);
			VerifyStartupOptionResult("REGULAR", HdfsServerConstants.StartupOption.Regular, null
				);
			VerifyStartupOptionResult("CHECKPOINT", HdfsServerConstants.StartupOption.Checkpoint
				, null);
			VerifyStartupOptionResult("UPGRADE", HdfsServerConstants.StartupOption.Upgrade, null
				);
			VerifyStartupOptionResult("ROLLBACK", HdfsServerConstants.StartupOption.Rollback, 
				null);
			VerifyStartupOptionResult("FINALIZE", HdfsServerConstants.StartupOption.Finalize, 
				null);
			VerifyStartupOptionResult("ROLLINGUPGRADE", HdfsServerConstants.StartupOption.Rollingupgrade
				, null);
			VerifyStartupOptionResult("IMPORT", HdfsServerConstants.StartupOption.Import, null
				);
			VerifyStartupOptionResult("INITIALIZESHAREDEDITS", HdfsServerConstants.StartupOption
				.Initializesharededits, null);
			try
			{
				VerifyStartupOptionResult("UNKNOWN(UNKNOWNOPTION)", HdfsServerConstants.StartupOption
					.Format, null);
				NUnit.Framework.Assert.Fail("Failed to get expected IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
		}

		// Expected!
		/// <summary>
		/// Test that we can parse a StartupOption string with a
		/// RollingUpgradeStartupOption.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestRollingUpgradeStartupOptionParsing()
		{
			VerifyStartupOptionResult("ROLLINGUPGRADE(ROLLBACK)", HdfsServerConstants.StartupOption
				.Rollingupgrade, HdfsServerConstants.RollingUpgradeStartupOption.Rollback);
			VerifyStartupOptionResult("ROLLINGUPGRADE(DOWNGRADE)", HdfsServerConstants.StartupOption
				.Rollingupgrade, HdfsServerConstants.RollingUpgradeStartupOption.Downgrade);
			VerifyStartupOptionResult("ROLLINGUPGRADE(STARTED)", HdfsServerConstants.StartupOption
				.Rollingupgrade, HdfsServerConstants.RollingUpgradeStartupOption.Started);
			try
			{
				VerifyStartupOptionResult("ROLLINGUPGRADE(UNKNOWNOPTION)", HdfsServerConstants.StartupOption
					.Rollingupgrade, null);
				NUnit.Framework.Assert.Fail("Failed to get expected IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
		}
		// Expected!
	}
}
