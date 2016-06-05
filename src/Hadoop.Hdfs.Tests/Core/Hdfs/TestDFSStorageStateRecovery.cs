using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This test ensures the appropriate response (successful or failure) from
	/// the system when the system is started under various storage state and
	/// version conditions.
	/// </summary>
	public class TestDFSStorageStateRecovery
	{
		private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestDFSStorageStateRecovery"
			);

		private Configuration conf = null;

		private int testCounter = 0;

		private MiniDFSCluster cluster = null;

		private const int CurrentExists = 0;

		private const int PreviousExists = 1;

		private const int PreviousTmpExists = 2;

		private const int RemovedTmpExists = 3;

		private const int ShouldRecover = 4;

		private const int CurrentShouldExistAfterRecover = 5;

		private const int PreviousShouldExistAfterRecover = 6;

		/// <summary>The test case table.</summary>
		/// <remarks>
		/// The test case table.  Each row represents a test case.  This table is
		/// taken from the table in Apendix A of the HDFS Upgrade Test Plan
		/// (TestPlan-HdfsUpgrade.html) attached to
		/// http://issues.apache.org/jira/browse/HADOOP-702
		/// It has been slightly modified since previouscheckpoint.tmp no longer
		/// exists.
		/// The column meanings are:
		/// 0) current directory exists
		/// 1) previous directory exists
		/// 2) previous.tmp directory exists
		/// 3) removed.tmp directory exists
		/// 4) node should recover and startup
		/// 5) current directory should exist after recovery but before startup
		/// 6) previous directory should exist after recovery but before startup
		/// </remarks>
		internal static readonly bool[][] testCases = new bool[][] { new bool[] { true, false
			, false, false, true, true, false }, new bool[] { true, true, false, false, true
			, true, true }, new bool[] { true, false, true, false, true, true, true }, new bool
			[] { true, true, true, true, false, false, false }, new bool[] { true, true, true
			, false, false, false, false }, new bool[] { false, true, true, true, false, false
			, false }, new bool[] { false, true, true, false, false, false, false }, new bool
			[] { false, false, false, false, false, false, false }, new bool[] { false, true
			, false, false, false, false, false }, new bool[] { false, false, true, false, true
			, true, false }, new bool[] { true, false, false, true, true, true, false }, new 
			bool[] { true, true, false, true, false, false, false }, new bool[] { true, true
			, true, true, false, false, false }, new bool[] { true, false, true, true, false
			, false, false }, new bool[] { false, true, true, true, false, false, false }, new 
			bool[] { false, false, true, true, false, false, false }, new bool[] { false, false
			, false, true, false, false, false }, new bool[] { false, true, false, true, true
			, true, true }, new bool[] { true, true, false, false, true, true, false } };

		private static readonly int NumNnTestCases = testCases.Length;

		private const int NumDnTestCases = 18;

		// Constants for indexes into test case table below.
		// 1
		// 2
		// 3
		// 4
		// 4
		// 4
		// 4
		// 5
		// 6
		// 7
		// 8
		// 9
		// 10
		// 10
		// 10
		// 10
		// 11
		// 12
		// name-node specific cases
		// 13
		/// <summary>Writes an INFO log message containing the parameters.</summary>
		/// <remarks>
		/// Writes an INFO log message containing the parameters. Only
		/// the first 4 elements of the state array are included in the message.
		/// </remarks>
		internal virtual void Log(string label, int numDirs, int testCaseNum, bool[] state
			)
		{
			Log.Info("============================================================");
			Log.Info("***TEST " + (testCounter++) + "*** " + label + ":" + " numDirs=" + numDirs
				 + " testCase=" + testCaseNum + " current=" + state[CurrentExists] + " previous="
				 + state[PreviousExists] + " previous.tmp=" + state[PreviousTmpExists] + " removed.tmp="
				 + state[RemovedTmpExists] + " should recover=" + state[ShouldRecover] + " current exists after="
				 + state[CurrentShouldExistAfterRecover] + " previous exists after=" + state[PreviousShouldExistAfterRecover
				]);
		}

		/// <summary>
		/// Sets up the storage directories for namenode as defined by
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// . For each element
		/// in
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// , the subdirectories
		/// represented by the first four elements of the <code>state</code> array
		/// will be created and populated.
		/// See
		/// <see cref="UpgradeUtilities.CreateNameNodeStorageDirs(string[], string)"/>
		/// </summary>
		/// <param name="state">
		/// a row from the testCases table which indicates which directories
		/// to setup for the node
		/// </param>
		/// <returns>file paths representing namenode storage directories</returns>
		/// <exception cref="System.Exception"/>
		internal virtual string[] CreateNameNodeStorageState(bool[] state)
		{
			string[] baseDirs = conf.GetStrings(DFSConfigKeys.DfsNamenodeNameDirKey);
			UpgradeUtilities.CreateEmptyDirs(baseDirs);
			if (state[CurrentExists])
			{
				// current
				UpgradeUtilities.CreateNameNodeStorageDirs(baseDirs, "current");
			}
			if (state[PreviousExists])
			{
				// previous
				UpgradeUtilities.CreateNameNodeStorageDirs(baseDirs, "previous");
			}
			if (state[PreviousTmpExists])
			{
				// previous.tmp
				UpgradeUtilities.CreateNameNodeStorageDirs(baseDirs, "previous.tmp");
			}
			if (state[RemovedTmpExists])
			{
				// removed.tmp
				UpgradeUtilities.CreateNameNodeStorageDirs(baseDirs, "removed.tmp");
			}
			return baseDirs;
		}

		/// <summary>
		/// Sets up the storage directories for a datanode under
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// . For each element in
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// , the subdirectories
		/// represented by the first four elements of the <code>state</code> array
		/// will be created and populated.
		/// See
		/// <see cref="UpgradeUtilities.CreateDataNodeStorageDirs(string[], string)"/>
		/// </summary>
		/// <param name="state">
		/// a row from the testCases table which indicates which directories
		/// to setup for the node
		/// </param>
		/// <returns>file paths representing datanode storage directories</returns>
		/// <exception cref="System.Exception"/>
		internal virtual string[] CreateDataNodeStorageState(bool[] state)
		{
			string[] baseDirs = conf.GetStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
			UpgradeUtilities.CreateEmptyDirs(baseDirs);
			if (state[CurrentExists])
			{
				// current
				UpgradeUtilities.CreateDataNodeStorageDirs(baseDirs, "current");
			}
			if (state[PreviousExists])
			{
				// previous
				UpgradeUtilities.CreateDataNodeStorageDirs(baseDirs, "previous");
			}
			if (state[PreviousTmpExists])
			{
				// previous.tmp
				UpgradeUtilities.CreateDataNodeStorageDirs(baseDirs, "previous.tmp");
			}
			if (state[RemovedTmpExists])
			{
				// removed.tmp
				UpgradeUtilities.CreateDataNodeStorageDirs(baseDirs, "removed.tmp");
			}
			return baseDirs;
		}

		/// <summary>
		/// Sets up the storage directories for a block pool under
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// . For each element
		/// in
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// , the subdirectories
		/// represented by the first four elements of the <code>state</code> array
		/// will be created and populated.
		/// See
		/// <see cref="UpgradeUtilities.CreateBlockPoolStorageDirs(string[], string, string)"
		/// 	/>
		/// </summary>
		/// <param name="bpid">block pool Id</param>
		/// <param name="state">
		/// a row from the testCases table which indicates which directories
		/// to setup for the node
		/// </param>
		/// <returns>file paths representing block pool storage directories</returns>
		/// <exception cref="System.Exception"/>
		internal virtual string[] CreateBlockPoolStorageState(string bpid, bool[] state)
		{
			string[] baseDirs = conf.GetStrings(DFSConfigKeys.DfsDatanodeDataDirKey);
			UpgradeUtilities.CreateEmptyDirs(baseDirs);
			UpgradeUtilities.CreateDataNodeStorageDirs(baseDirs, "current");
			// After copying the storage directories from master datanode, empty
			// the block pool storage directories
			string[] bpDirs = UpgradeUtilities.CreateEmptyBPDirs(baseDirs, bpid);
			if (state[CurrentExists])
			{
				// current
				UpgradeUtilities.CreateBlockPoolStorageDirs(baseDirs, "current", bpid);
			}
			if (state[PreviousExists])
			{
				// previous
				UpgradeUtilities.CreateBlockPoolStorageDirs(baseDirs, "previous", bpid);
			}
			if (state[PreviousTmpExists])
			{
				// previous.tmp
				UpgradeUtilities.CreateBlockPoolStorageDirs(baseDirs, "previous.tmp", bpid);
			}
			if (state[RemovedTmpExists])
			{
				// removed.tmp
				UpgradeUtilities.CreateBlockPoolStorageDirs(baseDirs, "removed.tmp", bpid);
			}
			return bpDirs;
		}

		/// <summary>
		/// For NameNode, verify that the current and/or previous exist as indicated by
		/// the method parameters.
		/// </summary>
		/// <remarks>
		/// For NameNode, verify that the current and/or previous exist as indicated by
		/// the method parameters.  If previous exists, verify that
		/// it hasn't been modified by comparing the checksum of all it's
		/// containing files with their original checksum.  It is assumed that
		/// the server has recovered.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckResultNameNode(string[] baseDirs, bool currentShouldExist
			, bool previousShouldExist)
		{
			if (currentShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					NUnit.Framework.Assert.IsTrue(new FilePath(baseDirs[i], "current").IsDirectory());
					NUnit.Framework.Assert.IsTrue(new FilePath(baseDirs[i], "current/VERSION").IsFile
						());
					NUnit.Framework.Assert.IsNotNull(FSImageTestUtil.FindNewestImageFile(baseDirs[i] 
						+ "/current"));
					NUnit.Framework.Assert.IsTrue(new FilePath(baseDirs[i], "current/seen_txid").IsFile
						());
				}
			}
			if (previousShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					NUnit.Framework.Assert.IsTrue(new FilePath(baseDirs[i], "previous").IsDirectory()
						);
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.NameNode, new FilePath(baseDirs[i], "previous"), false), UpgradeUtilities.ChecksumMasterNameNodeContents
						());
				}
			}
		}

		/// <summary>
		/// For datanode, verify that the current and/or previous exist as indicated by
		/// the method parameters.
		/// </summary>
		/// <remarks>
		/// For datanode, verify that the current and/or previous exist as indicated by
		/// the method parameters.  If previous exists, verify that
		/// it hasn't been modified by comparing the checksum of all it's
		/// containing files with their original checksum.  It is assumed that
		/// the server has recovered.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckResultDataNode(string[] baseDirs, bool currentShouldExist
			, bool previousShouldExist)
		{
			if (currentShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.DataNode, new FilePath(baseDirs[i], "current"), false), UpgradeUtilities.ChecksumMasterDataNodeContents
						());
				}
			}
			if (previousShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					NUnit.Framework.Assert.IsTrue(new FilePath(baseDirs[i], "previous").IsDirectory()
						);
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.DataNode, new FilePath(baseDirs[i], "previous"), false), UpgradeUtilities.ChecksumMasterDataNodeContents
						());
				}
			}
		}

		/// <summary>
		/// For block pool, verify that the current and/or previous exist as indicated
		/// by the method parameters.
		/// </summary>
		/// <remarks>
		/// For block pool, verify that the current and/or previous exist as indicated
		/// by the method parameters.  If previous exists, verify that
		/// it hasn't been modified by comparing the checksum of all it's
		/// containing files with their original checksum.  It is assumed that
		/// the server has recovered.
		/// </remarks>
		/// <param name="baseDirs">directories pointing to block pool storage</param>
		/// <param name="bpid">block pool Id</param>
		/// <param name="currentShouldExist">current directory exists under storage</param>
		/// <param name="currentShouldExist">previous directory exists under storage</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckResultBlockPool(string[] baseDirs, bool currentShouldExist
			, bool previousShouldExist)
		{
			if (currentShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					FilePath bpCurDir = new FilePath(baseDirs[i], Storage.StorageDirCurrent);
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.DataNode, bpCurDir, false), UpgradeUtilities.ChecksumMasterBlockPoolContents());
				}
			}
			if (previousShouldExist)
			{
				for (int i = 0; i < baseDirs.Length; i++)
				{
					FilePath bpPrevDir = new FilePath(baseDirs[i], Storage.StorageDirPrevious);
					NUnit.Framework.Assert.IsTrue(bpPrevDir.IsDirectory());
					NUnit.Framework.Assert.AreEqual(UpgradeUtilities.ChecksumContents(HdfsServerConstants.NodeType
						.DataNode, bpPrevDir, false), UpgradeUtilities.ChecksumMasterBlockPoolContents()
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MiniDFSCluster CreateCluster(Configuration c)
		{
			return new MiniDFSCluster.Builder(c).NumDataNodes(0).StartupOption(HdfsServerConstants.StartupOption
				.Regular).Format(false).ManageDataDfsDirs(false).ManageNameDfsDirs(false).Build(
				);
		}

		/// <summary>
		/// This test iterates over the testCases table and attempts
		/// to startup the NameNode normally.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNStorageStates()
		{
			string[] baseDirs;
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				for (int i = 0; i < NumNnTestCases; i++)
				{
					bool[] testCase = testCases[i];
					bool shouldRecover = testCase[ShouldRecover];
					bool curAfterRecover = testCase[CurrentShouldExistAfterRecover];
					bool prevAfterRecover = testCase[PreviousShouldExistAfterRecover];
					Log("NAME_NODE recovery", numDirs, i, testCase);
					baseDirs = CreateNameNodeStorageState(testCase);
					if (shouldRecover)
					{
						cluster = CreateCluster(conf);
						CheckResultNameNode(baseDirs, curAfterRecover, prevAfterRecover);
						cluster.Shutdown();
					}
					else
					{
						try
						{
							cluster = CreateCluster(conf);
							throw new Exception("NameNode should have failed to start");
						}
						catch (IOException expected)
						{
							// the exception is expected
							// check that the message says "not formatted" 
							// when storage directory is empty (case #5)
							if (!testCases[i][CurrentExists] && !testCases[i][PreviousTmpExists] && !testCases
								[i][PreviousExists] && !testCases[i][RemovedTmpExists])
							{
								NUnit.Framework.Assert.IsTrue(expected.GetLocalizedMessage().Contains("NameNode is not formatted"
									));
							}
						}
					}
					cluster.Shutdown();
				}
			}
		}

		// end testCases loop
		// end numDirs loop
		/// <summary>
		/// This test iterates over the testCases table for Datanode storage and
		/// attempts to startup the DataNode normally.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDNStorageStates()
		{
			string[] baseDirs;
			// First setup the datanode storage directory
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				conf = new HdfsConfiguration();
				conf.SetInt(DFSConfigKeys.DfsDatanodeScanPeriodHoursKey, -1);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				for (int i = 0; i < NumDnTestCases; i++)
				{
					bool[] testCase = testCases[i];
					bool shouldRecover = testCase[ShouldRecover];
					bool curAfterRecover = testCase[CurrentShouldExistAfterRecover];
					bool prevAfterRecover = testCase[PreviousShouldExistAfterRecover];
					Log("DATA_NODE recovery", numDirs, i, testCase);
					CreateNameNodeStorageState(new bool[] { true, true, false, false, false });
					cluster = CreateCluster(conf);
					baseDirs = CreateDataNodeStorageState(testCase);
					if (!testCase[CurrentExists] && !testCase[PreviousExists] && !testCase[PreviousTmpExists
						] && !testCase[RemovedTmpExists])
					{
						// DataNode will create and format current if no directories exist
						cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
							null);
					}
					else
					{
						if (shouldRecover)
						{
							cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
								null);
							CheckResultDataNode(baseDirs, curAfterRecover, prevAfterRecover);
						}
						else
						{
							cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
								null);
							NUnit.Framework.Assert.IsFalse(cluster.GetDataNodes()[0].IsDatanodeUp());
						}
					}
					cluster.Shutdown();
				}
			}
		}

		// end testCases loop
		// end numDirs loop
		/// <summary>
		/// This test iterates over the testCases table for block pool storage and
		/// attempts to startup the DataNode normally.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockPoolStorageStates()
		{
			string[] baseDirs;
			// First setup the datanode storage directory
			string bpid = UpgradeUtilities.GetCurrentBlockPoolID(null);
			for (int numDirs = 1; numDirs <= 2; numDirs++)
			{
				conf = new HdfsConfiguration();
				conf.SetInt("dfs.datanode.scan.period.hours", -1);
				conf = UpgradeUtilities.InitializeStorageStateConf(numDirs, conf);
				for (int i = 0; i < NumDnTestCases; i++)
				{
					bool[] testCase = testCases[i];
					bool shouldRecover = testCase[ShouldRecover];
					bool curAfterRecover = testCase[CurrentShouldExistAfterRecover];
					bool prevAfterRecover = testCase[PreviousShouldExistAfterRecover];
					Log("BLOCK_POOL recovery", numDirs, i, testCase);
					CreateNameNodeStorageState(new bool[] { true, true, false, false, false });
					cluster = CreateCluster(conf);
					baseDirs = CreateBlockPoolStorageState(bpid, testCase);
					if (!testCase[CurrentExists] && !testCase[PreviousExists] && !testCase[PreviousTmpExists
						] && !testCase[RemovedTmpExists])
					{
						// DataNode will create and format current if no directories exist
						cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
							null);
					}
					else
					{
						if (shouldRecover)
						{
							cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
								null);
							CheckResultBlockPool(baseDirs, curAfterRecover, prevAfterRecover);
						}
						else
						{
							cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
								null);
							NUnit.Framework.Assert.IsFalse(cluster.GetDataNodes()[0].IsBPServiceAlive(bpid));
						}
					}
					cluster.Shutdown();
				}
			}
		}

		// end testCases loop
		// end numDirs loop
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Log.Info("Setting up the directory structures.");
			UpgradeUtilities.Initialize();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			Log.Info("Shutting down MiniDFSCluster");
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}
	}
}
