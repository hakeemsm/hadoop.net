using System;
using Javax.Xml.Parsers;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Cli.Util;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Xml.Sax;
using Org.Xml.Sax.Helpers;
using Sharpen;

namespace Org.Apache.Hadoop.Cli
{
	/// <summary>Tests for the Command Line Interface (CLI)</summary>
	public class CLITestHelper
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CLITestHelper).FullName
			);

		public const string TestmodeTest = "test";

		public const string TestmodeNocompare = "nocompare";

		public static readonly string TestCacheDataDir = Runtime.GetProperty("test.cache.data"
			, "build/test/cache");

		protected internal string testMode = TestmodeTest;

		protected internal AList<CLITestData> testsFromConfigFile = null;

		protected internal AList<ComparatorData> testComparators = null;

		protected internal string thisTestCaseName = null;

		protected internal ComparatorData comparatorData = null;

		protected internal Configuration conf = null;

		protected internal string clitestDataDir = null;

		protected internal string username = null;

		// In this mode, it runs the command and compares the actual output
		// with the expected output  
		// Run the tests
		// If it is set to nocompare, run the command and do not compare.
		// This can be useful populate the testConfig.xml file the first time
		// a new command is added
		//By default, run the tests. The other mode is to run the commands and not
		// compare the output
		// Storage for tests read in from the config file
		/// <summary>Read the test config file - testConfig.xml</summary>
		protected internal virtual void ReadTestConfigFile()
		{
			string testConfigFile = GetTestFile();
			if (testsFromConfigFile == null)
			{
				bool success = false;
				testConfigFile = TestCacheDataDir + FilePath.separator + testConfigFile;
				try
				{
					SAXParser p = (SAXParserFactory.NewInstance()).NewSAXParser();
					p.Parse(testConfigFile, GetConfigParser());
					success = true;
				}
				catch (Exception)
				{
					Log.Info("File: " + testConfigFile + " not found");
					success = false;
				}
				Assert.True("Error reading test config file", success);
			}
		}

		/// <summary>
		/// Method decides what is a proper configuration file parser for this type
		/// of CLI tests.
		/// </summary>
		/// <remarks>
		/// Method decides what is a proper configuration file parser for this type
		/// of CLI tests.
		/// Ancestors need to override the implementation if a parser with additional
		/// features is needed. Also, such ancestor has to provide its own
		/// TestConfigParser implementation
		/// </remarks>
		/// <returns>an instance of TestConfigFileParser class</returns>
		protected internal virtual CLITestHelper.TestConfigFileParser GetConfigParser()
		{
			return new CLITestHelper.TestConfigFileParser(this);
		}

		protected internal virtual string GetTestFile()
		{
			return string.Empty;
		}

		/*
		* Setup
		*/
		/// <exception cref="System.Exception"/>
		public virtual void SetUp()
		{
			// Read the testConfig.xml file
			ReadTestConfigFile();
			conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, true);
			clitestDataDir = new FilePath(TestCacheDataDir).ToURI().ToString().Replace(' ', '+'
				);
		}

		/// <summary>Tear down</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TearDown()
		{
			DisplayResults();
		}

		/// <summary>Expand the commands from the test config xml file</summary>
		/// <param name="cmd"/>
		/// <returns>String expanded command</returns>
		protected internal virtual string ExpandCommand(string cmd)
		{
			string expCmd = cmd;
			expCmd = expCmd.ReplaceAll("CLITEST_DATA", clitestDataDir);
			expCmd = expCmd.ReplaceAll("USERNAME", username);
			return expCmd;
		}

		/// <summary>Display the summarized results</summary>
		private void DisplayResults()
		{
			Log.Info("Detailed results:");
			Log.Info("----------------------------------\n");
			for (int i = 0; i < testsFromConfigFile.Count; i++)
			{
				CLITestData td = testsFromConfigFile[i];
				bool testResult = td.GetTestResult();
				// Display the details only if there is a failure
				if (!testResult)
				{
					Log.Info("-------------------------------------------");
					Log.Info("                    Test ID: [" + (i + 1) + "]");
					Log.Info("           Test Description: [" + td.GetTestDesc() + "]");
					Log.Info(string.Empty);
					AList<CLICommand> testCommands = td.GetTestCommands();
					foreach (CLICommand cmd in testCommands)
					{
						Log.Info("              Test Commands: [" + ExpandCommand(cmd.GetCmd()) + "]");
					}
					Log.Info(string.Empty);
					AList<CLICommand> cleanupCommands = td.GetCleanupCommands();
					foreach (CLICommand cmd_1 in cleanupCommands)
					{
						Log.Info("           Cleanup Commands: [" + ExpandCommand(cmd_1.GetCmd()) + "]");
					}
					Log.Info(string.Empty);
					AList<ComparatorData> compdata = td.GetComparatorData();
					foreach (ComparatorData cd in compdata)
					{
						bool resultBoolean = cd.GetTestResult();
						Log.Info("                 Comparator: [" + cd.GetComparatorType() + "]");
						Log.Info("         Comparision result:   [" + (resultBoolean ? "pass" : "fail") +
							 "]");
						Log.Info("            Expected output:   [" + ExpandCommand(cd.GetExpectedOutput(
							)) + "]");
						Log.Info("              Actual output:   [" + cd.GetActualOutput() + "]");
					}
					Log.Info(string.Empty);
				}
			}
			Log.Info("Summary results:");
			Log.Info("----------------------------------\n");
			bool overallResults = true;
			int totalPass = 0;
			int totalFail = 0;
			int totalComparators = 0;
			for (int i_1 = 0; i_1 < testsFromConfigFile.Count; i_1++)
			{
				CLITestData td = testsFromConfigFile[i_1];
				totalComparators += testsFromConfigFile[i_1].GetComparatorData().Count;
				bool resultBoolean = td.GetTestResult();
				if (resultBoolean)
				{
					totalPass++;
				}
				else
				{
					totalFail++;
				}
				overallResults &= resultBoolean;
			}
			Log.Info("               Testing mode: " + testMode);
			Log.Info(string.Empty);
			Log.Info("             Overall result: " + (overallResults ? "+++ PASS +++" : "--- FAIL ---"
				));
			if ((totalPass + totalFail) == 0)
			{
				Log.Info("               # Tests pass: " + 0);
				Log.Info("               # Tests fail: " + 0);
			}
			else
			{
				Log.Info("               # Tests pass: " + totalPass + " (" + (100 * totalPass / 
					(totalPass + totalFail)) + "%)");
				Log.Info("               # Tests fail: " + totalFail + " (" + (100 * totalFail / 
					(totalPass + totalFail)) + "%)");
			}
			Log.Info("         # Validations done: " + totalComparators + " (each test may do multiple validations)"
				);
			Log.Info(string.Empty);
			Log.Info("Failing tests:");
			Log.Info("--------------");
			int i_2 = 0;
			bool foundTests = false;
			for (i_2 = 0; i_2 < testsFromConfigFile.Count; i_2++)
			{
				bool resultBoolean = testsFromConfigFile[i_2].GetTestResult();
				if (!resultBoolean)
				{
					Log.Info((i_2 + 1) + ": " + testsFromConfigFile[i_2].GetTestDesc());
					foundTests = true;
				}
			}
			if (!foundTests)
			{
				Log.Info("NONE");
			}
			foundTests = false;
			Log.Info(string.Empty);
			Log.Info("Passing tests:");
			Log.Info("--------------");
			for (i_2 = 0; i_2 < testsFromConfigFile.Count; i_2++)
			{
				bool resultBoolean = testsFromConfigFile[i_2].GetTestResult();
				if (resultBoolean)
				{
					Log.Info((i_2 + 1) + ": " + testsFromConfigFile[i_2].GetTestDesc());
					foundTests = true;
				}
			}
			if (!foundTests)
			{
				Log.Info("NONE");
			}
			Assert.True("One of the tests failed. " + "See the Detailed results to identify "
				 + "the command that failed", overallResults);
		}

		/// <summary>Compare the actual output with the expected output</summary>
		/// <param name="compdata"/>
		/// <returns/>
		private bool CompareTestOutput(ComparatorData compdata, CommandExecutor.Result cmdResult
			)
		{
			// Compare the output based on the comparator
			string comparatorType = compdata.GetComparatorType();
			Type comparatorClass = null;
			// If testMode is "test", then run the command and compare the output
			// If testMode is "nocompare", then run the command and dump the output.
			// Do not compare
			bool compareOutput = false;
			if (testMode.Equals(TestmodeTest))
			{
				try
				{
					// Initialize the comparator class and run its compare method
					comparatorClass = Sharpen.Runtime.GetType("org.apache.hadoop.cli.util." + comparatorType
						);
					ComparatorBase comp = (ComparatorBase)System.Activator.CreateInstance(comparatorClass
						);
					compareOutput = comp.Compare(cmdResult.GetCommandOutput(), ExpandCommand(compdata
						.GetExpectedOutput()));
				}
				catch (Exception e)
				{
					Log.Info("Error in instantiating the comparator" + e);
				}
			}
			return compareOutput;
		}

		/// <summary>TESTS RUNNER</summary>
		public virtual void TestAll()
		{
			Assert.True("Number of tests has to be greater then zero", testsFromConfigFile
				.Count > 0);
			Log.Info("TestAll");
			// Run the tests defined in the testConf.xml config file.
			for (int index = 0; index < testsFromConfigFile.Count; index++)
			{
				CLITestData testdata = testsFromConfigFile[index];
				// Execute the test commands
				AList<CLICommand> testCommands = testdata.GetTestCommands();
				CommandExecutor.Result cmdResult = null;
				foreach (CLICommand cmd in testCommands)
				{
					try
					{
						cmdResult = Execute(cmd);
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.Fail(StringUtils.StringifyException(e));
					}
				}
				bool overallTCResult = true;
				// Run comparators
				AList<ComparatorData> compdata = testdata.GetComparatorData();
				foreach (ComparatorData cd in compdata)
				{
					string comptype = cd.GetComparatorType();
					bool compareOutput = false;
					if (!Sharpen.Runtime.EqualsIgnoreCase(comptype, "none"))
					{
						compareOutput = CompareTestOutput(cd, cmdResult);
						overallTCResult &= compareOutput;
					}
					cd.SetExitCode(cmdResult.GetExitCode());
					cd.SetActualOutput(cmdResult.GetCommandOutput());
					cd.SetTestResult(compareOutput);
				}
				testdata.SetTestResult(overallTCResult);
				// Execute the cleanup commands
				AList<CLICommand> cleanupCommands = testdata.GetCleanupCommands();
				foreach (CLICommand cmd_1 in cleanupCommands)
				{
					try
					{
						Execute(cmd_1);
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.Fail(StringUtils.StringifyException(e));
					}
				}
			}
		}

		/// <summary>this method has to be overridden by an ancestor</summary>
		/// <exception cref="System.Exception"/>
		protected internal virtual CommandExecutor.Result Execute(CLICommand cmd)
		{
			throw new Exception("Unknown type of test command:" + cmd.GetType());
		}

		internal class TestConfigFileParser : DefaultHandler
		{
			internal string charString = null;

			internal CLITestData td = null;

			internal AList<CLICommand> testCommands = null;

			internal AList<CLICommand> cleanupCommands = null;

			internal bool runOnWindows = true;

			/*
			* Parser class for the test config xml file
			*/
			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartDocument()
			{
				this._enclosing.testsFromConfigFile = new AList<CLITestData>();
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void StartElement(string uri, string localName, string qName, Attributes
				 attributes)
			{
				if (qName.Equals("test"))
				{
					this.td = new CLITestData();
				}
				else
				{
					if (qName.Equals("test-commands"))
					{
						this.testCommands = new AList<CLICommand>();
					}
					else
					{
						if (qName.Equals("cleanup-commands"))
						{
							this.cleanupCommands = new AList<CLICommand>();
						}
						else
						{
							if (qName.Equals("comparators"))
							{
								this._enclosing.testComparators = new AList<ComparatorData>();
							}
							else
							{
								if (qName.Equals("comparator"))
								{
									this._enclosing.comparatorData = new ComparatorData();
								}
							}
						}
					}
				}
				this.charString = string.Empty;
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void EndElement(string uri, string localName, string qName)
			{
				if (qName.Equals("description"))
				{
					this.td.SetTestDesc(this.charString);
				}
				else
				{
					if (qName.Equals("windows"))
					{
						this.runOnWindows = System.Boolean.Parse(this.charString);
					}
					else
					{
						if (qName.Equals("test-commands"))
						{
							this.td.SetTestCommands(this.testCommands);
							this.testCommands = null;
						}
						else
						{
							if (qName.Equals("cleanup-commands"))
							{
								this.td.SetCleanupCommands(this.cleanupCommands);
								this.cleanupCommands = null;
							}
							else
							{
								if (qName.Equals("command"))
								{
									if (this.testCommands != null)
									{
										this.testCommands.AddItem(new CLITestCmd(this.charString, new CLICommandFS()));
									}
									else
									{
										if (this.cleanupCommands != null)
										{
											this.cleanupCommands.AddItem(new CLITestCmd(this.charString, new CLICommandFS()));
										}
									}
								}
								else
								{
									if (qName.Equals("comparators"))
									{
										this.td.SetComparatorData(this._enclosing.testComparators);
									}
									else
									{
										if (qName.Equals("comparator"))
										{
											this._enclosing.testComparators.AddItem(this._enclosing.comparatorData);
										}
										else
										{
											if (qName.Equals("type"))
											{
												this._enclosing.comparatorData.SetComparatorType(this.charString);
											}
											else
											{
												if (qName.Equals("expected-output"))
												{
													this._enclosing.comparatorData.SetExpectedOutput(this.charString);
												}
												else
												{
													if (qName.Equals("test"))
													{
														if (!Shell.Windows || this.runOnWindows)
														{
															this._enclosing.testsFromConfigFile.AddItem(this.td);
														}
														this.td = null;
														this.runOnWindows = true;
													}
													else
													{
														if (qName.Equals("mode"))
														{
															this._enclosing.testMode = this.charString;
															if (!this._enclosing.testMode.Equals(CLITestHelper.TestmodeNocompare) && !this._enclosing
																.testMode.Equals(CLITestHelper.TestmodeTest))
															{
																this._enclosing.testMode = CLITestHelper.TestmodeTest;
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			public override void Characters(char[] ch, int start, int length)
			{
				string s = new string(ch, start, length);
				this.charString += s;
			}

			internal TestConfigFileParser(CLITestHelper _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CLITestHelper _enclosing;
		}
	}
}
