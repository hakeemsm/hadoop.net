using Sharpen;

namespace org.apache.hadoop.cli
{
	/// <summary>Tests for the Command Line Interface (CLI)</summary>
	public class CLITestHelper
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.cli.CLITestHelper
			)).getName());

		public const string TESTMODE_TEST = "test";

		public const string TESTMODE_NOCOMPARE = "nocompare";

		public static readonly string TEST_CACHE_DATA_DIR = Sharpen.Runtime.getProperty("test.cache.data"
			, "build/test/cache");

		protected internal string testMode = TESTMODE_TEST;

		protected internal System.Collections.Generic.List<org.apache.hadoop.cli.util.CLITestData
			> testsFromConfigFile = null;

		protected internal System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData
			> testComparators = null;

		protected internal string thisTestCaseName = null;

		protected internal org.apache.hadoop.cli.util.ComparatorData comparatorData = null;

		protected internal org.apache.hadoop.conf.Configuration conf = null;

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
		protected internal virtual void readTestConfigFile()
		{
			string testConfigFile = getTestFile();
			if (testsFromConfigFile == null)
			{
				bool success = false;
				testConfigFile = TEST_CACHE_DATA_DIR + java.io.File.separator + testConfigFile;
				try
				{
					javax.xml.parsers.SAXParser p = (javax.xml.parsers.SAXParserFactory.newInstance()
						).newSAXParser();
					p.parse(testConfigFile, getConfigParser());
					success = true;
				}
				catch (System.Exception)
				{
					LOG.info("File: " + testConfigFile + " not found");
					success = false;
				}
				NUnit.Framework.Assert.IsTrue("Error reading test config file", success);
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
		protected internal virtual org.apache.hadoop.cli.CLITestHelper.TestConfigFileParser
			 getConfigParser()
		{
			return new org.apache.hadoop.cli.CLITestHelper.TestConfigFileParser(this);
		}

		protected internal virtual string getTestFile()
		{
			return string.Empty;
		}

		/*
		* Setup
		*/
		/// <exception cref="System.Exception"/>
		public virtual void setUp()
		{
			// Read the testConfig.xml file
			readTestConfigFile();
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, true);
			clitestDataDir = new java.io.File(TEST_CACHE_DATA_DIR).toURI().ToString().Replace
				(' ', '+');
		}

		/// <summary>Tear down</summary>
		/// <exception cref="System.Exception"/>
		public virtual void tearDown()
		{
			displayResults();
		}

		/// <summary>Expand the commands from the test config xml file</summary>
		/// <param name="cmd"/>
		/// <returns>String expanded command</returns>
		protected internal virtual string expandCommand(string cmd)
		{
			string expCmd = cmd;
			expCmd = expCmd.replaceAll("CLITEST_DATA", clitestDataDir);
			expCmd = expCmd.replaceAll("USERNAME", username);
			return expCmd;
		}

		/// <summary>Display the summarized results</summary>
		private void displayResults()
		{
			LOG.info("Detailed results:");
			LOG.info("----------------------------------\n");
			for (int i = 0; i < testsFromConfigFile.Count; i++)
			{
				org.apache.hadoop.cli.util.CLITestData td = testsFromConfigFile[i];
				bool testResult = td.getTestResult();
				// Display the details only if there is a failure
				if (!testResult)
				{
					LOG.info("-------------------------------------------");
					LOG.info("                    Test ID: [" + (i + 1) + "]");
					LOG.info("           Test Description: [" + td.getTestDesc() + "]");
					LOG.info(string.Empty);
					System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> testCommands
						 = td.getTestCommands();
					foreach (org.apache.hadoop.cli.util.CLICommand cmd in testCommands)
					{
						LOG.info("              Test Commands: [" + expandCommand(cmd.getCmd()) + "]");
					}
					LOG.info(string.Empty);
					System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> cleanupCommands
						 = td.getCleanupCommands();
					foreach (org.apache.hadoop.cli.util.CLICommand cmd_1 in cleanupCommands)
					{
						LOG.info("           Cleanup Commands: [" + expandCommand(cmd_1.getCmd()) + "]");
					}
					LOG.info(string.Empty);
					System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData> compdata
						 = td.getComparatorData();
					foreach (org.apache.hadoop.cli.util.ComparatorData cd in compdata)
					{
						bool resultBoolean = cd.getTestResult();
						LOG.info("                 Comparator: [" + cd.getComparatorType() + "]");
						LOG.info("         Comparision result:   [" + (resultBoolean ? "pass" : "fail") +
							 "]");
						LOG.info("            Expected output:   [" + expandCommand(cd.getExpectedOutput(
							)) + "]");
						LOG.info("              Actual output:   [" + cd.getActualOutput() + "]");
					}
					LOG.info(string.Empty);
				}
			}
			LOG.info("Summary results:");
			LOG.info("----------------------------------\n");
			bool overallResults = true;
			int totalPass = 0;
			int totalFail = 0;
			int totalComparators = 0;
			for (int i_1 = 0; i_1 < testsFromConfigFile.Count; i_1++)
			{
				org.apache.hadoop.cli.util.CLITestData td = testsFromConfigFile[i_1];
				totalComparators += testsFromConfigFile[i_1].getComparatorData().Count;
				bool resultBoolean = td.getTestResult();
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
			LOG.info("               Testing mode: " + testMode);
			LOG.info(string.Empty);
			LOG.info("             Overall result: " + (overallResults ? "+++ PASS +++" : "--- FAIL ---"
				));
			if ((totalPass + totalFail) == 0)
			{
				LOG.info("               # Tests pass: " + 0);
				LOG.info("               # Tests fail: " + 0);
			}
			else
			{
				LOG.info("               # Tests pass: " + totalPass + " (" + (100 * totalPass / 
					(totalPass + totalFail)) + "%)");
				LOG.info("               # Tests fail: " + totalFail + " (" + (100 * totalFail / 
					(totalPass + totalFail)) + "%)");
			}
			LOG.info("         # Validations done: " + totalComparators + " (each test may do multiple validations)"
				);
			LOG.info(string.Empty);
			LOG.info("Failing tests:");
			LOG.info("--------------");
			int i_2 = 0;
			bool foundTests = false;
			for (i_2 = 0; i_2 < testsFromConfigFile.Count; i_2++)
			{
				bool resultBoolean = testsFromConfigFile[i_2].getTestResult();
				if (!resultBoolean)
				{
					LOG.info((i_2 + 1) + ": " + testsFromConfigFile[i_2].getTestDesc());
					foundTests = true;
				}
			}
			if (!foundTests)
			{
				LOG.info("NONE");
			}
			foundTests = false;
			LOG.info(string.Empty);
			LOG.info("Passing tests:");
			LOG.info("--------------");
			for (i_2 = 0; i_2 < testsFromConfigFile.Count; i_2++)
			{
				bool resultBoolean = testsFromConfigFile[i_2].getTestResult();
				if (resultBoolean)
				{
					LOG.info((i_2 + 1) + ": " + testsFromConfigFile[i_2].getTestDesc());
					foundTests = true;
				}
			}
			if (!foundTests)
			{
				LOG.info("NONE");
			}
			NUnit.Framework.Assert.IsTrue("One of the tests failed. " + "See the Detailed results to identify "
				 + "the command that failed", overallResults);
		}

		/// <summary>Compare the actual output with the expected output</summary>
		/// <param name="compdata"/>
		/// <returns/>
		private bool compareTestOutput(org.apache.hadoop.cli.util.ComparatorData compdata
			, org.apache.hadoop.cli.util.CommandExecutor.Result cmdResult)
		{
			// Compare the output based on the comparator
			string comparatorType = compdata.getComparatorType();
			java.lang.Class comparatorClass = null;
			// If testMode is "test", then run the command and compare the output
			// If testMode is "nocompare", then run the command and dump the output.
			// Do not compare
			bool compareOutput = false;
			if (testMode.Equals(TESTMODE_TEST))
			{
				try
				{
					// Initialize the comparator class and run its compare method
					comparatorClass = java.lang.Class.forName("org.apache.hadoop.cli.util." + comparatorType
						);
					org.apache.hadoop.cli.util.ComparatorBase comp = (org.apache.hadoop.cli.util.ComparatorBase
						)comparatorClass.newInstance();
					compareOutput = comp.compare(cmdResult.getCommandOutput(), expandCommand(compdata
						.getExpectedOutput()));
				}
				catch (System.Exception e)
				{
					LOG.info("Error in instantiating the comparator" + e);
				}
			}
			return compareOutput;
		}

		/// <summary>TESTS RUNNER</summary>
		public virtual void testAll()
		{
			NUnit.Framework.Assert.IsTrue("Number of tests has to be greater then zero", testsFromConfigFile
				.Count > 0);
			LOG.info("TestAll");
			// Run the tests defined in the testConf.xml config file.
			for (int index = 0; index < testsFromConfigFile.Count; index++)
			{
				org.apache.hadoop.cli.util.CLITestData testdata = testsFromConfigFile[index];
				// Execute the test commands
				System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> testCommands
					 = testdata.getTestCommands();
				org.apache.hadoop.cli.util.CommandExecutor.Result cmdResult = null;
				foreach (org.apache.hadoop.cli.util.CLICommand cmd in testCommands)
				{
					try
					{
						cmdResult = execute(cmd);
					}
					catch (System.Exception e)
					{
						NUnit.Framework.Assert.Fail(org.apache.hadoop.util.StringUtils.stringifyException
							(e));
					}
				}
				bool overallTCResult = true;
				// Run comparators
				System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData> compdata
					 = testdata.getComparatorData();
				foreach (org.apache.hadoop.cli.util.ComparatorData cd in compdata)
				{
					string comptype = cd.getComparatorType();
					bool compareOutput = false;
					if (!Sharpen.Runtime.equalsIgnoreCase(comptype, "none"))
					{
						compareOutput = compareTestOutput(cd, cmdResult);
						overallTCResult &= compareOutput;
					}
					cd.setExitCode(cmdResult.getExitCode());
					cd.setActualOutput(cmdResult.getCommandOutput());
					cd.setTestResult(compareOutput);
				}
				testdata.setTestResult(overallTCResult);
				// Execute the cleanup commands
				System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> cleanupCommands
					 = testdata.getCleanupCommands();
				foreach (org.apache.hadoop.cli.util.CLICommand cmd_1 in cleanupCommands)
				{
					try
					{
						execute(cmd_1);
					}
					catch (System.Exception e)
					{
						NUnit.Framework.Assert.Fail(org.apache.hadoop.util.StringUtils.stringifyException
							(e));
					}
				}
			}
		}

		/// <summary>this method has to be overridden by an ancestor</summary>
		/// <exception cref="System.Exception"/>
		protected internal virtual org.apache.hadoop.cli.util.CommandExecutor.Result execute
			(org.apache.hadoop.cli.util.CLICommand cmd)
		{
			throw new System.Exception("Unknown type of test command:" + cmd.getType());
		}

		internal class TestConfigFileParser : org.xml.sax.helpers.DefaultHandler
		{
			internal string charString = null;

			internal org.apache.hadoop.cli.util.CLITestData td = null;

			internal System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> testCommands
				 = null;

			internal System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand> cleanupCommands
				 = null;

			internal bool runOnWindows = true;

			/*
			* Parser class for the test config xml file
			*/
			/// <exception cref="org.xml.sax.SAXException"/>
			public override void startDocument()
			{
				this._enclosing.testsFromConfigFile = new System.Collections.Generic.List<org.apache.hadoop.cli.util.CLITestData
					>();
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void startElement(string uri, string localName, string qName, org.xml.sax.Attributes
				 attributes)
			{
				if (qName.Equals("test"))
				{
					this.td = new org.apache.hadoop.cli.util.CLITestData();
				}
				else
				{
					if (qName.Equals("test-commands"))
					{
						this.testCommands = new System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
							>();
					}
					else
					{
						if (qName.Equals("cleanup-commands"))
						{
							this.cleanupCommands = new System.Collections.Generic.List<org.apache.hadoop.cli.util.CLICommand
								>();
						}
						else
						{
							if (qName.Equals("comparators"))
							{
								this._enclosing.testComparators = new System.Collections.Generic.List<org.apache.hadoop.cli.util.ComparatorData
									>();
							}
							else
							{
								if (qName.Equals("comparator"))
								{
									this._enclosing.comparatorData = new org.apache.hadoop.cli.util.ComparatorData();
								}
							}
						}
					}
				}
				this.charString = string.Empty;
			}

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void endElement(string uri, string localName, string qName)
			{
				if (qName.Equals("description"))
				{
					this.td.setTestDesc(this.charString);
				}
				else
				{
					if (qName.Equals("windows"))
					{
						this.runOnWindows = bool.parseBoolean(this.charString);
					}
					else
					{
						if (qName.Equals("test-commands"))
						{
							this.td.setTestCommands(this.testCommands);
							this.testCommands = null;
						}
						else
						{
							if (qName.Equals("cleanup-commands"))
							{
								this.td.setCleanupCommands(this.cleanupCommands);
								this.cleanupCommands = null;
							}
							else
							{
								if (qName.Equals("command"))
								{
									if (this.testCommands != null)
									{
										this.testCommands.add(new org.apache.hadoop.cli.util.CLITestCmd(this.charString, 
											new org.apache.hadoop.cli.util.CLICommandFS()));
									}
									else
									{
										if (this.cleanupCommands != null)
										{
											this.cleanupCommands.add(new org.apache.hadoop.cli.util.CLITestCmd(this.charString
												, new org.apache.hadoop.cli.util.CLICommandFS()));
										}
									}
								}
								else
								{
									if (qName.Equals("comparators"))
									{
										this.td.setComparatorData(this._enclosing.testComparators);
									}
									else
									{
										if (qName.Equals("comparator"))
										{
											this._enclosing.testComparators.add(this._enclosing.comparatorData);
										}
										else
										{
											if (qName.Equals("type"))
											{
												this._enclosing.comparatorData.setComparatorType(this.charString);
											}
											else
											{
												if (qName.Equals("expected-output"))
												{
													this._enclosing.comparatorData.setExpectedOutput(this.charString);
												}
												else
												{
													if (qName.Equals("test"))
													{
														if (!org.apache.hadoop.util.Shell.WINDOWS || this.runOnWindows)
														{
															this._enclosing.testsFromConfigFile.add(this.td);
														}
														this.td = null;
														this.runOnWindows = true;
													}
													else
													{
														if (qName.Equals("mode"))
														{
															this._enclosing.testMode = this.charString;
															if (!this._enclosing.testMode.Equals(org.apache.hadoop.cli.CLITestHelper.TESTMODE_NOCOMPARE
																) && !this._enclosing.testMode.Equals(org.apache.hadoop.cli.CLITestHelper.TESTMODE_TEST
																))
															{
																this._enclosing.testMode = org.apache.hadoop.cli.CLITestHelper.TESTMODE_TEST;
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

			/// <exception cref="org.xml.sax.SAXException"/>
			public override void characters(char[] ch, int start, int length)
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
