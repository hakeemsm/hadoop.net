/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.fs.contract
{
	/// <summary>This is the base class for all the contract tests</summary>
	public abstract class AbstractFSContractTestBase : NUnit.Framework.Assert, org.apache.hadoop.fs.contract.ContractOptions
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractFSContractTestBase
			)));

		/// <summary>
		/// Length of files to work with:
		/// <value/>
		/// </summary>
		public const int TEST_FILE_LEN = 1024;

		/// <summary>
		/// standard test timeout:
		/// <value/>
		/// </summary>
		public const int DEFAULT_TEST_TIMEOUT = 180 * 1000;

		/// <summary>The FS contract used for these tets</summary>
		private org.apache.hadoop.fs.contract.AbstractFSContract contract;

		/// <summary>The test filesystem extracted from it</summary>
		private org.apache.hadoop.fs.FileSystem fileSystem;

		/// <summary>The path for tests</summary>
		private org.apache.hadoop.fs.Path testPath;

		/// <summary>
		/// This must be implemented by all instantiated test cases
		/// -provide the FS contract
		/// </summary>
		/// <returns>the FS contract</returns>
		protected internal abstract org.apache.hadoop.fs.contract.AbstractFSContract createContract
			(org.apache.hadoop.conf.Configuration conf);

		/// <summary>Get the contract</summary>
		/// <returns>
		/// the contract, which will be non-null once the setup operation has
		/// succeeded
		/// </returns>
		protected internal virtual org.apache.hadoop.fs.contract.AbstractFSContract getContract
			()
		{
			return contract;
		}

		/// <summary>Get the filesystem created in startup</summary>
		/// <returns>the filesystem to use for tests</returns>
		public virtual org.apache.hadoop.fs.FileSystem getFileSystem()
		{
			return fileSystem;
		}

		/// <summary>Get the log of the base class</summary>
		/// <returns>a logger</returns>
		public static org.slf4j.Logger getLog()
		{
			return LOG;
		}

		/// <summary>Skip a test if a feature is unsupported in this FS</summary>
		/// <param name="feature">feature to look for</param>
		/// <exception cref="System.IO.IOException">IO problem</exception>
		protected internal virtual void skipIfUnsupported(string feature)
		{
			if (!isSupported(feature))
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.skip("Skipping as unsupported feature: "
					 + feature);
			}
		}

		/// <summary>Is a feature supported?</summary>
		/// <param name="feature">feature</param>
		/// <returns>true iff the feature is supported</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual bool isSupported(string feature)
		{
			return contract.isSupported(feature, false);
		}

		/// <summary>Include at the start of tests to skip them if the FS is not enabled.</summary>
		protected internal virtual void assumeEnabled()
		{
			if (!contract.isEnabled())
			{
				throw new NUnit.Framework.@internal.AssumptionViolatedException("test cases disabled for "
					 + contract);
			}
		}

		/// <summary>Create a configuration.</summary>
		/// <remarks>Create a configuration. May be overridden by tests/instantiations</remarks>
		/// <returns>a configuration</returns>
		protected internal virtual org.apache.hadoop.conf.Configuration createConfiguration
			()
		{
			return new org.apache.hadoop.conf.Configuration();
		}

		/// <summary>Set the timeout for every test</summary>
		[NUnit.Framework.Rule]
		public NUnit.Framework.rules.Timeout testTimeout;

		/// <summary>Option for tests to override the default timeout value</summary>
		/// <returns>the current test timeout</returns>
		protected internal virtual int getTestTimeoutMillis()
		{
			return DEFAULT_TEST_TIMEOUT;
		}

		/// <summary>Setup: create the contract then init it</summary>
		/// <exception cref="System.Exception">on any failure</exception>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			contract = createContract(createConfiguration());
			contract.init();
			//skip tests if they aren't enabled
			assumeEnabled();
			//extract the test FS
			fileSystem = contract.getTestFileSystem();
			NUnit.Framework.Assert.IsNotNull("null filesystem", fileSystem);
			java.net.URI fsURI = fileSystem.getUri();
			LOG.info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
			//sanity check to make sure that the test FS picked up really matches
			//the scheme chosen. This is to avoid defaulting back to the localFS
			//which would be drastic for root FS tests
			NUnit.Framework.Assert.AreEqual("wrong filesystem of " + fsURI, contract.getScheme
				(), fsURI.getScheme());
			//create the test path
			testPath = getContract().getTestPath();
			mkdirs(testPath);
		}

		/// <summary>Teardown</summary>
		/// <exception cref="System.Exception">on any failure</exception>
		[NUnit.Framework.TearDown]
		public virtual void teardown()
		{
			deleteTestDirInTeardown();
		}

		/// <summary>Delete the test dir in the per-test teardown</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void deleteTestDirInTeardown()
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.cleanup("TEARDOWN", getFileSystem
				(), testPath);
		}

		/// <summary>
		/// Create a path under the test path provided by
		/// the FS contract
		/// </summary>
		/// <param name="filepath">path string in</param>
		/// <returns>a path qualified by the test filesystem</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual org.apache.hadoop.fs.Path path(string filepath)
		{
			return getFileSystem().makeQualified(new org.apache.hadoop.fs.Path(getContract().
				getTestPath(), filepath));
		}

		/// <summary>
		/// Take a simple path like "/something" and turn it into
		/// a qualified path against the test FS
		/// </summary>
		/// <param name="filepath">path string in</param>
		/// <returns>a path qualified by the test filesystem</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual org.apache.hadoop.fs.Path absolutepath(string filepath
			)
		{
			return getFileSystem().makeQualified(new org.apache.hadoop.fs.Path(filepath));
		}

		/// <summary>List a path in the test FS</summary>
		/// <param name="path">path to list</param>
		/// <returns>the contents of the path/dir</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual string ls(org.apache.hadoop.fs.Path path)
		{
			return org.apache.hadoop.fs.contract.ContractTestUtils.ls(fileSystem, path);
		}

		/// <summary>Describe a test.</summary>
		/// <remarks>
		/// Describe a test. This is a replacement for javadocs
		/// where the tests role is printed in the log output
		/// </remarks>
		/// <param name="text">description</param>
		protected internal virtual void describe(string text)
		{
			LOG.info(text);
		}

		/// <summary>
		/// Handle the outcome of an operation not being the strictest
		/// exception desired, but one that, while still within the boundary
		/// of the contract, is a bit looser.
		/// </summary>
		/// <remarks>
		/// Handle the outcome of an operation not being the strictest
		/// exception desired, but one that, while still within the boundary
		/// of the contract, is a bit looser.
		/// If the FS contract says that they support the strictest exceptions,
		/// that is what they must return, and the exception here is rethrown
		/// </remarks>
		/// <param name="action">Action</param>
		/// <param name="expectedException">what was expected</param>
		/// <param name="e">exception that was received</param>
		/// <exception cref="System.Exception"/>
		protected internal virtual void handleRelaxedException(string action, string expectedException
			, System.Exception e)
		{
			if (getContract().isSupported(SUPPORTS_STRICT_EXCEPTIONS, false))
			{
				throw e;
			}
			LOG.warn("The expected exception {}  was not the exception class" + " raised on {}: {}"
				, action, Sharpen.Runtime.getClassForObject(e), expectedException, e);
		}

		/// <summary>Handle expected exceptions through logging and/or other actions</summary>
		/// <param name="e">exception raised.</param>
		protected internal virtual void handleExpectedException(System.Exception e)
		{
			getLog().debug("expected :{}", e, e);
		}

		/// <summary>assert that a path exists</summary>
		/// <param name="message">message to use in an assertion</param>
		/// <param name="path">path to probe</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void assertPathExists(string message, org.apache.hadoop.fs.Path path
			)
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fileSystem, message
				, path);
		}

		/// <summary>assert that a path does not</summary>
		/// <param name="message">message to use in an assertion</param>
		/// <param name="path">path to probe</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void assertPathDoesNotExist(string message, org.apache.hadoop.fs.Path
			 path)
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(fileSystem
				, message, path);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="org.apache.hadoop.fs.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="filename">name of the file</param>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void assertIsFile(org.apache.hadoop.fs.Path filename)
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile(fileSystem, filename
				);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="org.apache.hadoop.fs.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="path">name of the file</param>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void assertIsDirectory(org.apache.hadoop.fs.Path path)
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(fileSystem, path
				);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="org.apache.hadoop.fs.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void mkdirs(org.apache.hadoop.fs.Path path)
		{
			NUnit.Framework.Assert.IsTrue("Failed to mkdir " + path, fileSystem.mkdirs(path));
		}

		/// <summary>Assert that a delete succeeded</summary>
		/// <param name="path">path to delete</param>
		/// <param name="recursive">recursive flag</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual void assertDeleted(org.apache.hadoop.fs.Path path, bool
			 recursive)
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted(fileSystem, path, recursive
				);
		}

		/// <summary>
		/// Assert that the result value == -1; which implies
		/// that a read was successful
		/// </summary>
		/// <param name="text">text to include in a message (usually the operation)</param>
		/// <param name="result">read result to validate</param>
		protected internal virtual void assertMinusOne(string text, int result)
		{
			NUnit.Framework.Assert.AreEqual(text + " wrong read result " + result, -1, result
				);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			return getFileSystem().rename(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string generateAndLogErrorListing(org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			getLog().error("src dir " + org.apache.hadoop.fs.contract.ContractTestUtils.ls(fs
				, src.getParent()));
			string destDirLS = org.apache.hadoop.fs.contract.ContractTestUtils.ls(fs, dst.getParent
				());
			if (fs.isDirectory(dst))
			{
				//include the dir into the listing
				destDirLS = destDirLS + "\n" + org.apache.hadoop.fs.contract.ContractTestUtils.ls
					(fs, dst);
			}
			return destDirLS;
		}

		public AbstractFSContractTestBase()
		{
			testTimeout = new NUnit.Framework.rules.Timeout(getTestTimeoutMillis());
		}
	}
}
