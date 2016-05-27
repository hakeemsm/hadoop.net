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
using System;
using NUnit.Framework;
using NUnit.Framework.Internal;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>This is the base class for all the contract tests</summary>
	public abstract class AbstractFSContractTestBase : Assert, ContractOptions
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractFSContractTestBase
			));

		/// <summary>
		/// Length of files to work with:
		/// <value/>
		/// </summary>
		public const int TestFileLen = 1024;

		/// <summary>
		/// standard test timeout:
		/// <value/>
		/// </summary>
		public const int DefaultTestTimeout = 180 * 1000;

		/// <summary>The FS contract used for these tets</summary>
		private AbstractFSContract contract;

		/// <summary>The test filesystem extracted from it</summary>
		private FileSystem fileSystem;

		/// <summary>The path for tests</summary>
		private Org.Apache.Hadoop.FS.Path testPath;

		/// <summary>
		/// This must be implemented by all instantiated test cases
		/// -provide the FS contract
		/// </summary>
		/// <returns>the FS contract</returns>
		protected internal abstract AbstractFSContract CreateContract(Configuration conf);

		/// <summary>Get the contract</summary>
		/// <returns>
		/// the contract, which will be non-null once the setup operation has
		/// succeeded
		/// </returns>
		protected internal virtual AbstractFSContract GetContract()
		{
			return contract;
		}

		/// <summary>Get the filesystem created in startup</summary>
		/// <returns>the filesystem to use for tests</returns>
		public virtual FileSystem GetFileSystem()
		{
			return fileSystem;
		}

		/// <summary>Get the log of the base class</summary>
		/// <returns>a logger</returns>
		public static Logger GetLog()
		{
			return Log;
		}

		/// <summary>Skip a test if a feature is unsupported in this FS</summary>
		/// <param name="feature">feature to look for</param>
		/// <exception cref="System.IO.IOException">IO problem</exception>
		protected internal virtual void SkipIfUnsupported(string feature)
		{
			if (!IsSupported(feature))
			{
				ContractTestUtils.Skip("Skipping as unsupported feature: " + feature);
			}
		}

		/// <summary>Is a feature supported?</summary>
		/// <param name="feature">feature</param>
		/// <returns>true iff the feature is supported</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual bool IsSupported(string feature)
		{
			return contract.IsSupported(feature, false);
		}

		/// <summary>Include at the start of tests to skip them if the FS is not enabled.</summary>
		protected internal virtual void AssumeEnabled()
		{
			if (!contract.IsEnabled())
			{
				throw new AssumptionViolatedException("test cases disabled for " + contract);
			}
		}

		/// <summary>Create a configuration.</summary>
		/// <remarks>Create a configuration. May be overridden by tests/instantiations</remarks>
		/// <returns>a configuration</returns>
		protected internal virtual Configuration CreateConfiguration()
		{
			return new Configuration();
		}

		/// <summary>Set the timeout for every test</summary>
		[Rule]
		public Timeout testTimeout;

		/// <summary>Option for tests to override the default timeout value</summary>
		/// <returns>the current test timeout</returns>
		protected internal virtual int GetTestTimeoutMillis()
		{
			return DefaultTestTimeout;
		}

		/// <summary>Setup: create the contract then init it</summary>
		/// <exception cref="System.Exception">on any failure</exception>
		[SetUp]
		public virtual void Setup()
		{
			contract = CreateContract(CreateConfiguration());
			contract.Init();
			//skip tests if they aren't enabled
			AssumeEnabled();
			//extract the test FS
			fileSystem = contract.GetTestFileSystem();
			NUnit.Framework.Assert.IsNotNull("null filesystem", fileSystem);
			URI fsURI = fileSystem.GetUri();
			Log.Info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
			//sanity check to make sure that the test FS picked up really matches
			//the scheme chosen. This is to avoid defaulting back to the localFS
			//which would be drastic for root FS tests
			NUnit.Framework.Assert.AreEqual("wrong filesystem of " + fsURI, contract.GetScheme
				(), fsURI.GetScheme());
			//create the test path
			testPath = GetContract().GetTestPath();
			Mkdirs(testPath);
		}

		/// <summary>Teardown</summary>
		/// <exception cref="System.Exception">on any failure</exception>
		[TearDown]
		public virtual void Teardown()
		{
			DeleteTestDirInTeardown();
		}

		/// <summary>Delete the test dir in the per-test teardown</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void DeleteTestDirInTeardown()
		{
			ContractTestUtils.Cleanup("TEARDOWN", GetFileSystem(), testPath);
		}

		/// <summary>
		/// Create a path under the test path provided by
		/// the FS contract
		/// </summary>
		/// <param name="filepath">path string in</param>
		/// <returns>a path qualified by the test filesystem</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual Org.Apache.Hadoop.FS.Path Path(string filepath)
		{
			return GetFileSystem().MakeQualified(new Org.Apache.Hadoop.FS.Path(GetContract().
				GetTestPath(), filepath));
		}

		/// <summary>
		/// Take a simple path like "/something" and turn it into
		/// a qualified path against the test FS
		/// </summary>
		/// <param name="filepath">path string in</param>
		/// <returns>a path qualified by the test filesystem</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual Org.Apache.Hadoop.FS.Path Absolutepath(string filepath
			)
		{
			return GetFileSystem().MakeQualified(new Org.Apache.Hadoop.FS.Path(filepath));
		}

		/// <summary>List a path in the test FS</summary>
		/// <param name="path">path to list</param>
		/// <returns>the contents of the path/dir</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual string Ls(Org.Apache.Hadoop.FS.Path path)
		{
			return ContractTestUtils.Ls(fileSystem, path);
		}

		/// <summary>Describe a test.</summary>
		/// <remarks>
		/// Describe a test. This is a replacement for javadocs
		/// where the tests role is printed in the log output
		/// </remarks>
		/// <param name="text">description</param>
		protected internal virtual void Describe(string text)
		{
			Log.Info(text);
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
		protected internal virtual void HandleRelaxedException(string action, string expectedException
			, Exception e)
		{
			if (GetContract().IsSupported(SupportsStrictExceptions, false))
			{
				throw e;
			}
			Log.Warn("The expected exception {}  was not the exception class" + " raised on {}: {}"
				, action, e.GetType(), expectedException, e);
		}

		/// <summary>Handle expected exceptions through logging and/or other actions</summary>
		/// <param name="e">exception raised.</param>
		protected internal virtual void HandleExpectedException(Exception e)
		{
			GetLog().Debug("expected :{}", e, e);
		}

		/// <summary>assert that a path exists</summary>
		/// <param name="message">message to use in an assertion</param>
		/// <param name="path">path to probe</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void AssertPathExists(string message, Org.Apache.Hadoop.FS.Path path
			)
		{
			ContractTestUtils.AssertPathExists(fileSystem, message, path);
		}

		/// <summary>assert that a path does not</summary>
		/// <param name="message">message to use in an assertion</param>
		/// <param name="path">path to probe</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void AssertPathDoesNotExist(string message, Org.Apache.Hadoop.FS.Path
			 path)
		{
			ContractTestUtils.AssertPathDoesNotExist(fileSystem, message, path);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="filename">name of the file</param>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void AssertIsFile(Org.Apache.Hadoop.FS.Path filename)
		{
			ContractTestUtils.AssertIsFile(fileSystem, filename);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <param name="path">name of the file</param>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void AssertIsDirectory(Org.Apache.Hadoop.FS.Path path)
		{
			ContractTestUtils.AssertIsDirectory(fileSystem, path);
		}

		/// <summary>
		/// Assert that a file exists and whose
		/// <see cref="Org.Apache.Hadoop.FS.FileStatus"/>
		/// entry
		/// declares that this is a file and not a symlink or directory.
		/// </summary>
		/// <exception cref="System.IO.IOException">IO problems during file operations</exception>
		protected internal virtual void Mkdirs(Org.Apache.Hadoop.FS.Path path)
		{
			NUnit.Framework.Assert.IsTrue("Failed to mkdir " + path, fileSystem.Mkdirs(path));
		}

		/// <summary>Assert that a delete succeeded</summary>
		/// <param name="path">path to delete</param>
		/// <param name="recursive">recursive flag</param>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		protected internal virtual void AssertDeleted(Org.Apache.Hadoop.FS.Path path, bool
			 recursive)
		{
			ContractTestUtils.AssertDeleted(fileSystem, path, recursive);
		}

		/// <summary>
		/// Assert that the result value == -1; which implies
		/// that a read was successful
		/// </summary>
		/// <param name="text">text to include in a message (usually the operation)</param>
		/// <param name="result">read result to validate</param>
		protected internal virtual void AssertMinusOne(string text, int result)
		{
			NUnit.Framework.Assert.AreEqual(text + " wrong read result " + result, -1, result
				);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool Rename(Org.Apache.Hadoop.FS.Path src, Org.Apache.Hadoop.FS.Path
			 dst)
		{
			return GetFileSystem().Rename(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string GenerateAndLogErrorListing(Org.Apache.Hadoop.FS.Path
			 src, Org.Apache.Hadoop.FS.Path dst)
		{
			FileSystem fs = GetFileSystem();
			GetLog().Error("src dir " + ContractTestUtils.Ls(fs, src.GetParent()));
			string destDirLS = ContractTestUtils.Ls(fs, dst.GetParent());
			if (fs.IsDirectory(dst))
			{
				//include the dir into the listing
				destDirLS = destDirLS + "\n" + ContractTestUtils.Ls(fs, dst);
			}
			return destDirLS;
		}

		public AbstractFSContractTestBase()
		{
			testTimeout = new Timeout(GetTestTimeoutMillis());
		}
	}
}
