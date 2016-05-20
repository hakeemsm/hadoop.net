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
	/// <summary>This class does things to the root directory.</summary>
	/// <remarks>
	/// This class does things to the root directory.
	/// Only subclass this for tests against transient filesystems where
	/// you don't care about the data.
	/// </remarks>
	public abstract class AbstractContractRootDirectoryTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest
			)));

		/// <exception cref="System.Exception"/>
		public override void setup()
		{
			base.setup();
			skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkDirDepth1()
		{
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path("/testmkdirdepth1");
			assertPathDoesNotExist("directory already exists", dir);
			fs.mkdirs(dir);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, dir);
			assertPathExists("directory already exists", dir);
			assertDeleted(dir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRmEmptyRootDirNonRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
			bool deleted = getFileSystem().delete(root, true);
			LOG.info("rm / of empty dir result is {}", deleted);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRmNonEmptyRootDirNonRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
			string touchfile = "/testRmNonEmptyRootDirNonRecursive";
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(touchfile);
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), file);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
			try
			{
				bool deleted = getFileSystem().delete(root, false);
				NUnit.Framework.Assert.Fail("non recursive delete should have raised an exception,"
					 + " but completed with exit code " + deleted);
			}
			catch (System.IO.IOException e)
			{
				//expected
				handleExpectedException(e);
			}
			finally
			{
				getFileSystem().delete(file, false);
			}
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRmRootRecursive()
		{
			//extra sanity checks here to avoid support calls about complete loss of data
			skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path("/testRmRootRecursive"
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), file);
			bool deleted = getFileSystem().delete(root, true);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, root);
			LOG.info("rm -rf / result is {}", deleted);
			if (deleted)
			{
				assertPathDoesNotExist("expected file to be deleted", file);
			}
			else
			{
				assertPathExists("expected file to be preserved", file);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFileOverRoot()
		{
			org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path("/");
			byte[] dataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(1024, ' '
				, 'z');
			try
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), root, 
					false, dataset);
				NUnit.Framework.Assert.Fail("expected an exception, got a file created over root: "
					 + ls(root));
			}
			catch (System.IO.IOException e)
			{
				//expected
				handleExpectedException(e);
			}
			assertIsDirectory(root);
		}
	}
}
