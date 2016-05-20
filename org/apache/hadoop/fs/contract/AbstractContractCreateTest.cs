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
	/// <summary>Test creating files, overwrite options &c</summary>
	public abstract class AbstractContractCreateTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateNewFile()
		{
			describe("Foundational 'create a file' test");
			org.apache.hadoop.fs.Path path = path("testCreateNewFile");
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
				, data, data.Length, 1024 * 1024, false);
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
				), path, data);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreateFileOverExistingFileNoOverwrite()
		{
			describe("Verify overwriting an existing file fails");
			org.apache.hadoop.fs.Path path = path("testCreateFileOverExistingFileNoOverwrite"
				);
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
				, data, data.Length, 1024, false);
			byte[] data2 = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(10 * 1024, 
				'A', 'Z');
			try
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
					, data2, data2.Length, 1024, false);
				NUnit.Framework.Assert.Fail("writing without overwrite unexpectedly succeeded");
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException expected)
			{
				//expected
				handleExpectedException(expected);
			}
			catch (System.IO.IOException relaxed)
			{
				handleRelaxedException("Creating a file over a file with overwrite==false", "FileAlreadyExistsException"
					, relaxed);
			}
		}

		/// <summary>
		/// This test catches some eventual consistency problems that blobstores exhibit,
		/// as we are implicitly verifying that updates are consistent.
		/// </summary>
		/// <remarks>
		/// This test catches some eventual consistency problems that blobstores exhibit,
		/// as we are implicitly verifying that updates are consistent. This
		/// is why different file lengths and datasets are used
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testOverwriteExistingFile()
		{
			describe("Overwrite an existing file and verify the new data is there");
			org.apache.hadoop.fs.Path path = path("testOverwriteExistingFile");
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
				, data, data.Length, 1024, false);
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
				), path, data);
			byte[] data2 = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(10 * 1024, 
				'A', 'Z');
			org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
				, data2, data2.Length, 1024, true);
			org.apache.hadoop.fs.contract.ContractTestUtils.verifyFileContents(getFileSystem(
				), path, data2);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testOverwriteEmptyDirectory()
		{
			describe("verify trying to create a file over an empty dir fails");
			org.apache.hadoop.fs.Path path = path("testOverwriteEmptyDirectory");
			mkdirs(path);
			assertIsDirectory(path);
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			try
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
					, data, data.Length, 1024, true);
				assertIsDirectory(path);
				NUnit.Framework.Assert.Fail("write of file over empty dir succeeded");
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException expected)
			{
				//expected
				handleExpectedException(expected);
			}
			catch (java.io.FileNotFoundException e)
			{
				handleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			catch (System.IO.IOException e)
			{
				handleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			assertIsDirectory(path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testOverwriteNonEmptyDirectory()
		{
			describe("verify trying to create a file over a non-empty dir fails");
			org.apache.hadoop.fs.Path path = path("testOverwriteNonEmptyDirectory");
			mkdirs(path);
			try
			{
				assertIsDirectory(path);
			}
			catch (java.lang.AssertionError failure)
			{
				if (isSupported(IS_BLOBSTORE))
				{
					// file/directory hack surfaces here
					throw new NUnit.Framework.@internal.AssumptionViolatedException(failure.ToString(
						)).initCause(failure);
				}
				// else: rethrow
				throw;
			}
			org.apache.hadoop.fs.Path child = new org.apache.hadoop.fs.Path(path, "child");
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(getFileSystem(), child
				, "child file", true);
			byte[] data = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a', 'z'
				);
			try
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset(getFileSystem(), path
					, data, data.Length, 1024, true);
				org.apache.hadoop.fs.FileStatus status = getFileSystem().getFileStatus(path);
				bool isDir = status.isDirectory();
				if (!isDir && isSupported(IS_BLOBSTORE))
				{
					// object store: downgrade to a skip so that the failure is visible
					// in test results
					org.apache.hadoop.fs.contract.ContractTestUtils.skip("Object store allows a file to overwrite a directory"
						);
				}
				NUnit.Framework.Assert.Fail("write of file over dir succeeded");
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException expected)
			{
				//expected
				handleExpectedException(expected);
			}
			catch (java.io.FileNotFoundException e)
			{
				handleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			catch (System.IO.IOException e)
			{
				handleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			assertIsDirectory(path);
			assertIsFile(child);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreatedFileIsImmediatelyVisible()
		{
			describe("verify that a newly created file exists as soon as open returns");
			org.apache.hadoop.fs.Path path = path("testCreatedFileIsImmediatelyVisible");
			org.apache.hadoop.fs.FSDataOutputStream @out = null;
			try
			{
				@out = getFileSystem().create(path, false, 4096, (short)1, 1024);
				if (!getFileSystem().exists(path))
				{
					if (isSupported(IS_BLOBSTORE))
					{
						// object store: downgrade to a skip so that the failure is visible
						// in test results
						org.apache.hadoop.fs.contract.ContractTestUtils.skip("Filesystem is an object store and newly created files are not immediately visible"
							);
					}
					assertPathExists("expected path to be visible before anything written", path);
				}
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@out);
			}
		}
	}
}
