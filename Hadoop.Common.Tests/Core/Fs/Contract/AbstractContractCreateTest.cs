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
using System.IO;
using NUnit.Framework.Internal;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test creating files, overwrite options &c</summary>
	public abstract class AbstractContractCreateTest : AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateNewFile()
		{
			Describe("Foundational 'create a file' test");
			Path path = Path("testCreateNewFile");
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), path, data, data.Length, 1024 * 1024
				, false);
			ContractTestUtils.VerifyFileContents(GetFileSystem(), path, data);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreateFileOverExistingFileNoOverwrite()
		{
			Describe("Verify overwriting an existing file fails");
			Path path = Path("testCreateFileOverExistingFileNoOverwrite");
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), path, data, data.Length, 1024, false
				);
			byte[] data2 = ContractTestUtils.Dataset(10 * 1024, 'A', 'Z');
			try
			{
				ContractTestUtils.WriteDataset(GetFileSystem(), path, data2, data2.Length, 1024, 
					false);
				NUnit.Framework.Assert.Fail("writing without overwrite unexpectedly succeeded");
			}
			catch (FileAlreadyExistsException expected)
			{
				//expected
				HandleExpectedException(expected);
			}
			catch (IOException relaxed)
			{
				HandleRelaxedException("Creating a file over a file with overwrite==false", "FileAlreadyExistsException"
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
		[Fact]
		public virtual void TestOverwriteExistingFile()
		{
			Describe("Overwrite an existing file and verify the new data is there");
			Path path = Path("testOverwriteExistingFile");
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			ContractTestUtils.WriteDataset(GetFileSystem(), path, data, data.Length, 1024, false
				);
			ContractTestUtils.VerifyFileContents(GetFileSystem(), path, data);
			byte[] data2 = ContractTestUtils.Dataset(10 * 1024, 'A', 'Z');
			ContractTestUtils.WriteDataset(GetFileSystem(), path, data2, data2.Length, 1024, 
				true);
			ContractTestUtils.VerifyFileContents(GetFileSystem(), path, data2);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOverwriteEmptyDirectory()
		{
			Describe("verify trying to create a file over an empty dir fails");
			Path path = Path("testOverwriteEmptyDirectory");
			Mkdirs(path);
			AssertIsDirectory(path);
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			try
			{
				ContractTestUtils.WriteDataset(GetFileSystem(), path, data, data.Length, 1024, true
					);
				AssertIsDirectory(path);
				NUnit.Framework.Assert.Fail("write of file over empty dir succeeded");
			}
			catch (FileAlreadyExistsException expected)
			{
				//expected
				HandleExpectedException(expected);
			}
			catch (FileNotFoundException e)
			{
				HandleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			catch (IOException e)
			{
				HandleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			AssertIsDirectory(path);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOverwriteNonEmptyDirectory()
		{
			Describe("verify trying to create a file over a non-empty dir fails");
			Path path = Path("testOverwriteNonEmptyDirectory");
			Mkdirs(path);
			try
			{
				AssertIsDirectory(path);
			}
			catch (Exception failure)
			{
				if (IsSupported(IsBlobstore))
				{
					// file/directory hack surfaces here
					throw Sharpen.Extensions.InitCause(new AssumptionViolatedException(failure.ToString
						()), failure);
				}
				// else: rethrow
				throw;
			}
			Path child = new Path(path, "child");
			ContractTestUtils.WriteTextFile(GetFileSystem(), child, "child file", true);
			byte[] data = ContractTestUtils.Dataset(256, 'a', 'z');
			try
			{
				ContractTestUtils.WriteDataset(GetFileSystem(), path, data, data.Length, 1024, true
					);
				FileStatus status = GetFileSystem().GetFileStatus(path);
				bool isDir = status.IsDirectory();
				if (!isDir && IsSupported(IsBlobstore))
				{
					// object store: downgrade to a skip so that the failure is visible
					// in test results
					ContractTestUtils.Skip("Object store allows a file to overwrite a directory");
				}
				NUnit.Framework.Assert.Fail("write of file over dir succeeded");
			}
			catch (FileAlreadyExistsException expected)
			{
				//expected
				HandleExpectedException(expected);
			}
			catch (FileNotFoundException e)
			{
				HandleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			catch (IOException e)
			{
				HandleRelaxedException("overwriting a dir with a file ", "FileAlreadyExistsException"
					, e);
			}
			AssertIsDirectory(path);
			AssertIsFile(child);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreatedFileIsImmediatelyVisible()
		{
			Describe("verify that a newly created file exists as soon as open returns");
			Path path = Path("testCreatedFileIsImmediatelyVisible");
			FSDataOutputStream @out = null;
			try
			{
				@out = GetFileSystem().Create(path, false, 4096, (short)1, 1024);
				if (!GetFileSystem().Exists(path))
				{
					if (IsSupported(IsBlobstore))
					{
						// object store: downgrade to a skip so that the failure is visible
						// in test results
						ContractTestUtils.Skip("Filesystem is an object store and newly created files are not immediately visible"
							);
					}
					AssertPathExists("expected path to be visible before anything written", path);
				}
			}
			finally
			{
				IOUtils.CloseStream(@out);
			}
		}
	}
}
