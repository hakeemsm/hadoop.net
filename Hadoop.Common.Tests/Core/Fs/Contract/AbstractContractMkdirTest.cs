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
using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test directory operations</summary>
	public abstract class AbstractContractMkdirTest : AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkDirRmDir()
		{
			FileSystem fs = GetFileSystem();
			Path dir = Path("testMkDirRmDir");
			AssertPathDoesNotExist("directory already exists", dir);
			fs.Mkdirs(dir);
			AssertPathExists("mkdir failed", dir);
			AssertDeleted(dir, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkDirRmRfDir()
		{
			Describe("create a directory then recursive delete it");
			FileSystem fs = GetFileSystem();
			Path dir = Path("testMkDirRmRfDir");
			AssertPathDoesNotExist("directory already exists", dir);
			fs.Mkdirs(dir);
			AssertPathExists("mkdir failed", dir);
			AssertDeleted(dir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoMkdirOverFile()
		{
			Describe("try to mkdir over a file");
			FileSystem fs = GetFileSystem();
			Path path = Path("testNoMkdirOverFile");
			byte[] dataset = ContractTestUtils.Dataset(1024, ' ', 'z');
			ContractTestUtils.CreateFile(GetFileSystem(), path, false, dataset);
			try
			{
				bool made = fs.Mkdirs(path);
				NUnit.Framework.Assert.Fail("mkdirs did not fail over a file but returned " + made
					 + "; " + Ls(path));
			}
			catch (ParentNotDirectoryException e)
			{
				//parent is a directory
				HandleExpectedException(e);
			}
			catch (FileAlreadyExistsException e)
			{
				//also allowed as an exception (HDFS)
				HandleExpectedException(e);
			}
			catch (IOException e)
			{
				//here the FS says "no create"
				HandleRelaxedException("mkdirs", "FileAlreadyExistsException", e);
			}
			AssertIsFile(path);
			byte[] bytes = ContractTestUtils.ReadDataset(GetFileSystem(), path, dataset.Length
				);
			ContractTestUtils.CompareByteArrays(dataset, bytes, dataset.Length);
			AssertPathExists("mkdir failed", path);
			AssertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirOverParentFile()
		{
			Describe("try to mkdir where a parent is a file");
			FileSystem fs = GetFileSystem();
			Path path = Path("testMkdirOverParentFile");
			byte[] dataset = ContractTestUtils.Dataset(1024, ' ', 'z');
			ContractTestUtils.CreateFile(GetFileSystem(), path, false, dataset);
			Path child = new Path(path, "child-to-mkdir");
			try
			{
				bool made = fs.Mkdirs(child);
				NUnit.Framework.Assert.Fail("mkdirs did not fail over a file but returned " + made
					 + "; " + Ls(path));
			}
			catch (ParentNotDirectoryException e)
			{
				//parent is a directory
				HandleExpectedException(e);
			}
			catch (FileAlreadyExistsException e)
			{
				HandleExpectedException(e);
			}
			catch (IOException e)
			{
				HandleRelaxedException("mkdirs", "ParentNotDirectoryException", e);
			}
			AssertIsFile(path);
			byte[] bytes = ContractTestUtils.ReadDataset(GetFileSystem(), path, dataset.Length
				);
			ContractTestUtils.CompareByteArrays(dataset, bytes, dataset.Length);
			AssertPathExists("mkdir failed", path);
			AssertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMkdirSlashHandling()
		{
			Describe("verify mkdir slash handling");
			FileSystem fs = GetFileSystem();
			// No trailing slash
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(Path("testmkdir/a")));
			AssertPathExists("mkdir without trailing slash failed", Path("testmkdir/a"));
			// With trailing slash
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(Path("testmkdir/b/")));
			AssertPathExists("mkdir with trailing slash failed", Path("testmkdir/b/"));
			// Mismatched slashes
			AssertPathExists("check path existence without trailing slash failed", Path("testmkdir/b"
				));
		}
	}
}
