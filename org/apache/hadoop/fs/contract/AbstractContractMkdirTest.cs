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
	/// <summary>Test directory operations</summary>
	public abstract class AbstractContractMkdirTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkDirRmDir()
		{
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.Path dir = path("testMkDirRmDir");
			assertPathDoesNotExist("directory already exists", dir);
			fs.mkdirs(dir);
			assertPathExists("mkdir failed", dir);
			assertDeleted(dir, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkDirRmRfDir()
		{
			describe("create a directory then recursive delete it");
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.Path dir = path("testMkDirRmRfDir");
			assertPathDoesNotExist("directory already exists", dir);
			fs.mkdirs(dir);
			assertPathExists("mkdir failed", dir);
			assertDeleted(dir, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNoMkdirOverFile()
		{
			describe("try to mkdir over a file");
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.Path path = path("testNoMkdirOverFile");
			byte[] dataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(1024, ' '
				, 'z');
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), path, 
				false, dataset);
			try
			{
				bool made = fs.mkdirs(path);
				NUnit.Framework.Assert.Fail("mkdirs did not fail over a file but returned " + made
					 + "; " + ls(path));
			}
			catch (org.apache.hadoop.fs.ParentNotDirectoryException e)
			{
				//parent is a directory
				handleExpectedException(e);
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException e)
			{
				//also allowed as an exception (HDFS)
				handleExpectedException(e);
			}
			catch (System.IO.IOException e)
			{
				//here the FS says "no create"
				handleRelaxedException("mkdirs", "FileAlreadyExistsException", e);
			}
			assertIsFile(path);
			byte[] bytes = org.apache.hadoop.fs.contract.ContractTestUtils.readDataset(getFileSystem
				(), path, dataset.Length);
			org.apache.hadoop.fs.contract.ContractTestUtils.compareByteArrays(dataset, bytes, 
				dataset.Length);
			assertPathExists("mkdir failed", path);
			assertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirOverParentFile()
		{
			describe("try to mkdir where a parent is a file");
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.Path path = path("testMkdirOverParentFile");
			byte[] dataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(1024, ' '
				, 'z');
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), path, 
				false, dataset);
			org.apache.hadoop.fs.Path child = new org.apache.hadoop.fs.Path(path, "child-to-mkdir"
				);
			try
			{
				bool made = fs.mkdirs(child);
				NUnit.Framework.Assert.Fail("mkdirs did not fail over a file but returned " + made
					 + "; " + ls(path));
			}
			catch (org.apache.hadoop.fs.ParentNotDirectoryException e)
			{
				//parent is a directory
				handleExpectedException(e);
			}
			catch (org.apache.hadoop.fs.FileAlreadyExistsException e)
			{
				handleExpectedException(e);
			}
			catch (System.IO.IOException e)
			{
				handleRelaxedException("mkdirs", "ParentNotDirectoryException", e);
			}
			assertIsFile(path);
			byte[] bytes = org.apache.hadoop.fs.contract.ContractTestUtils.readDataset(getFileSystem
				(), path, dataset.Length);
			org.apache.hadoop.fs.contract.ContractTestUtils.compareByteArrays(dataset, bytes, 
				dataset.Length);
			assertPathExists("mkdir failed", path);
			assertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMkdirSlashHandling()
		{
			describe("verify mkdir slash handling");
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			// No trailing slash
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(path("testmkdir/a")));
			assertPathExists("mkdir without trailing slash failed", path("testmkdir/a"));
			// With trailing slash
			NUnit.Framework.Assert.IsTrue(fs.mkdirs(path("testmkdir/b/")));
			assertPathExists("mkdir with trailing slash failed", path("testmkdir/b/"));
			// Mismatched slashes
			assertPathExists("check path existence without trailing slash failed", path("testmkdir/b"
				));
		}
	}
}
