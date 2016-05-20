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
	public abstract class AbstractContractDeleteTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteEmptyDirNonRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteEmptyDirNonRecursive");
			mkdirs(path);
			assertDeleted(path, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteEmptyDirRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteEmptyDirRecursive");
			mkdirs(path);
			assertDeleted(path, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonexistentPathRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteNonexistentPathRecursive");
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(getFileSystem
				(), "leftover", path);
			org.apache.hadoop.fs.contract.ContractTestUtils.rejectRootOperation(path);
			NUnit.Framework.Assert.IsFalse("Returned true attempting to delete" + " a nonexistent path "
				 + path, getFileSystem().delete(path, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonexistentPathNonRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteNonexistentPathNonRecursive");
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(getFileSystem
				(), "leftover", path);
			org.apache.hadoop.fs.contract.ContractTestUtils.rejectRootOperation(path);
			NUnit.Framework.Assert.IsFalse("Returned true attempting to recursively delete" +
				 " a nonexistent path " + path, getFileSystem().delete(path, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonEmptyDirNonRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteNonEmptyDirNonRecursive");
			mkdirs(path);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path, "childfile");
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(getFileSystem(), file
				, "goodbye, world", true);
			try
			{
				org.apache.hadoop.fs.contract.ContractTestUtils.rejectRootOperation(path);
				bool deleted = getFileSystem().delete(path, false);
				NUnit.Framework.Assert.Fail("non recursive delete should have raised an exception,"
					 + " but completed with exit code " + deleted);
			}
			catch (System.IO.IOException expected)
			{
				//expected
				handleExpectedException(expected);
			}
			org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory(getFileSystem()
				, path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteNonEmptyDirRecursive()
		{
			org.apache.hadoop.fs.Path path = path("testDeleteNonEmptyDirNonRecursive");
			mkdirs(path);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path, "childfile");
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(getFileSystem(), file
				, "goodbye, world", true);
			assertDeleted(path, true);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(getFileSystem
				(), "not deleted", file);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteDeepEmptyDir()
		{
			mkdirs(path("testDeleteDeepEmptyDir/d1/d2/d3/d4"));
			assertDeleted(path("testDeleteDeepEmptyDir/d1/d2/d3"), true);
			org.apache.hadoop.fs.FileSystem fs = getFileSystem();
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(fs, "not deleted"
				, path("testDeleteDeepEmptyDir/d1/d2/d3/d4"));
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist(fs, "not deleted"
				, path("testDeleteDeepEmptyDir/d1/d2/d3"));
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(fs, "parent dir is deleted"
				, path("testDeleteDeepEmptyDir/d1/d2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteSingleFile()
		{
			// Test delete of just a file
			org.apache.hadoop.fs.Path path = path("testDeleteSingleFile/d1/d2");
			mkdirs(path);
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(path, "childfile");
			org.apache.hadoop.fs.contract.ContractTestUtils.writeTextFile(getFileSystem(), file
				, "single file to be deleted.", true);
			org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists(getFileSystem(), 
				"single file not created", file);
			assertDeleted(file, false);
		}
	}
}
