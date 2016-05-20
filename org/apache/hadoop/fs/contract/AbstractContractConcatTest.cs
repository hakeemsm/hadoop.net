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
	/// <summary>Test concat -if supported</summary>
	public abstract class AbstractContractConcatTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractContractConcatTest
			)));

		private org.apache.hadoop.fs.Path testPath;

		private org.apache.hadoop.fs.Path srcFile;

		private org.apache.hadoop.fs.Path zeroByteFile;

		private org.apache.hadoop.fs.Path target;

		/// <exception cref="System.Exception"/>
		public override void setup()
		{
			base.setup();
			skipIfUnsupported(SUPPORTS_CONCAT);
			//delete the test directory
			testPath = path("test");
			srcFile = new org.apache.hadoop.fs.Path(testPath, "small.txt");
			zeroByteFile = new org.apache.hadoop.fs.Path(testPath, "zero.txt");
			target = new org.apache.hadoop.fs.Path(testPath, "target");
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(TEST_FILE_LEN
				, 0, 255);
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), srcFile
				, false, block);
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), zeroByteFile
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConcatEmptyFiles()
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), target);
			try
			{
				getFileSystem().concat(target, new org.apache.hadoop.fs.Path[0]);
				NUnit.Framework.Assert.Fail("expected a failure");
			}
			catch (System.Exception e)
			{
				//expected
				handleExpectedException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConcatMissingTarget()
		{
			try
			{
				getFileSystem().concat(target, new org.apache.hadoop.fs.Path[] { zeroByteFile });
				NUnit.Framework.Assert.Fail("expected a failure");
			}
			catch (System.Exception e)
			{
				//expected
				handleExpectedException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConcatFileOnFile()
		{
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(TEST_FILE_LEN
				, 0, 255);
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), target
				, false, block);
			getFileSystem().concat(target, new org.apache.hadoop.fs.Path[] { srcFile });
			org.apache.hadoop.fs.contract.ContractTestUtils.assertFileHasLength(getFileSystem
				(), target, TEST_FILE_LEN * 2);
			org.apache.hadoop.fs.contract.ContractTestUtils.validateFileContent(org.apache.hadoop.fs.contract.ContractTestUtils
				.readDataset(getFileSystem(), target, TEST_FILE_LEN * 2), new byte[][] { block, 
				block });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConcatOnSelf()
		{
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(TEST_FILE_LEN
				, 0, 255);
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), target
				, false, block);
			try
			{
				getFileSystem().concat(target, new org.apache.hadoop.fs.Path[] { target });
			}
			catch (System.Exception e)
			{
				//expected
				handleExpectedException(e);
			}
		}
	}
}
