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
	public abstract class AbstractContractAppendTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractContractAppendTest
			)));

		private org.apache.hadoop.fs.Path testPath;

		private org.apache.hadoop.fs.Path target;

		/// <exception cref="System.Exception"/>
		public override void setup()
		{
			base.setup();
			skipIfUnsupported(SUPPORTS_APPEND);
			//delete the test directory
			testPath = path("test");
			target = new org.apache.hadoop.fs.Path(testPath, "target");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAppendToEmptyFile()
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), target);
			byte[] dataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a'
				, 'z');
			org.apache.hadoop.fs.FSDataOutputStream outputStream = getFileSystem().append(target
				);
			try
			{
				outputStream.write(dataset);
			}
			finally
			{
				outputStream.close();
			}
			byte[] bytes = org.apache.hadoop.fs.contract.ContractTestUtils.readDataset(getFileSystem
				(), target, dataset.Length);
			org.apache.hadoop.fs.contract.ContractTestUtils.compareByteArrays(dataset, bytes, 
				dataset.Length);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAppendNonexistentFile()
		{
			try
			{
				org.apache.hadoop.fs.FSDataOutputStream @out = getFileSystem().append(target);
				//got here: trouble
				@out.close();
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
		public virtual void testAppendToExistingFile()
		{
			byte[] original = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(8192, 'A'
				, 'Z');
			byte[] appended = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(8192, '0'
				, '9');
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), target
				, false, original);
			org.apache.hadoop.fs.FSDataOutputStream outputStream = getFileSystem().append(target
				);
			outputStream.write(appended);
			outputStream.close();
			byte[] bytes = org.apache.hadoop.fs.contract.ContractTestUtils.readDataset(getFileSystem
				(), target, original.Length + appended.Length);
			org.apache.hadoop.fs.contract.ContractTestUtils.validateFileContent(bytes, new byte
				[][] { original, appended });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAppendMissingTarget()
		{
			try
			{
				org.apache.hadoop.fs.FSDataOutputStream @out = getFileSystem().append(target);
				//got here: trouble
				@out.close();
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
		public virtual void testRenameFileBeingAppended()
		{
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), target);
			assertPathExists("original file does not exist", target);
			byte[] dataset = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(256, 'a'
				, 'z');
			org.apache.hadoop.fs.FSDataOutputStream outputStream = getFileSystem().append(target
				);
			outputStream.write(dataset);
			org.apache.hadoop.fs.Path renamed = new org.apache.hadoop.fs.Path(testPath, "renamed"
				);
			outputStream.close();
			string listing = ls(testPath);
			//expected: the stream goes to the file that was being renamed, not
			//the original path
			assertPathExists("renamed destination file does not exist", renamed);
			assertPathDoesNotExist("Source file found after rename during append:\n" + listing
				, target);
			byte[] bytes = org.apache.hadoop.fs.contract.ContractTestUtils.readDataset(getFileSystem
				(), renamed, dataset.Length);
			org.apache.hadoop.fs.contract.ContractTestUtils.compareByteArrays(dataset, bytes, 
				dataset.Length);
		}
	}
}
