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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Contract;
using Org.Apache.Hadoop.FS.Ftp;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract.Ftp
{
	public class TestFTPContractRename : AbstractContractRenameTest
	{
		protected internal override AbstractFSContract CreateContract(Configuration conf)
		{
			return new FTPContract(conf);
		}

		/// <summary>
		/// Check the exception was about cross-directory renames
		/// -if not, rethrow it.
		/// </summary>
		/// <param name="e">exception raised</param>
		/// <exception cref="System.IO.IOException"/>
		private void VerifyUnsupportedDirRenameException(IOException e)
		{
			if (!e.ToString().Contains(FTPFileSystem.ESameDirectoryOnly))
			{
				throw e;
			}
		}

		/// <exception cref="System.Exception"/>
		public override void TestRenameDirIntoExistingDir()
		{
			try
			{
				base.TestRenameDirIntoExistingDir();
				NUnit.Framework.Assert.Fail("Expected a failure");
			}
			catch (IOException e)
			{
				VerifyUnsupportedDirRenameException(e);
			}
		}

		/// <exception cref="System.Exception"/>
		public override void TestRenameFileNonexistentDir()
		{
			try
			{
				base.TestRenameFileNonexistentDir();
				NUnit.Framework.Assert.Fail("Expected a failure");
			}
			catch (IOException e)
			{
				VerifyUnsupportedDirRenameException(e);
			}
		}
	}
}
