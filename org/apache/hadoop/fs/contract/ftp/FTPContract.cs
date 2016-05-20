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

namespace org.apache.hadoop.fs.contract.ftp
{
	/// <summary>The contract of FTP; requires the option "test.testdir" to be set</summary>
	public class FTPContract : org.apache.hadoop.fs.contract.AbstractBondedFSContract
	{
		public const string CONTRACT_XML = "contract/ftp.xml";

		public const string TEST_FS_TESTDIR = "test.ftp.testdir";

		private string fsName;

		private java.net.URI fsURI;

		private org.apache.hadoop.fs.FileSystem fs;

		public FTPContract(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			//insert the base features
			addConfResource(CONTRACT_XML);
		}

		public override string getScheme()
		{
			return "ftp";
		}

		public override org.apache.hadoop.fs.Path getTestPath()
		{
			string pathString = getOption(TEST_FS_TESTDIR, null);
			NUnit.Framework.Assert.IsNotNull("Undefined test option " + TEST_FS_TESTDIR, pathString
				);
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(pathString);
			return path;
		}
	}
}
