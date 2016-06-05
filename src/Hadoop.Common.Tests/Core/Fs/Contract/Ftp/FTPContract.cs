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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Contract;


namespace Org.Apache.Hadoop.FS.Contract.Ftp
{
	/// <summary>The contract of FTP; requires the option "test.testdir" to be set</summary>
	public class FTPContract : AbstractBondedFSContract
	{
		public const string ContractXml = "contract/ftp.xml";

		public const string TestFsTestdir = "test.ftp.testdir";

		private string fsName;

		private URI fsURI;

		private FileSystem fs;

		public FTPContract(Configuration conf)
			: base(conf)
		{
			//insert the base features
			AddConfResource(ContractXml);
		}

		public override string GetScheme()
		{
			return "ftp";
		}

		public override Path GetTestPath()
		{
			string pathString = GetOption(TestFsTestdir, null);
			NUnit.Framework.Assert.IsNotNull("Undefined test option " + TestFsTestdir, pathString
				);
			Path path = new Path(pathString);
			return path;
		}
	}
}
