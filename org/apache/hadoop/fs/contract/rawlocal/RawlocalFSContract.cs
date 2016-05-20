/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.fs.contract.rawlocal
{
	/// <summary>Raw local filesystem.</summary>
	/// <remarks>
	/// Raw local filesystem. This is the inner OS-layer FS
	/// before checksumming is added around it.
	/// </remarks>
	public class RawlocalFSContract : org.apache.hadoop.fs.contract.localfs.LocalFSContract
	{
		public RawlocalFSContract(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
		}

		public const string RAW_CONTRACT_XML = "contract/localfs.xml";

		protected internal override string getContractXml()
		{
			return RAW_CONTRACT_XML;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.FileSystem getLocalFS()
		{
			return org.apache.hadoop.fs.FileSystem.getLocal(getConf()).getRawFileSystem();
		}

		public virtual java.io.File getTestDirectory()
		{
			return new java.io.File(getTestDataDir());
		}
	}
}
