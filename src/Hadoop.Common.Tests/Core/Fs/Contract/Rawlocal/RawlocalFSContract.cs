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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Contract.Localfs;


namespace Org.Apache.Hadoop.FS.Contract.Rawlocal
{
	/// <summary>Raw local filesystem.</summary>
	/// <remarks>
	/// Raw local filesystem. This is the inner OS-layer FS
	/// before checksumming is added around it.
	/// </remarks>
	public class RawlocalFSContract : LocalFSContract
	{
		public RawlocalFSContract(Configuration conf)
			: base(conf)
		{
		}

		public const string RawContractXml = "contract/localfs.xml";

		protected internal override string GetContractXml()
		{
			return RawContractXml;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FileSystem GetLocalFS()
		{
			return FileSystem.GetLocal(GetConf()).GetRawFileSystem();
		}

		public virtual FilePath GetTestDirectory()
		{
			return new FilePath(GetTestDataDir());
		}
	}
}
