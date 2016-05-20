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

namespace org.apache.hadoop.fs.contract.localfs
{
	/// <summary>just here to make sure that the local.xml resource is actually loading</summary>
	public class TestLocalFSContractLoaded : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		protected internal override org.apache.hadoop.fs.contract.AbstractFSContract createContract
			(org.apache.hadoop.conf.Configuration conf)
		{
			return new org.apache.hadoop.fs.contract.localfs.LocalFSContract(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testContractWorks()
		{
			string key = getContract().getConfKey(SUPPORTS_ATOMIC_RENAME);
			NUnit.Framework.Assert.IsNotNull("not set: " + key, getContract().getConf().get(key
				));
			NUnit.Framework.Assert.IsTrue("not true: " + key, getContract().isSupported(SUPPORTS_ATOMIC_RENAME
				, false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testContractResourceOnClasspath()
		{
			java.net.URL url = Sharpen.Runtime.getClassForObject(this).getClassLoader().getResource
				(org.apache.hadoop.fs.contract.localfs.LocalFSContract.CONTRACT_XML);
			NUnit.Framework.Assert.IsNotNull("could not find contract resource", url);
		}
	}
}
