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
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>
	/// Tests for the
	/// <see cref="RegistryUtils"/>
	/// class
	/// </summary>
	public class TestRegistryOperationUtils : Assert
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUsernameExtractionEnvVarOverrride()
		{
			string whoami = RegistryUtils.GetCurrentUsernameUnencoded("drwho");
			NUnit.Framework.Assert.AreEqual("drwho", whoami);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUsernameExtractionCurrentuser()
		{
			string whoami = RegistryUtils.GetCurrentUsernameUnencoded(string.Empty);
			string ugiUser = UserGroupInformation.GetCurrentUser().GetShortUserName();
			NUnit.Framework.Assert.AreEqual(ugiUser, whoami);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShortenUsername()
		{
			NUnit.Framework.Assert.AreEqual("hbase", RegistryUtils.ConvertUsername("hbase@HADOOP.APACHE.ORG"
				));
			NUnit.Framework.Assert.AreEqual("hbase", RegistryUtils.ConvertUsername("hbase/localhost@HADOOP.APACHE.ORG"
				));
			NUnit.Framework.Assert.AreEqual("hbase", RegistryUtils.ConvertUsername("hbase"));
			NUnit.Framework.Assert.AreEqual("hbase user", RegistryUtils.ConvertUsername("hbase user"
				));
		}
	}
}
