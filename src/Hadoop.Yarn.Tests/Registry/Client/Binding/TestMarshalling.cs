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
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>Test record marshalling</summary>
	public class TestMarshalling : RegistryTestHelper
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestMarshalling
			));

		[Rule]
		public readonly Timeout testTimeout = new Timeout(10000);

		[Rule]
		public TestName methodName = new TestName();

		private static RegistryUtils.ServiceRecordMarshal marshal;

		[BeforeClass]
		public static void SetupClass()
		{
			marshal = new RegistryUtils.ServiceRecordMarshal();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRoundTrip()
		{
			string persistence = PersistencePolicies.Permanent;
			ServiceRecord record = CreateRecord(persistence);
			record.Set("customkey", "customvalue");
			record.Set("customkey2", "customvalue2");
			RegistryTypeUtils.ValidateServiceRecord(string.Empty, record);
			Log.Info(marshal.ToJson(record));
			byte[] bytes = marshal.ToBytes(record);
			ServiceRecord r2 = marshal.FromBytes(string.Empty, bytes);
			AssertMatches(record, r2);
			RegistryTypeUtils.ValidateServiceRecord(string.Empty, r2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallNoData()
		{
			marshal.FromBytes("src", new byte[] {  });
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallNotEnoughData()
		{
			// this is nominally JSON -but without the service record header
			marshal.FromBytes("src", new byte[] { (byte)('{'), (byte)('}') }, ServiceRecord.RecordType
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallNoBody()
		{
			byte[] bytes = Sharpen.Runtime.GetBytesForString("this is not valid JSON at all and should fail"
				);
			marshal.FromBytes("src", bytes);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallWrongType()
		{
			byte[] bytes = Sharpen.Runtime.GetBytesForString("{'type':''}");
			ServiceRecord serviceRecord = marshal.FromBytes("marshalling", bytes);
			RegistryTypeUtils.ValidateServiceRecord("validating", serviceRecord);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallWrongLongType()
		{
			ServiceRecord record = new ServiceRecord();
			record.type = "ThisRecordHasALongButNonMatchingType";
			byte[] bytes = marshal.ToBytes(record);
			ServiceRecord serviceRecord = marshal.FromBytes("marshalling", bytes, ServiceRecord
				.RecordType);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnmarshallNoType()
		{
			ServiceRecord record = new ServiceRecord();
			record.type = "NoRecord";
			byte[] bytes = marshal.ToBytes(record);
			ServiceRecord serviceRecord = marshal.FromBytes("marshalling", bytes, ServiceRecord
				.RecordType);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRecordValidationWrongType()
		{
			ServiceRecord record = new ServiceRecord();
			record.type = "NotAServiceRecordType";
			RegistryTypeUtils.ValidateServiceRecord("validating", record);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnknownFieldsRoundTrip()
		{
			ServiceRecord record = CreateRecord(PersistencePolicies.ApplicationAttempt);
			record.Set("key", "value");
			record.Set("intval", "2");
			NUnit.Framework.Assert.AreEqual("value", record.Get("key"));
			NUnit.Framework.Assert.AreEqual("2", record.Get("intval"));
			NUnit.Framework.Assert.IsNull(record.Get("null"));
			NUnit.Framework.Assert.AreEqual("defval", record.Get("null", "defval"));
			byte[] bytes = marshal.ToBytes(record);
			ServiceRecord r2 = marshal.FromBytes(string.Empty, bytes);
			NUnit.Framework.Assert.AreEqual("value", r2.Get("key"));
			NUnit.Framework.Assert.AreEqual("2", r2.Get("intval"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFieldPropagationInCopy()
		{
			ServiceRecord record = CreateRecord(PersistencePolicies.ApplicationAttempt);
			record.Set("key", "value");
			record.Set("intval", "2");
			ServiceRecord that = new ServiceRecord(record);
			AssertMatches(record, that);
		}
	}
}
