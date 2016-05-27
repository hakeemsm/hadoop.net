/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Jmx
{
	public class TestJMXJsonServlet : HttpServerFunctionalTest
	{
		private static HttpServer2 server;

		private static Uri baseUrl;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			server = CreateTestServer();
			server.Start();
			baseUrl = GetServerURL(server);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void Cleanup()
		{
			server.Stop();
		}

		public static void AssertReFind(string re, string value)
		{
			Sharpen.Pattern p = Sharpen.Pattern.Compile(re);
			Matcher m = p.Matcher(value);
			NUnit.Framework.Assert.IsTrue("'" + p + "' does not match " + value, m.Find());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQury()
		{
			string result = ReadOutput(new Uri(baseUrl, "/jmx?qry=java.lang:type=Runtime"));
			AssertReFind("\"name\"\\s*:\\s*\"java.lang:type=Runtime\"", result);
			AssertReFind("\"modelerType\"", result);
			result = ReadOutput(new Uri(baseUrl, "/jmx?qry=java.lang:type=Memory"));
			AssertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
			AssertReFind("\"modelerType\"", result);
			result = ReadOutput(new Uri(baseUrl, "/jmx"));
			AssertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
			// test to get an attribute of a mbean
			result = ReadOutput(new Uri(baseUrl, "/jmx?get=java.lang:type=Memory::HeapMemoryUsage"
				));
			AssertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
			AssertReFind("\"committed\"\\s*:", result);
			// negative test to get an attribute of a mbean
			result = ReadOutput(new Uri(baseUrl, "/jmx?get=java.lang:type=Memory::"));
			AssertReFind("\"ERROR\"", result);
			// test to CORS headers
			HttpURLConnection conn = (HttpURLConnection)new Uri(baseUrl, "/jmx?qry=java.lang:type=Memory"
				).OpenConnection();
			NUnit.Framework.Assert.AreEqual("GET", conn.GetHeaderField(JMXJsonServlet.AccessControlAllowMethods
				));
			NUnit.Framework.Assert.IsNotNull(conn.GetHeaderField(JMXJsonServlet.AccessControlAllowOrigin
				));
		}
	}
}
