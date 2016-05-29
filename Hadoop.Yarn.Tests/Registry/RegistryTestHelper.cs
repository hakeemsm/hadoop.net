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
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Javax.Security.Auth;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Apache.Hadoop.Registry.Secure;
using Org.Apache.Hadoop.Security;
using Org.Apache.Zookeeper.Common;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry
{
	/// <summary>This is a set of static methods to aid testing the registry operations.</summary>
	/// <remarks>
	/// This is a set of static methods to aid testing the registry operations.
	/// The methods can be imported statically —or the class used as a base
	/// class for tests.
	/// </remarks>
	public class RegistryTestHelper : Assert
	{
		public const string ScHadoop = "org-apache-hadoop";

		public const string User = "devteam/";

		public const string Name = "hdfs";

		public const string ApiWebhdfs = "classpath:org.apache.hadoop.namenode.webhdfs";

		public const string ApiHdfs = "classpath:org.apache.hadoop.namenode.dfs";

		public const string Userpath = RegistryConstants.PathUsers + User;

		public const string ParentPath = Userpath + ScHadoop + "/";

		public const string EntryPath = ParentPath + Name;

		public const string Nnipc = "uuid:423C2B93-C927-4050-AEC6-6540E6646437";

		public const string Ipc2 = "uuid:0663501D-5AD3-4F7E-9419-52F5D6636FCF";

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(RegistryTestHelper
			));

		private static readonly RegistryUtils.ServiceRecordMarshal recordMarshal = new RegistryUtils.ServiceRecordMarshal
			();

		public const string HttpApi = "http://";

		/// <summary>Assert the path is valid by ZK rules</summary>
		/// <param name="path">path to check</param>
		public static void AssertValidZKPath(string path)
		{
			try
			{
				PathUtils.ValidatePath(path);
			}
			catch (ArgumentException e)
			{
				throw new ArgumentException("Invalid Path " + path + ": " + e, e);
			}
		}

		/// <summary>Assert that a string is not empty (null or "")</summary>
		/// <param name="message">message to raise if the string is empty</param>
		/// <param name="check">string to check</param>
		public static void AssertNotEmpty(string message, string check)
		{
			if (StringUtils.IsEmpty(check))
			{
				NUnit.Framework.Assert.Fail(message);
			}
		}

		/// <summary>Assert that a string is empty (null or "")</summary>
		/// <param name="check">string to check</param>
		public static void AssertNotEmpty(string check)
		{
			if (StringUtils.IsEmpty(check))
			{
				NUnit.Framework.Assert.Fail("Empty string");
			}
		}

		/// <summary>Log the details of a login context</summary>
		/// <param name="name">name to assert that the user is logged in as</param>
		/// <param name="loginContext">the login context</param>
		public static void LogLoginDetails(string name, LoginContext loginContext)
		{
			NUnit.Framework.Assert.IsNotNull("Null login context", loginContext);
			Subject subject = loginContext.GetSubject();
			Log.Info("Logged in as {}:\n {}", name, subject);
		}

		/// <summary>Set the JVM property to enable Kerberos debugging</summary>
		public static void EnableKerberosDebugging()
		{
			Runtime.SetProperty(AbstractSecureRegistryTest.SunSecurityKrb5Debug, "true");
		}

		/// <summary>Set the JVM property to enable Kerberos debugging</summary>
		public static void DisableKerberosDebugging()
		{
			Runtime.SetProperty(AbstractSecureRegistryTest.SunSecurityKrb5Debug, "false");
		}

		/// <summary>
		/// General code to validate bits of a component/service entry built iwth
		/// <see cref="AddSampleEndpoints(Org.Apache.Hadoop.Registry.Client.Types.ServiceRecord, string)
		/// 	"/>
		/// </summary>
		/// <param name="record">instance to check</param>
		public static void ValidateEntry(ServiceRecord record)
		{
			NUnit.Framework.Assert.IsNotNull("null service record", record);
			IList<Endpoint> endpoints = record.external;
			NUnit.Framework.Assert.AreEqual(2, endpoints.Count);
			Endpoint webhdfs = FindEndpoint(record, ApiWebhdfs, true, 1, 1);
			NUnit.Framework.Assert.AreEqual(ApiWebhdfs, webhdfs.api);
			NUnit.Framework.Assert.AreEqual(AddressTypes.AddressUri, webhdfs.addressType);
			NUnit.Framework.Assert.AreEqual(ProtocolTypes.ProtocolRest, webhdfs.protocolType);
			IList<IDictionary<string, string>> addressList = webhdfs.addresses;
			IDictionary<string, string> url = addressList[0];
			string addr = url["uri"];
			NUnit.Framework.Assert.IsTrue(addr.Contains("http"));
			NUnit.Framework.Assert.IsTrue(addr.Contains(":8020"));
			Endpoint nnipc = FindEndpoint(record, Nnipc, false, 1, 2);
			NUnit.Framework.Assert.AreEqual("wrong protocol in " + nnipc, ProtocolTypes.ProtocolThrift
				, nnipc.protocolType);
			Endpoint ipc2 = FindEndpoint(record, Ipc2, false, 1, 2);
			NUnit.Framework.Assert.IsNotNull(ipc2);
			Endpoint web = FindEndpoint(record, HttpApi, true, 1, 1);
			NUnit.Framework.Assert.AreEqual(1, web.addresses.Count);
			NUnit.Framework.Assert.AreEqual(1, web.addresses[0].Count);
		}

		/// <summary>Assert that an endpoint matches the criteria</summary>
		/// <param name="endpoint">endpoint to examine</param>
		/// <param name="addressType">expected address type</param>
		/// <param name="protocolType">expected protocol type</param>
		/// <param name="api">API</param>
		public static void AssertMatches(Endpoint endpoint, string addressType, string protocolType
			, string api)
		{
			NUnit.Framework.Assert.IsNotNull(endpoint);
			NUnit.Framework.Assert.AreEqual(addressType, endpoint.addressType);
			NUnit.Framework.Assert.AreEqual(protocolType, endpoint.protocolType);
			NUnit.Framework.Assert.AreEqual(api, endpoint.api);
		}

		/// <summary>Assert the records match.</summary>
		/// <param name="source">record that was written</param>
		/// <param name="resolved">the one that resolved.</param>
		public static void AssertMatches(ServiceRecord source, ServiceRecord resolved)
		{
			NUnit.Framework.Assert.IsNotNull("Null source record ", source);
			NUnit.Framework.Assert.IsNotNull("Null resolved record ", resolved);
			NUnit.Framework.Assert.AreEqual(source.description, resolved.description);
			IDictionary<string, string> srcAttrs = source.Attributes();
			IDictionary<string, string> resolvedAttrs = resolved.Attributes();
			string sourceAsString = source.ToString();
			string resolvedAsString = resolved.ToString();
			NUnit.Framework.Assert.AreEqual("Wrong count of attrs in \n" + sourceAsString + "\nfrom\n"
				 + resolvedAsString, srcAttrs.Count, resolvedAttrs.Count);
			foreach (KeyValuePair<string, string> entry in srcAttrs)
			{
				string attr = entry.Key;
				NUnit.Framework.Assert.AreEqual("attribute " + attr, entry.Value, resolved.Get(attr
					));
			}
			NUnit.Framework.Assert.AreEqual("wrong external endpoint count", source.external.
				Count, resolved.external.Count);
			NUnit.Framework.Assert.AreEqual("wrong external endpoint count", source.@internal
				.Count, resolved.@internal.Count);
		}

		/// <summary>Find an endpoint in a record or fail,</summary>
		/// <param name="record">record</param>
		/// <param name="api">API</param>
		/// <param name="external">external?</param>
		/// <param name="addressElements">expected # of address elements?</param>
		/// <param name="addressTupleSize">expected size of a type</param>
		/// <returns>the endpoint.</returns>
		public static Endpoint FindEndpoint(ServiceRecord record, string api, bool external
			, int addressElements, int addressTupleSize)
		{
			Endpoint epr = external ? record.GetExternalEndpoint(api) : record.GetInternalEndpoint
				(api);
			if (epr != null)
			{
				NUnit.Framework.Assert.AreEqual("wrong # of addresses", addressElements, epr.addresses
					.Count);
				NUnit.Framework.Assert.AreEqual("wrong # of elements in an address tuple", addressTupleSize
					, epr.addresses[0].Count);
				return epr;
			}
			IList<Endpoint> endpoints = external ? record.external : record.@internal;
			StringBuilder builder = new StringBuilder();
			foreach (Endpoint endpoint in endpoints)
			{
				builder.Append("\"").Append(endpoint).Append("\" ");
			}
			NUnit.Framework.Assert.Fail("Did not find " + api + " in endpoints " + builder);
			// never reached; here to keep the compiler happy
			return null;
		}

		/// <summary>Log a record</summary>
		/// <param name="name">record name</param>
		/// <param name="record">details</param>
		/// <exception cref="System.IO.IOException">
		/// only if something bizarre goes wrong marshalling
		/// a record.
		/// </exception>
		public static void LogRecord(string name, ServiceRecord record)
		{
			Log.Info(" {} = \n{}\n", name, recordMarshal.ToJson(record));
		}

		/// <summary>Create a service entry with the sample endpoints</summary>
		/// <param name="persistence">persistence policy</param>
		/// <returns>the record</returns>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static ServiceRecord BuildExampleServiceEntry(string persistence)
		{
			ServiceRecord record = new ServiceRecord();
			record.Set(YarnRegistryAttributes.YarnId, "example-0001");
			record.Set(YarnRegistryAttributes.YarnPersistence, persistence);
			AddSampleEndpoints(record, "namenode");
			return record;
		}

		/// <summary>Add some endpoints</summary>
		/// <param name="entry">entry</param>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static void AddSampleEndpoints(ServiceRecord entry, string hostname)
		{
			NUnit.Framework.Assert.IsNotNull(hostname);
			entry.AddExternalEndpoint(RegistryTypeUtils.WebEndpoint(HttpApi, new URI("http", 
				hostname + ":80", "/")));
			entry.AddExternalEndpoint(RegistryTypeUtils.RestEndpoint(ApiWebhdfs, new URI("http"
				, hostname + ":8020", "/")));
			Endpoint endpoint = RegistryTypeUtils.IpcEndpoint(ApiHdfs, null);
			endpoint.addresses.AddItem(RegistryTypeUtils.HostnamePortPair(hostname, 8030));
			entry.AddInternalEndpoint(endpoint);
			IPEndPoint localhost = new IPEndPoint("localhost", 8050);
			entry.AddInternalEndpoint(RegistryTypeUtils.InetAddrEndpoint(Nnipc, ProtocolTypes
				.ProtocolThrift, "localhost", 8050));
			entry.AddInternalEndpoint(RegistryTypeUtils.IpcEndpoint(Ipc2, localhost));
		}

		/// <summary>
		/// Describe the stage in the process with a box around it -so as
		/// to highlight it in test logs
		/// </summary>
		/// <param name="log">log to use</param>
		/// <param name="text">text</param>
		/// <param name="args">logger args</param>
		public static void Describe(Logger log, string text, params object[] args)
		{
			log.Info("\n=======================================");
			log.Info(text, args);
			log.Info("=======================================\n");
		}

		/// <summary>log out from a context if non-null ...</summary>
		/// <remarks>log out from a context if non-null ... exceptions are caught and logged</remarks>
		/// <param name="login">login context</param>
		/// <returns>null, always</returns>
		public static LoginContext Logout(LoginContext login)
		{
			try
			{
				if (login != null)
				{
					Log.Debug("Logging out login context {}", login.ToString());
					login.Logout();
				}
			}
			catch (LoginException e)
			{
				Log.Warn("Exception logging out: {}", e, e);
			}
			return null;
		}

		/// <summary>Login via a UGI.</summary>
		/// <remarks>Login via a UGI. Requres UGI to have been set up</remarks>
		/// <param name="user">username</param>
		/// <param name="keytab">keytab to list</param>
		/// <returns>the UGI</returns>
		/// <exception cref="System.IO.IOException"/>
		public static UserGroupInformation LoginUGI(string user, FilePath keytab)
		{
			Log.Info("Logging in as {} from {}", user, keytab);
			return UserGroupInformation.LoginUserFromKeytabAndReturnUGI(user, keytab.GetAbsolutePath
				());
		}

		public static ServiceRecord CreateRecord(string persistence)
		{
			return CreateRecord("01", persistence, "description");
		}

		public static ServiceRecord CreateRecord(string id, string persistence, string description
			)
		{
			ServiceRecord serviceRecord = new ServiceRecord();
			serviceRecord.Set(YarnRegistryAttributes.YarnId, id);
			serviceRecord.description = description;
			serviceRecord.Set(YarnRegistryAttributes.YarnPersistence, persistence);
			return serviceRecord;
		}

		public static ServiceRecord CreateRecord(string id, string persistence, string description
			, string data)
		{
			return CreateRecord(id, persistence, description);
		}
	}
}
