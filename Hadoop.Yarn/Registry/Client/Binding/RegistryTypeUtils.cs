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
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Types;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>
	/// Static methods to work with registry types —primarily endpoints and the
	/// list representation of addresses.
	/// </summary>
	public class RegistryTypeUtils
	{
		/// <summary>Create a URL endpoint from a list of URIs</summary>
		/// <param name="api">implemented API</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="uris">URIs</param>
		/// <returns>a new endpoint</returns>
		public static Endpoint UrlEndpoint(string api, string protocolType, params URI[] 
			uris)
		{
			return new Endpoint(api, protocolType, uris);
		}

		/// <summary>Create a REST endpoint from a list of URIs</summary>
		/// <param name="api">implemented API</param>
		/// <param name="uris">URIs</param>
		/// <returns>a new endpoint</returns>
		public static Endpoint RestEndpoint(string api, params URI[] uris)
		{
			return UrlEndpoint(api, ProtocolTypes.ProtocolRest, uris);
		}

		/// <summary>Create a Web UI endpoint from a list of URIs</summary>
		/// <param name="api">implemented API</param>
		/// <param name="uris">URIs</param>
		/// <returns>a new endpoint</returns>
		public static Endpoint WebEndpoint(string api, params URI[] uris)
		{
			return UrlEndpoint(api, ProtocolTypes.ProtocolWebui, uris);
		}

		/// <summary>Create an internet address endpoint from a list of URIs</summary>
		/// <param name="api">implemented API</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="hostname">hostname/FQDN</param>
		/// <param name="port">port</param>
		/// <returns>a new endpoint</returns>
		public static Endpoint InetAddrEndpoint(string api, string protocolType, string hostname
			, int port)
		{
			Preconditions.CheckArgument(api != null, "null API");
			Preconditions.CheckArgument(protocolType != null, "null protocolType");
			Preconditions.CheckArgument(hostname != null, "null hostname");
			return new Endpoint(api, AddressHostnameAndPort, protocolType, HostnamePortPair(hostname
				, port));
		}

		/// <summary>Create an IPC endpoint</summary>
		/// <param name="api">API</param>
		/// <param name="address">the address as a tuple of (hostname, port)</param>
		/// <returns>the new endpoint</returns>
		public static Endpoint IpcEndpoint(string api, IPEndPoint address)
		{
			return new Endpoint(api, AddressHostnameAndPort, ProtocolTypes.ProtocolHadoopIpc, 
				address == null ? null : HostnamePortPair(address));
		}

		/// <summary>Create a single entry map</summary>
		/// <param name="key">map entry key</param>
		/// <param name="val">map entry value</param>
		/// <returns>a 1 entry map.</returns>
		public static IDictionary<string, string> Map(string key, string val)
		{
			IDictionary<string, string> map = new Dictionary<string, string>(1);
			map[key] = val;
			return map;
		}

		/// <summary>Create a URI</summary>
		/// <param name="uri">value</param>
		/// <returns>a 1 entry map.</returns>
		public static IDictionary<string, string> Uri(string uri)
		{
			return Map(AddressUri, uri);
		}

		/// <summary>Create a (hostname, port) address pair</summary>
		/// <param name="hostname">hostname</param>
		/// <param name="port">port</param>
		/// <returns>a 1 entry map.</returns>
		public static IDictionary<string, string> HostnamePortPair(string hostname, int port
			)
		{
			IDictionary<string, string> map = Map(AddressHostnameField, hostname);
			map[AddressPortField] = Sharpen.Extensions.ToString(port);
			return map;
		}

		/// <summary>Create a (hostname, port) address pair</summary>
		/// <param name="address">
		/// socket address whose hostname and port are used for the
		/// generated address.
		/// </param>
		/// <returns>a 1 entry map.</returns>
		public static IDictionary<string, string> HostnamePortPair(IPEndPoint address)
		{
			return HostnamePortPair(address.GetHostName(), address.Port);
		}

		/// <summary>Require a specific address type on an endpoint</summary>
		/// <param name="required">required type</param>
		/// <param name="epr">endpoint</param>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">if the type is wrong</exception>
		public static void RequireAddressType(string required, Endpoint epr)
		{
			if (!required.Equals(epr.addressType))
			{
				throw new InvalidRecordException(epr.ToString(), "Address type of " + epr.addressType
					 + " does not match required type of " + required);
			}
		}

		/// <summary>Get a single URI endpoint</summary>
		/// <param name="epr">endpoint</param>
		/// <returns>
		/// the uri of the first entry in the address list. Null if the endpoint
		/// itself is null
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">
		/// if the type is wrong, there are no addresses
		/// or the payload ill-formatted
		/// </exception>
		public static IList<string> RetrieveAddressesUriType(Endpoint epr)
		{
			if (epr == null)
			{
				return null;
			}
			RequireAddressType(AddressUri, epr);
			IList<IDictionary<string, string>> addresses = epr.addresses;
			if (addresses.Count < 1)
			{
				throw new InvalidRecordException(epr.ToString(), "No addresses in endpoint");
			}
			IList<string> results = new AList<string>(addresses.Count);
			foreach (IDictionary<string, string> address in addresses)
			{
				results.AddItem(GetAddressField(address, AddressUri));
			}
			return results;
		}

		/// <summary>
		/// Get a specific field from an address -raising an exception if
		/// the field is not present
		/// </summary>
		/// <param name="address">address to query</param>
		/// <param name="field">field to resolve</param>
		/// <returns>the resolved value. Guaranteed to be non-null.</returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">if the field did not resolve</exception>
		public static string GetAddressField(IDictionary<string, string> address, string 
			field)
		{
			string val = address[field];
			if (val == null)
			{
				throw new InvalidRecordException(string.Empty, "Missing address field: " + field);
			}
			return val;
		}

		/// <summary>Get the address URLs.</summary>
		/// <remarks>Get the address URLs. Guranteed to return at least one address.</remarks>
		/// <param name="epr">endpoint</param>
		/// <returns>the address as a URL</returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">
		/// if the type is wrong, there are no addresses
		/// or the payload ill-formatted
		/// </exception>
		/// <exception cref="System.UriFormatException">address can't be turned into a URL</exception>
		public static IList<System.Uri> RetrieveAddressURLs(Endpoint epr)
		{
			if (epr == null)
			{
				throw new InvalidRecordException(string.Empty, "Null endpoint");
			}
			IList<string> addresses = RetrieveAddressesUriType(epr);
			IList<System.Uri> results = new AList<System.Uri>(addresses.Count);
			foreach (string address in addresses)
			{
				results.AddItem(new System.Uri(address));
			}
			return results;
		}

		/// <summary>
		/// Validate the record by checking for null fields and other invalid
		/// conditions
		/// </summary>
		/// <param name="path">path for exceptions</param>
		/// <param name="record">record to validate. May be null</param>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">on invalid entries</exception>
		public static void ValidateServiceRecord(string path, ServiceRecord record)
		{
			if (record == null)
			{
				throw new InvalidRecordException(path, "Null record");
			}
			if (!ServiceRecord.RecordType.Equals(record.type))
			{
				throw new InvalidRecordException(path, "invalid record type field: \"" + record.type
					 + "\"");
			}
			if (record.external != null)
			{
				foreach (Endpoint endpoint in record.external)
				{
					ValidateEndpoint(path, endpoint);
				}
			}
			if (record.@internal != null)
			{
				foreach (Endpoint endpoint in record.@internal)
				{
					ValidateEndpoint(path, endpoint);
				}
			}
		}

		/// <summary>
		/// Validate the endpoint by checking for null fields and other invalid
		/// conditions
		/// </summary>
		/// <param name="path">path for exceptions</param>
		/// <param name="endpoint">endpoint to validate. May be null</param>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidRecordException
		/// 	">on invalid entries</exception>
		public static void ValidateEndpoint(string path, Endpoint endpoint)
		{
			if (endpoint == null)
			{
				throw new InvalidRecordException(path, "Null endpoint");
			}
			try
			{
				endpoint.Validate();
			}
			catch (RuntimeException e)
			{
				throw new InvalidRecordException(path, e.ToString());
			}
		}
	}
}
