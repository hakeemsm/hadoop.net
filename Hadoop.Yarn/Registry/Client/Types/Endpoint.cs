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
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Types
{
	/// <summary>Description of a single service/component endpoint.</summary>
	/// <remarks>
	/// Description of a single service/component endpoint.
	/// It is designed to be marshalled as JSON.
	/// <p>
	/// Every endpoint can have more than one address entry, such as
	/// a list of URLs to a replicated service, or a (hostname, port)
	/// pair. Each of these address entries is represented as a string list,
	/// as that is the only reliably marshallable form of a tuple JSON can represent.
	/// </remarks>
	public sealed class Endpoint : ICloneable
	{
		/// <summary>API implemented at the end of the binding</summary>
		public string api;

		/// <summary>Type of address.</summary>
		/// <remarks>
		/// Type of address. The standard types are defined in
		/// <see cref="AddressTypes"/>
		/// </remarks>
		public string addressType;

		/// <summary>Protocol type.</summary>
		/// <remarks>
		/// Protocol type. Some standard types are defined in
		/// <see cref="ProtocolTypes"/>
		/// </remarks>
		public string protocolType;

		/// <summary>a list of address tuples —tuples whose format depends on the address type
		/// 	</summary>
		public IList<IDictionary<string, string>> addresses;

		/// <summary>Create an empty instance.</summary>
		public Endpoint()
		{
		}

		/// <summary>Create an endpoint from another endpoint.</summary>
		/// <remarks>
		/// Create an endpoint from another endpoint.
		/// This is a deep clone with a new list of addresses.
		/// </remarks>
		/// <param name="that">the endpoint to copy from</param>
		public Endpoint(Org.Apache.Hadoop.Registry.Client.Types.Endpoint that)
		{
			this.api = that.api;
			this.addressType = that.addressType;
			this.protocolType = that.protocolType;
			this.addresses = NewAddresses(that.addresses.Count);
			foreach (IDictionary<string, string> address in that.addresses)
			{
				IDictionary<string, string> addr2 = new Dictionary<string, string>(address.Count);
				addr2.PutAll(address);
				addresses.AddItem(addr2);
			}
		}

		/// <summary>Build an endpoint with a list of addresses</summary>
		/// <param name="api">API name</param>
		/// <param name="addressType">address type</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="addrs">addresses</param>
		public Endpoint(string api, string addressType, string protocolType, IList<IDictionary
			<string, string>> addrs)
		{
			this.api = api;
			this.addressType = addressType;
			this.protocolType = protocolType;
			this.addresses = NewAddresses(0);
			if (addrs != null)
			{
				Sharpen.Collections.AddAll(addresses, addrs);
			}
		}

		/// <summary>Build an endpoint with an empty address list</summary>
		/// <param name="api">API name</param>
		/// <param name="addressType">address type</param>
		/// <param name="protocolType">protocol type</param>
		public Endpoint(string api, string addressType, string protocolType)
		{
			this.api = api;
			this.addressType = addressType;
			this.protocolType = protocolType;
			this.addresses = NewAddresses(0);
		}

		/// <summary>Build an endpoint with a single address entry.</summary>
		/// <remarks>
		/// Build an endpoint with a single address entry.
		/// <p>
		/// This constructor is superfluous given the varags constructor is equivalent
		/// for a single element argument. However, type-erasure in java generics
		/// causes javac to warn about unchecked generic array creation. This
		/// constructor, which represents the common "one address" case, does
		/// not generate compile-time warnings.
		/// </remarks>
		/// <param name="api">API name</param>
		/// <param name="addressType">address type</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="addr">address. May be null —in which case it is not added</param>
		public Endpoint(string api, string addressType, string protocolType, IDictionary<
			string, string> addr)
			: this(api, addressType, protocolType)
		{
			if (addr != null)
			{
				addresses.AddItem(addr);
			}
		}

		/// <summary>Build an endpoint with a list of addresses</summary>
		/// <param name="api">API name</param>
		/// <param name="addressType">address type</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="addrs">addresses. Null elements will be skipped</param>
		public Endpoint(string api, string addressType, string protocolType, params IDictionary
			<string, string>[] addrs)
			: this(api, addressType, protocolType)
		{
			foreach (IDictionary<string, string> addr in addrs)
			{
				if (addr != null)
				{
					addresses.AddItem(addr);
				}
			}
		}

		/// <summary>Create a new address structure of the requested size</summary>
		/// <param name="size">size to create</param>
		/// <returns>the new list</returns>
		private IList<IDictionary<string, string>> NewAddresses(int size)
		{
			return new AList<IDictionary<string, string>>(size);
		}

		/// <summary>
		/// Build an endpoint from a list of URIs; each URI
		/// is ASCII-encoded and added to the list of addresses.
		/// </summary>
		/// <param name="api">API name</param>
		/// <param name="protocolType">protocol type</param>
		/// <param name="uris">URIs to convert to a list of tup;les</param>
		public Endpoint(string api, string protocolType, params URI[] uris)
		{
			this.api = api;
			this.addressType = AddressTypes.AddressUri;
			this.protocolType = protocolType;
			IList<IDictionary<string, string>> addrs = NewAddresses(uris.Length);
			foreach (URI uri in uris)
			{
				addrs.AddItem(RegistryTypeUtils.Uri(uri.ToString()));
			}
			this.addresses = addrs;
		}

		public override string ToString()
		{
			return marshalToString.ToString(this);
		}

		/// <summary>
		/// Validate the record by checking for null fields and other invalid
		/// conditions
		/// </summary>
		/// <exception cref="System.ArgumentNullException">
		/// if a field is null when it
		/// MUST be set.
		/// </exception>
		/// <exception cref="Sharpen.RuntimeException">on invalid entries</exception>
		public void Validate()
		{
			Preconditions.CheckNotNull(api, "null API field");
			Preconditions.CheckNotNull(addressType, "null addressType field");
			Preconditions.CheckNotNull(protocolType, "null protocolType field");
			Preconditions.CheckNotNull(addresses, "null addresses field");
			foreach (IDictionary<string, string> address in addresses)
			{
				Preconditions.CheckNotNull(address, "null element in address");
			}
		}

		/// <summary>Shallow clone: the lists of addresses are shared</summary>
		/// <returns>a cloned instance</returns>
		/// <exception cref="Sharpen.CloneNotSupportedException"/>
		public object Clone()
		{
			return base.MemberwiseClone();
		}

		/// <summary>Static instance of service record marshalling</summary>
		private class Marshal : JsonSerDeser<Endpoint>
		{
			private Marshal()
				: base(typeof(Endpoint))
			{
			}
		}

		private static readonly Endpoint.Marshal marshalToString = new Endpoint.Marshal();
	}
}
