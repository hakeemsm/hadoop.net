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

namespace Org.Apache.Hadoop.Registry.Client.Types
{
	/// <summary>Enum of address types -as integers.</summary>
	/// <remarks>
	/// Enum of address types -as integers.
	/// Why integers and not enums? Cross platform serialization as JSON
	/// </remarks>
	public abstract class AddressTypes
	{
		/// <summary>
		/// hostname/FQDN and port pair:
		/// <value/>
		/// .
		/// The host/domain name and port are set as separate strings in the address
		/// list, e.g.
		/// <pre>
		/// ["namenode.example.org", "50070"]
		/// </pre>
		/// </summary>
		public const string AddressHostnameAndPort = "host/port";

		public const string AddressHostnameField = "host";

		public const string AddressPortField = "port";

		/// <summary>
		/// Path <code>/a/b/c</code> style:
		/// <value/>
		/// .
		/// The entire path is encoded in a single entry
		/// <pre>
		/// ["/users/example/dataset"]
		/// </pre>
		/// </summary>
		public const string AddressPath = "path";

		/// <summary>
		/// URI entries:
		/// <value/>
		/// .
		/// <pre>
		/// ["http://example.org"]
		/// </pre>
		/// </summary>
		public const string AddressUri = "uri";

		/// <summary>
		/// Zookeeper addresses as a triple :
		/// <value/>
		/// .
		/// <p>
		/// These are provide as a 3 element tuple of: hostname, port
		/// and optionally path (depending on the application)
		/// <p>
		/// A single element would be
		/// <pre>
		/// ["zk1","2181","/registry"]
		/// </pre>
		/// An endpoint with multiple elements would list them as
		/// <pre>
		/// [
		/// ["zk1","2181","/registry"]
		/// ["zk2","1600","/registry"]
		/// ]
		/// </pre>
		/// the third element in each entry , the path, MUST be the same in each entry.
		/// A client reading the addresses of an endpoint is free to pick any
		/// of the set, so they must be the same.
		/// </summary>
		public const string AddressZookeeper = "zktriple";

		/// <summary>
		/// Any other address:
		/// <value/>
		/// .
		/// </summary>
		public const string AddressOther = string.Empty;
	}

	public static class AddressTypesConstants
	{
	}
}
