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
	/// <summary>some common protocol types</summary>
	public abstract class ProtocolTypes
	{
		/// <summary>
		/// Addresses are URIs of Hadoop Filesystem paths:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolFilesystem = "hadoop/filesystem";

		/// <summary>
		/// Hadoop IPC,  "classic" or protobuf :
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolHadoopIpc = "hadoop/IPC";

		/// <summary>
		/// Corba IIOP:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolIiop = "IIOP";

		/// <summary>
		/// REST:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolRest = "REST";

		/// <summary>
		/// Java RMI:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolRmi = "RMI";

		/// <summary>
		/// SunOS RPC, as used by NFS and similar:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolSunRpc = "sunrpc";

		/// <summary>
		/// Thrift-based protocols:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolThrift = "thrift";

		/// <summary>
		/// Custom TCP protocol:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolTcp = "tcp";

		/// <summary>
		/// Custom UPC-based protocol :
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolUdp = "udp";

		/// <summary>
		/// Default value —the protocol is unknown : "
		/// <value/>
		/// "
		/// </summary>
		public const string ProtocolUnknown = string.Empty;

		/// <summary>
		/// Web page:
		/// <value/>
		/// .
		/// This protocol implies that the URLs are designed for
		/// people to view via web browsers.
		/// </summary>
		public const string ProtocolWebui = "webui";

		/// <summary>
		/// Web Services:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolWsapi = "WS-*";

		/// <summary>
		/// A zookeeper binding:
		/// <value/>
		/// .
		/// </summary>
		public const string ProtocolZookeeperBinding = "zookeeper";
	}

	public static class ProtocolTypesConstants
	{
	}
}
