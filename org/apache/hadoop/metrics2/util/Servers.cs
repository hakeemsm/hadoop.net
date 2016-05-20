/*
* Util.java
*
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

namespace org.apache.hadoop.metrics2.util
{
	/// <summary>Helpers to handle server addresses</summary>
	public class Servers
	{
		/// <summary>This class is not intended to be instantiated</summary>
		private Servers()
		{
		}

		/// <summary>
		/// Parses a space and/or comma separated sequence of server specifications
		/// of the form <i>hostname</i> or <i>hostname:port</i>.
		/// </summary>
		/// <remarks>
		/// Parses a space and/or comma separated sequence of server specifications
		/// of the form <i>hostname</i> or <i>hostname:port</i>.  If
		/// the specs string is null, defaults to localhost:defaultPort.
		/// </remarks>
		/// <param name="specs">server specs (see description)</param>
		/// <param name="defaultPort">the default port if not specified</param>
		/// <returns>a list of InetSocketAddress objects.</returns>
		public static System.Collections.Generic.IList<java.net.InetSocketAddress> parse(
			string specs, int defaultPort)
		{
			System.Collections.Generic.IList<java.net.InetSocketAddress> result = com.google.common.collect.Lists
				.newArrayList();
			if (specs == null)
			{
				result.add(new java.net.InetSocketAddress("localhost", defaultPort));
			}
			else
			{
				string[] specStrings = specs.split("[ ,]+");
				foreach (string specString in specStrings)
				{
					result.add(org.apache.hadoop.net.NetUtils.createSocketAddr(specString, defaultPort
						));
				}
			}
			return result;
		}
	}
}
