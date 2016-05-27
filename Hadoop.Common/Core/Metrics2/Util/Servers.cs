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
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Util
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
		public static IList<IPEndPoint> Parse(string specs, int defaultPort)
		{
			IList<IPEndPoint> result = Lists.NewArrayList();
			if (specs == null)
			{
				result.AddItem(new IPEndPoint("localhost", defaultPort));
			}
			else
			{
				string[] specStrings = specs.Split("[ ,]+");
				foreach (string specString in specStrings)
				{
					result.AddItem(NetUtils.CreateSocketAddr(specString, defaultPort));
				}
			}
			return result;
		}
	}
}
