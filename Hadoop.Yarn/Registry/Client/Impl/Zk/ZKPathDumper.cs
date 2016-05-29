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
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>This class dumps a registry tree to a string.</summary>
	/// <remarks>
	/// This class dumps a registry tree to a string.
	/// It does this in the <code>toString()</code> method, so it
	/// can be used in a log statement -the operation
	/// will only take place if the method is evaluated.
	/// </remarks>
	public class ZKPathDumper
	{
		public const int Indent = 2;

		private readonly CuratorFramework curator;

		private readonly string root;

		private readonly bool verbose;

		/// <summary>Create a path dumper -but do not dump the path until asked</summary>
		/// <param name="curator">curator instance</param>
		/// <param name="root">root</param>
		/// <param name="verbose">verbose flag - includes more details (such as ACLs)</param>
		public ZKPathDumper(CuratorFramework curator, string root, bool verbose)
		{
			Preconditions.CheckArgument(curator != null);
			Preconditions.CheckArgument(root != null);
			this.curator = curator;
			this.root = root;
			this.verbose = verbose;
		}

		/// <summary>Trigger the recursive registry dump.</summary>
		/// <returns>a string view of the registry</returns>
		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			builder.Append("ZK tree for ").Append(root).Append('\n');
			Expand(builder, root, 1);
			return builder.ToString();
		}

		/// <summary>
		/// Recursively expand the path into the supplied string builder, increasing
		/// the indentation by
		/// <see cref="Indent"/>
		/// as it proceeds (depth first) down
		/// the tree
		/// </summary>
		/// <param name="builder">string build to append to</param>
		/// <param name="path">path to examine</param>
		/// <param name="indent">current indentation</param>
		private void Expand(StringBuilder builder, string path, int indent)
		{
			try
			{
				GetChildrenBuilder childrenBuilder = curator.GetChildren();
				IList<string> children = childrenBuilder.ForPath(path);
				foreach (string child in children)
				{
					string childPath = path + "/" + child;
					string body;
					Stat stat = curator.CheckExists().ForPath(childPath);
					StringBuilder bodyBuilder = new StringBuilder(256);
					bodyBuilder.Append("  [").Append(stat.GetDataLength()).Append("]");
					if (stat.GetEphemeralOwner() > 0)
					{
						bodyBuilder.Append("*");
					}
					if (verbose)
					{
						// verbose: extract ACLs
						builder.Append(" -- ");
						IList<ACL> acls = curator.GetACL().ForPath(childPath);
						foreach (ACL acl in acls)
						{
							builder.Append(RegistrySecurity.AclToString(acl));
							builder.Append(" ");
						}
					}
					body = bodyBuilder.ToString();
					// print each child
					Append(builder, indent, ' ');
					builder.Append('/').Append(child);
					builder.Append(body);
					builder.Append('\n');
					// recurse
					Expand(builder, childPath, indent + Indent);
				}
			}
			catch (Exception e)
			{
				builder.Append(e.ToString()).Append("\n");
			}
		}

		/// <summary>Append the specified indentation to a builder</summary>
		/// <param name="builder">string build to append to</param>
		/// <param name="indent">current indentation</param>
		/// <param name="c">charactor to use for indentation</param>
		private void Append(StringBuilder builder, int indent, char c)
		{
			for (int i = 0; i < indent; i++)
			{
				builder.Append(c);
			}
		}
	}
}
