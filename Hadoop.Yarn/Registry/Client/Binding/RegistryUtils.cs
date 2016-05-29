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
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>Utility methods for working with a registry.</summary>
	public class RegistryUtils
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(RegistryUtils
			));

		/// <summary>Buld the user path -switches to the system path if the user is "".</summary>
		/// <remarks>
		/// Buld the user path -switches to the system path if the user is "".
		/// It also cross-converts the username to ascii via punycode
		/// </remarks>
		/// <param name="username">username or ""</param>
		/// <returns>the path to the user</returns>
		public static string HomePathForUser(string username)
		{
			Preconditions.CheckArgument(username != null, "null user");
			// catch recursion
			if (username.StartsWith(RegistryConstants.PathUsers))
			{
				return username;
			}
			if (username.IsEmpty())
			{
				return RegistryConstants.PathSystemServices;
			}
			// convert username to registry name
			string convertedName = ConvertUsername(username);
			return RegistryPathUtils.Join(RegistryConstants.PathUsers, RegistryPathUtils.EncodeForRegistry
				(convertedName));
		}

		/// <summary>
		/// Convert the username to that which can be used for registry
		/// entries.
		/// </summary>
		/// <remarks>
		/// Convert the username to that which can be used for registry
		/// entries. Lower cases it,
		/// Strip the kerberos realm off a username if needed, and any "/" hostname
		/// entries
		/// </remarks>
		/// <param name="username">user</param>
		/// <returns>the converted username</returns>
		public static string ConvertUsername(string username)
		{
			string converted = StringUtils.ToLowerCase(username);
			int atSymbol = converted.IndexOf('@');
			if (atSymbol > 0)
			{
				converted = Sharpen.Runtime.Substring(converted, 0, atSymbol);
			}
			int slashSymbol = converted.IndexOf('/');
			if (slashSymbol > 0)
			{
				converted = Sharpen.Runtime.Substring(converted, 0, slashSymbol);
			}
			return converted;
		}

		/// <summary>Create a service classpath</summary>
		/// <param name="user">username or ""</param>
		/// <param name="serviceClass">service name</param>
		/// <returns>a full path</returns>
		public static string ServiceclassPath(string user, string serviceClass)
		{
			string services = RegistryPathUtils.Join(HomePathForUser(user), RegistryConstants
				.PathUserServices);
			return RegistryPathUtils.Join(services, serviceClass);
		}

		/// <summary>Create a path to a service under a user and service class</summary>
		/// <param name="user">username or ""</param>
		/// <param name="serviceClass">service name</param>
		/// <param name="serviceName">service name unique for that user and service class</param>
		/// <returns>a full path</returns>
		public static string ServicePath(string user, string serviceClass, string serviceName
			)
		{
			return RegistryPathUtils.Join(ServiceclassPath(user, serviceClass), serviceName);
		}

		/// <summary>Create a path for listing components under a service</summary>
		/// <param name="user">username or ""</param>
		/// <param name="serviceClass">service name</param>
		/// <param name="serviceName">service name unique for that user and service class</param>
		/// <returns>a full path</returns>
		public static string ComponentListPath(string user, string serviceClass, string serviceName
			)
		{
			return RegistryPathUtils.Join(ServicePath(user, serviceClass, serviceName), RegistryConstants
				.SubpathComponents);
		}

		/// <summary>Create the path to a service record for a component</summary>
		/// <param name="user">username or ""</param>
		/// <param name="serviceClass">service name</param>
		/// <param name="serviceName">service name unique for that user and service class</param>
		/// <param name="componentName">unique name/ID of the component</param>
		/// <returns>a full path</returns>
		public static string ComponentPath(string user, string serviceClass, string serviceName
			, string componentName)
		{
			return RegistryPathUtils.Join(ComponentListPath(user, serviceClass, serviceName), 
				componentName);
		}

		/// <summary>List service records directly under a path</summary>
		/// <param name="registryOperations">registry operations instance</param>
		/// <param name="path">path to list</param>
		/// <returns>
		/// a mapping of the service records that were resolved, indexed
		/// by their full path
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static IDictionary<string, ServiceRecord> ListServiceRecords(RegistryOperations
			 registryOperations, string path)
		{
			IDictionary<string, RegistryPathStatus> children = StatChildren(registryOperations
				, path);
			return ExtractServiceRecords(registryOperations, path, children.Values);
		}

		/// <summary>
		/// List children of a directory and retrieve their
		/// <see cref="Org.Apache.Hadoop.Registry.Client.Types.RegistryPathStatus"/>
		/// values.
		/// <p>
		/// This is not an atomic operation; A child may be deleted
		/// during the iteration through the child entries. If this happens,
		/// the <code>PathNotFoundException</code> is caught and that child
		/// entry ommitted.
		/// </summary>
		/// <param name="path">path</param>
		/// <returns>
		/// a possibly empty map of child entries listed by
		/// their short name.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">path is not in the registry.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">the path is invalid.</exception>
		/// <exception cref="System.IO.IOException">Any other IO Exception</exception>
		public static IDictionary<string, RegistryPathStatus> StatChildren(RegistryOperations
			 registryOperations, string path)
		{
			IList<string> childNames = registryOperations.List(path);
			IDictionary<string, RegistryPathStatus> results = new Dictionary<string, RegistryPathStatus
				>();
			foreach (string childName in childNames)
			{
				string child = RegistryPathUtils.Join(path, childName);
				try
				{
					RegistryPathStatus stat = registryOperations.Stat(child);
					results[childName] = stat;
				}
				catch (PathNotFoundException pnfe)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("stat failed on {}: moved? {}", child, pnfe, pnfe);
					}
				}
			}
			// and continue
			return results;
		}

		/// <summary>Get the home path of the current user.</summary>
		/// <remarks>
		/// Get the home path of the current user.
		/// <p>
		/// In an insecure cluster, the environment variable
		/// <code>HADOOP_USER_NAME</code> is queried <i>first</i>.
		/// <p>
		/// This means that in a YARN container where the creator set this
		/// environment variable to propagate their identity, the defined
		/// user name is used in preference to the actual user.
		/// <p>
		/// In a secure cluster, the kerberos identity of the current user is used.
		/// </remarks>
		/// <returns>a path for the current user's home dir.</returns>
		/// <exception cref="Sharpen.RuntimeException">
		/// if the current user identity cannot be determined
		/// from the OS/kerberos.
		/// </exception>
		public static string HomePathForCurrentUser()
		{
			string shortUserName = CurrentUsernameUnencoded();
			return HomePathForUser(shortUserName);
		}

		/// <summary>Get the current username, before any encoding has been applied.</summary>
		/// <returns>
		/// the current user from the kerberos identity, falling back
		/// to the user and/or env variables.
		/// </returns>
		private static string CurrentUsernameUnencoded()
		{
			string env_hadoop_username = Runtime.Getenv(RegistryInternalConstants.HadoopUserName
				);
			return GetCurrentUsernameUnencoded(env_hadoop_username);
		}

		/// <summary>
		/// Get the current username, using the value of the parameter
		/// <code>env_hadoop_username</code> if it is set on an insecure cluster.
		/// </summary>
		/// <remarks>
		/// Get the current username, using the value of the parameter
		/// <code>env_hadoop_username</code> if it is set on an insecure cluster.
		/// This ensures that the username propagates correctly across processes
		/// started by YARN.
		/// <p>
		/// This method is primarly made visible for testing.
		/// </remarks>
		/// <param name="env_hadoop_username">the environment variable</param>
		/// <returns>the selected username</returns>
		/// <exception cref="Sharpen.RuntimeException">
		/// if there is a problem getting the short user
		/// name of the current user.
		/// </exception>
		[VisibleForTesting]
		public static string GetCurrentUsernameUnencoded(string env_hadoop_username)
		{
			string shortUserName = null;
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				shortUserName = env_hadoop_username;
			}
			if (StringUtils.IsEmpty(shortUserName))
			{
				try
				{
					shortUserName = UserGroupInformation.GetCurrentUser().GetShortUserName();
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
			return shortUserName;
		}

		/// <summary>
		/// Get the current user path formatted for the registry
		/// <p>
		/// In an insecure cluster, the environment variable
		/// <code>HADOOP_USER_NAME </code> is queried <i>first</i>.
		/// </summary>
		/// <remarks>
		/// Get the current user path formatted for the registry
		/// <p>
		/// In an insecure cluster, the environment variable
		/// <code>HADOOP_USER_NAME </code> is queried <i>first</i>.
		/// <p>
		/// This means that in a YARN container where the creator set this
		/// environment variable to propagate their identity, the defined
		/// user name is used in preference to the actual user.
		/// <p>
		/// In a secure cluster, the kerberos identity of the current user is used.
		/// </remarks>
		/// <returns>the encoded shortname of the current user</returns>
		/// <exception cref="Sharpen.RuntimeException">
		/// if the current user identity cannot be determined
		/// from the OS/kerberos.
		/// </exception>
		public static string CurrentUser()
		{
			string shortUserName = CurrentUsernameUnencoded();
			return RegistryPathUtils.EncodeForRegistry(shortUserName);
		}

		/// <summary>
		/// Extract all service records under a list of stat operations...this
		/// skips entries that are too short or simply not matching
		/// </summary>
		/// <param name="operations">operation support for fetches</param>
		/// <param name="parentpath">path of the parent of all the entries</param>
		/// <param name="stats">Collection of stat results</param>
		/// <returns>a possibly empty map of fullpath:record.</returns>
		/// <exception cref="System.IO.IOException">for any IO Operation that wasn't ignored.
		/// 	</exception>
		public static IDictionary<string, ServiceRecord> ExtractServiceRecords(RegistryOperations
			 operations, string parentpath, ICollection<RegistryPathStatus> stats)
		{
			IDictionary<string, ServiceRecord> results = new Dictionary<string, ServiceRecord
				>(stats.Count);
			foreach (RegistryPathStatus stat in stats)
			{
				if (stat.size > ServiceRecord.RecordType.Length)
				{
					// maybe has data
					string path = RegistryPathUtils.Join(parentpath, stat.path);
					try
					{
						ServiceRecord serviceRecord = operations.Resolve(path);
						results[path] = serviceRecord;
					}
					catch (EOFException)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("data too short for {}", path);
						}
					}
					catch (InvalidRecordException)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Invalid record at {}", path);
						}
					}
					catch (NoRecordException)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("No record at {}", path);
						}
					}
				}
			}
			return results;
		}

		/// <summary>
		/// Extract all service records under a list of stat operations...this
		/// non-atomic action skips entries that are too short or simply not matching.
		/// </summary>
		/// <remarks>
		/// Extract all service records under a list of stat operations...this
		/// non-atomic action skips entries that are too short or simply not matching.
		/// <p>
		/// </remarks>
		/// <param name="operations">operation support for fetches</param>
		/// <param name="parentpath">path of the parent of all the entries</param>
		/// <returns>a possibly empty map of fullpath:record.</returns>
		/// <exception cref="System.IO.IOException">for any IO Operation that wasn't ignored.
		/// 	</exception>
		public static IDictionary<string, ServiceRecord> ExtractServiceRecords(RegistryOperations
			 operations, string parentpath, IDictionary<string, RegistryPathStatus> stats)
		{
			return ExtractServiceRecords(operations, parentpath, stats.Values);
		}

		/// <summary>
		/// Extract all service records under a list of stat operations...this
		/// non-atomic action skips entries that are too short or simply not matching.
		/// </summary>
		/// <remarks>
		/// Extract all service records under a list of stat operations...this
		/// non-atomic action skips entries that are too short or simply not matching.
		/// <p>
		/// </remarks>
		/// <param name="operations">operation support for fetches</param>
		/// <param name="parentpath">path of the parent of all the entries</param>
		/// <returns>a possibly empty map of fullpath:record.</returns>
		/// <exception cref="System.IO.IOException">for any IO Operation that wasn't ignored.
		/// 	</exception>
		public static IDictionary<string, ServiceRecord> ExtractServiceRecords(RegistryOperations
			 operations, string parentpath)
		{
			return ExtractServiceRecords(operations, parentpath, StatChildren(operations, parentpath
				).Values);
		}

		/// <summary>Static instance of service record marshalling</summary>
		public class ServiceRecordMarshal : JsonSerDeser<ServiceRecord>
		{
			public ServiceRecordMarshal()
				: base(typeof(ServiceRecord))
			{
			}
		}
	}
}
