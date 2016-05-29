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
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Registry.Client.Impl;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Api
{
	/// <summary>A factory for registry operation service instances.</summary>
	/// <remarks>
	/// A factory for registry operation service instances.
	/// <p>
	/// <i>Each created instance will be returned initialized.</i>
	/// <p>
	/// That is, the service will have had <code>Service.init(conf)</code> applied
	/// to it —possibly after the configuration has been modified to
	/// support the specific binding/security mechanism used
	/// </remarks>
	public sealed class RegistryOperationsFactory
	{
		private RegistryOperationsFactory()
		{
		}

		/// <summary>Create and initialize a registry operations instance.</summary>
		/// <remarks>
		/// Create and initialize a registry operations instance.
		/// Access writes will be determined from the configuration
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <returns>a registry operations instance</returns>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">on any failure to initialize
		/// 	</exception>
		public static RegistryOperations CreateInstance(Configuration conf)
		{
			return CreateInstance("RegistryOperations", conf);
		}

		/// <summary>Create and initialize a registry operations instance.</summary>
		/// <remarks>
		/// Create and initialize a registry operations instance.
		/// Access rights will be determined from the configuration
		/// </remarks>
		/// <param name="name">name of the instance</param>
		/// <param name="conf">configuration</param>
		/// <returns>a registry operations instance</returns>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">on any failure to initialize
		/// 	</exception>
		public static RegistryOperations CreateInstance(string name, Configuration conf)
		{
			Preconditions.CheckArgument(conf != null, "Null configuration");
			RegistryOperationsClient operations = new RegistryOperationsClient(name);
			operations.Init(conf);
			return operations;
		}

		/// <summary>Create and initialize an anonymous read/write registry operations instance.
		/// 	</summary>
		/// <remarks>
		/// Create and initialize an anonymous read/write registry operations instance.
		/// In a secure cluster, this instance will only have read access to the
		/// registry.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <returns>an anonymous registry operations instance</returns>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">on any failure to initialize
		/// 	</exception>
		public static RegistryOperations CreateAnonymousInstance(Configuration conf)
		{
			Preconditions.CheckArgument(conf != null, "Null configuration");
			conf.Set(KeyRegistryClientAuth, RegistryClientAuthAnonymous);
			return CreateInstance("AnonymousRegistryOperations", conf);
		}

		/// <summary>Create and initialize an secure, Kerberos-authenticated instance.</summary>
		/// <remarks>
		/// Create and initialize an secure, Kerberos-authenticated instance.
		/// The user identity will be inferred from the current user
		/// The authentication of this instance will expire when any kerberos
		/// tokens needed to authenticate with the registry infrastructure expire.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="jaasContext">the JAAS context of the account.</param>
		/// <returns>a registry operations instance</returns>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">on any failure to initialize
		/// 	</exception>
		public static RegistryOperations CreateKerberosInstance(Configuration conf, string
			 jaasContext)
		{
			Preconditions.CheckArgument(conf != null, "Null configuration");
			conf.Set(KeyRegistryClientAuth, RegistryClientAuthKerberos);
			conf.Set(KeyRegistryClientJaasContext, jaasContext);
			return CreateInstance("KerberosRegistryOperations", conf);
		}

		/// <summary>
		/// Create and initialize an operations instance authenticated with write
		/// access via an <code>id:password</code> pair.
		/// </summary>
		/// <remarks>
		/// Create and initialize an operations instance authenticated with write
		/// access via an <code>id:password</code> pair.
		/// The instance will have the read access
		/// across the registry, but write access only to that part of the registry
		/// to which it has been give the relevant permissions.
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="id">user ID</param>
		/// <param name="password">password</param>
		/// <returns>a registry operations instance</returns>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">on any failure to initialize
		/// 	</exception>
		/// <exception cref="System.ArgumentException">if an argument is invalid</exception>
		public static RegistryOperations CreateAuthenticatedInstance(Configuration conf, 
			string id, string password)
		{
			Preconditions.CheckArgument(!StringUtils.IsEmpty(id), "empty Id");
			Preconditions.CheckArgument(!StringUtils.IsEmpty(password), "empty Password");
			Preconditions.CheckArgument(conf != null, "Null configuration");
			conf.Set(KeyRegistryClientAuth, RegistryClientAuthDigest);
			conf.Set(KeyRegistryClientAuthenticationId, id);
			conf.Set(KeyRegistryClientAuthenticationPassword, password);
			return CreateInstance("DigestRegistryOperations", conf);
		}
	}
}
