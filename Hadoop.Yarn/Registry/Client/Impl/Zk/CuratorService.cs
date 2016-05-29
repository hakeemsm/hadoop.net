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
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Curator.Ensemble;
using Org.Apache.Curator.Ensemble.Fixed;
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Curator.Retry;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Registry.Client.Api;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Service;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl.ZK
{
	/// <summary>This service binds to Zookeeper via Apache Curator.</summary>
	/// <remarks>
	/// This service binds to Zookeeper via Apache Curator. It is more
	/// generic than just the YARN service registry; it does not implement
	/// any of the Registry Operations API.
	/// </remarks>
	public class CuratorService : CompositeService, RegistryConstants, RegistryBindingSource
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Client.Impl.ZK.CuratorService
			));

		/// <summary>the Curator binding</summary>
		private CuratorFramework curator;

		/// <summary>Path to the registry root</summary>
		private string registryRoot;

		/// <summary>Supplied binding source.</summary>
		/// <remarks>
		/// Supplied binding source. This defaults to being this
		/// service itself.
		/// </remarks>
		private readonly RegistryBindingSource bindingSource;

		/// <summary>Security service</summary>
		private RegistrySecurity registrySecurity;

		/// <summary>the connection binding text for messages</summary>
		private string connectionDescription;

		/// <summary>Security connection diagnostics</summary>
		private string securityConnectionDiagnostics = string.Empty;

		/// <summary>
		/// Provider of curator "ensemble"; offers a basis for
		/// more flexible bonding in future.
		/// </summary>
		private EnsembleProvider ensembleProvider;

		/// <summary>Construct the service.</summary>
		/// <param name="name">service name</param>
		/// <param name="bindingSource">
		/// source of binding information.
		/// If null: use this instance
		/// </param>
		public CuratorService(string name, RegistryBindingSource bindingSource)
			: base(name)
		{
			if (bindingSource != null)
			{
				this.bindingSource = bindingSource;
			}
			else
			{
				this.bindingSource = this;
			}
		}

		/// <summary>Create an instance using this service as the binding source (i.e.</summary>
		/// <remarks>
		/// Create an instance using this service as the binding source (i.e. read
		/// configuration options from the registry)
		/// </remarks>
		/// <param name="name">service name</param>
		public CuratorService(string name)
			: this(name, null)
		{
		}

		/// <summary>Init the service.</summary>
		/// <remarks>
		/// Init the service.
		/// This is where the security bindings are set up
		/// </remarks>
		/// <param name="conf">configuration of the service</param>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			registryRoot = conf.GetTrimmed(KeyRegistryZkRoot, DefaultZkRegistryRoot);
			// create and add the registy service
			registrySecurity = new RegistrySecurity("registry security");
			AddService(registrySecurity);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating Registry with root {}", registryRoot);
			}
			base.ServiceInit(conf);
		}

		/// <summary>Start the service.</summary>
		/// <remarks>
		/// Start the service.
		/// This is where the curator instance is started.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			base.ServiceStart();
			// create the curator; rely on the registry security code
			// to set up the JVM context and curator
			curator = CreateCurator();
		}

		/// <summary>Close the ZK connection if it is open</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			IOUtils.CloseStream(curator);
			base.ServiceStop();
		}

		/// <summary>Internal check that a service is in the live state</summary>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException">if not</exception>
		private void CheckServiceLive()
		{
			if (!IsInState(Service.STATE.Started))
			{
				throw new ServiceStateException("Service " + GetName() + " is in wrong state: " +
					 GetServiceState());
			}
		}

		/// <summary>Flag to indicate whether or not the registry is secure.</summary>
		/// <remarks>
		/// Flag to indicate whether or not the registry is secure.
		/// Valid once the service is inited.
		/// </remarks>
		/// <returns>service security policy</returns>
		public virtual bool IsSecure()
		{
			return registrySecurity.IsSecureRegistry();
		}

		/// <summary>Get the registry security helper</summary>
		/// <returns>the registry security helper</returns>
		protected internal virtual RegistrySecurity GetRegistrySecurity()
		{
			return registrySecurity;
		}

		/// <summary>Build the security diagnostics string</summary>
		/// <returns>a string for diagnostics</returns>
		protected internal virtual string BuildSecurityDiagnostics()
		{
			// build up the security connection diags
			if (!IsSecure())
			{
				return "security disabled";
			}
			else
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("secure cluster; ");
				builder.Append(registrySecurity.BuildSecurityDiagnostics());
				return builder.ToString();
			}
		}

		/// <summary>
		/// Create a new curator instance off the root path; using configuration
		/// options provided in the service configuration to set timeouts and
		/// retry policy.
		/// </summary>
		/// <returns>the newly created creator</returns>
		/// <exception cref="System.IO.IOException"/>
		private CuratorFramework CreateCurator()
		{
			Configuration conf = GetConfig();
			CreateEnsembleProvider();
			int sessionTimeout = conf.GetInt(KeyRegistryZkSessionTimeout, DefaultZkSessionTimeout
				);
			int connectionTimeout = conf.GetInt(KeyRegistryZkConnectionTimeout, DefaultZkConnectionTimeout
				);
			int retryTimes = conf.GetInt(KeyRegistryZkRetryTimes, DefaultZkRetryTimes);
			int retryInterval = conf.GetInt(KeyRegistryZkRetryInterval, DefaultZkRetryInterval
				);
			int retryCeiling = conf.GetInt(KeyRegistryZkRetryCeiling, DefaultZkRetryCeiling);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Creating CuratorService with connection {}", connectionDescription);
			}
			CuratorFramework framework;
			lock (typeof(Org.Apache.Hadoop.Registry.Client.Impl.ZK.CuratorService))
			{
				// set the security options
				// build up the curator itself
				CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.Builder();
				builder.EnsembleProvider(ensembleProvider).ConnectionTimeoutMs(connectionTimeout)
					.SessionTimeoutMs(sessionTimeout).RetryPolicy(new BoundedExponentialBackoffRetry
					(retryInterval, retryCeiling, retryTimes));
				// set up the builder AND any JVM context
				registrySecurity.ApplySecurityEnvironment(builder);
				//log them
				securityConnectionDiagnostics = BuildSecurityDiagnostics();
				framework = builder.Build();
				framework.Start();
			}
			return framework;
		}

		public override string ToString()
		{
			return base.ToString() + " " + BindingDiagnosticDetails();
		}

		/// <summary>Get the binding diagnostics</summary>
		/// <returns>a diagnostics string valid after the service is started.</returns>
		public virtual string BindingDiagnosticDetails()
		{
			return " Connection=\"" + connectionDescription + "\"" + " root=\"" + registryRoot
				 + "\"" + " " + securityConnectionDiagnostics;
		}

		/// <summary>Create a full path from the registry root and the supplied subdir</summary>
		/// <param name="path">path of operation</param>
		/// <returns>an absolute path</returns>
		/// <exception cref="System.ArgumentException">if the path is invalide</exception>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string CreateFullPath(string path)
		{
			return RegistryPathUtils.CreateFullPath(registryRoot, path);
		}

		/// <summary>Get the registry binding source ...</summary>
		/// <remarks>
		/// Get the registry binding source ... this can be used to
		/// create new ensemble providers
		/// </remarks>
		/// <returns>the registry binding source in use</returns>
		public virtual RegistryBindingSource GetBindingSource()
		{
			return bindingSource;
		}

		/// <summary>
		/// Create the ensemble provider for this registry, by invoking
		/// <see cref="RegistryBindingSource.SupplyBindingInformation()"/>
		/// on
		/// the provider stored in
		/// <see cref="bindingSource"/>
		/// Sets
		/// <see cref="ensembleProvider"/>
		/// to that value;
		/// sets
		/// <see cref="connectionDescription"/>
		/// to the binding info
		/// for use in toString and logging;
		/// </summary>
		protected internal virtual void CreateEnsembleProvider()
		{
			BindingInformation binding = bindingSource.SupplyBindingInformation();
			connectionDescription = binding.description + " " + securityConnectionDiagnostics;
			ensembleProvider = binding.ensembleProvider;
		}

		/// <summary>Supply the binding information.</summary>
		/// <remarks>
		/// Supply the binding information.
		/// This implementation returns a fixed ensemble bonded to
		/// the quorum supplied by
		/// <see cref="BuildConnectionString()"/>
		/// </remarks>
		/// <returns>the binding information</returns>
		public virtual BindingInformation SupplyBindingInformation()
		{
			BindingInformation binding = new BindingInformation();
			string connectString = BuildConnectionString();
			binding.ensembleProvider = new FixedEnsembleProvider(connectString);
			binding.description = "fixed ZK quorum \"" + connectString + "\"";
			return binding;
		}

		/// <summary>
		/// Override point: get the connection string used to connect to
		/// the ZK service
		/// </summary>
		/// <returns>a registry quorum</returns>
		protected internal virtual string BuildConnectionString()
		{
			return GetConfig().GetTrimmed(KeyRegistryZkQuorum, DefaultRegistryZkQuorum);
		}

		/// <summary>Create an IOE when an operation fails</summary>
		/// <param name="path">path of operation</param>
		/// <param name="operation">operation attempted</param>
		/// <param name="exception">caught the exception caught</param>
		/// <returns>an IOE to throw that contains the path and operation details.</returns>
		protected internal virtual IOException OperationFailure(string path, string operation
			, Exception exception)
		{
			return OperationFailure(path, operation, exception, null);
		}

		/// <summary>Create an IOE when an operation fails</summary>
		/// <param name="path">path of operation</param>
		/// <param name="operation">operation attempted</param>
		/// <param name="exception">caught the exception caught</param>
		/// <returns>an IOE to throw that contains the path and operation details.</returns>
		protected internal virtual IOException OperationFailure(string path, string operation
			, Exception exception, IList<ACL> acls)
		{
			IOException ioe;
			string aclList = "[" + RegistrySecurity.AclsToString(acls) + "]";
			if (exception is KeeperException.NoNodeException)
			{
				ioe = new PathNotFoundException(path);
			}
			else
			{
				if (exception is KeeperException.NodeExistsException)
				{
					ioe = new FileAlreadyExistsException(path);
				}
				else
				{
					if (exception is KeeperException.NoAuthException)
					{
						ioe = new NoPathPermissionsException(path, "Not authorized to access path; ACLs: "
							 + aclList);
					}
					else
					{
						if (exception is KeeperException.NotEmptyException)
						{
							ioe = new PathIsNotEmptyDirectoryException(path);
						}
						else
						{
							if (exception is KeeperException.AuthFailedException)
							{
								ioe = new AuthenticationFailedException(path, "Authentication Failed: " + exception
									 + "; " + securityConnectionDiagnostics, exception);
							}
							else
							{
								if (exception is KeeperException.NoChildrenForEphemeralsException)
								{
									ioe = new NoChildrenForEphemeralsException(path, "Cannot create a path under an ephemeral node: "
										 + exception, exception);
								}
								else
								{
									if (exception is KeeperException.InvalidACLException)
									{
										// this is a security exception of a kind
										// include the ACLs to help the diagnostics
										StringBuilder builder = new StringBuilder();
										builder.Append("Path access failure ").Append(aclList);
										builder.Append(" ");
										builder.Append(securityConnectionDiagnostics);
										ioe = new NoPathPermissionsException(path, builder.ToString());
									}
									else
									{
										ioe = new RegistryIOException(path, "Failure of " + operation + " on " + path + ": "
											 + exception.ToString(), exception);
									}
								}
							}
						}
					}
				}
			}
			if (ioe.InnerException == null)
			{
				Sharpen.Extensions.InitCause(ioe, exception);
			}
			return ioe;
		}

		/// <summary>Create a path if it does not exist.</summary>
		/// <remarks>
		/// Create a path if it does not exist.
		/// The check is poll + create; there's a risk that another process
		/// may create the same path before the create() operation is executed/
		/// propagated to the ZK node polled.
		/// </remarks>
		/// <param name="path">path to create</param>
		/// <param name="acl">ACL for path -used when creating a new entry</param>
		/// <param name="createParents">flag to trigger parent creation</param>
		/// <returns>true iff the path was created</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual bool MaybeCreate(string path, CreateMode mode, IList<ACL> acl, bool
			 createParents)
		{
			return ZkMkPath(path, mode, createParents, acl);
		}

		/// <summary>Stat the file</summary>
		/// <param name="path">path of operation</param>
		/// <returns>a curator stat entry</returns>
		/// <exception cref="System.IO.IOException">on a failure</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">if the path was not found
		/// 	</exception>
		public virtual Stat ZkStat(string path)
		{
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			Stat stat;
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Stat {}", fullpath);
				}
				stat = curator.CheckExists().ForPath(fullpath);
			}
			catch (Exception e)
			{
				throw OperationFailure(fullpath, "read()", e);
			}
			if (stat == null)
			{
				throw new PathNotFoundException(path);
			}
			return stat;
		}

		/// <summary>Get the ACLs of a path</summary>
		/// <param name="path">path of operation</param>
		/// <returns>a possibly empty list of ACLs</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<ACL> ZkGetACLS(string path)
		{
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			IList<ACL> acls;
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("GetACLS {}", fullpath);
				}
				acls = curator.GetACL().ForPath(fullpath);
			}
			catch (Exception e)
			{
				throw OperationFailure(fullpath, "read()", e);
			}
			if (acls == null)
			{
				throw new PathNotFoundException(path);
			}
			return acls;
		}

		/// <summary>Probe for a path existing</summary>
		/// <param name="path">path of operation</param>
		/// <returns>
		/// true if the path was visible from the ZK server
		/// queried.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// on any exception other than
		/// <see cref="Org.Apache.Hadoop.FS.PathNotFoundException"/>
		/// </exception>
		public virtual bool ZkPathExists(string path)
		{
			CheckServiceLive();
			try
			{
				// if zkStat(path) returns without throwing an exception, the return value
				// is guaranteed to be not null
				ZkStat(path);
				return true;
			}
			catch (PathNotFoundException)
			{
				return false;
			}
			catch (IOException e)
			{
				throw;
			}
		}

		/// <summary>Verify a path exists</summary>
		/// <param name="path">path of operation</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">if the path is absent
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual string ZkPathMustExist(string path)
		{
			ZkStat(path);
			return path;
		}

		/// <summary>Create a directory.</summary>
		/// <remarks>Create a directory. It is not an error if it already exists</remarks>
		/// <param name="path">path to create</param>
		/// <param name="mode">mode for path</param>
		/// <param name="createParents">flag to trigger parent creation</param>
		/// <param name="acls">ACL for path</param>
		/// <exception cref="System.IO.IOException">any problem</exception>
		public virtual bool ZkMkPath(string path, CreateMode mode, bool createParents, IList
			<ACL> acls)
		{
			CheckServiceLive();
			path = CreateFullPath(path);
			if (acls == null || acls.IsEmpty())
			{
				throw new NoPathPermissionsException(path, "Empty ACL list");
			}
			try
			{
				RegistrySecurity.AclListInfo aclInfo = new RegistrySecurity.AclListInfo(acls);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Creating path {} with mode {} and ACL {}", path, mode, aclInfo);
				}
				CreateBuilder createBuilder = curator.Create();
				createBuilder.WithMode(mode).WithACL(acls);
				if (createParents)
				{
					createBuilder.CreatingParentsIfNeeded();
				}
				createBuilder.ForPath(path);
			}
			catch (KeeperException.NodeExistsException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("path already present: {}", path, e);
				}
				return false;
			}
			catch (Exception e)
			{
				throw OperationFailure(path, "mkdir() ", e, acls);
			}
			return true;
		}

		/// <summary>Recursively make a path</summary>
		/// <param name="path">path to create</param>
		/// <param name="acl">ACL for path</param>
		/// <exception cref="System.IO.IOException">any problem</exception>
		public virtual void ZkMkParentPath(string path, IList<ACL> acl)
		{
			// split path into elements
			ZkMkPath(RegistryPathUtils.ParentOf(path), CreateMode.Persistent, true, acl);
		}

		/// <summary>Create a path with given data.</summary>
		/// <remarks>
		/// Create a path with given data. byte[0] is used for a path
		/// without data
		/// </remarks>
		/// <param name="path">path of operation</param>
		/// <param name="data">initial data</param>
		/// <param name="acls"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ZkCreate(string path, CreateMode mode, byte[] data, IList<ACL
			> acls)
		{
			Preconditions.CheckArgument(data != null, "null data");
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Creating {} with {} bytes of data and ACL {}", fullpath, data.Length, 
						new RegistrySecurity.AclListInfo(acls));
				}
				curator.Create().WithMode(mode).WithACL(acls).ForPath(fullpath, data);
			}
			catch (Exception e)
			{
				throw OperationFailure(fullpath, "create()", e, acls);
			}
		}

		/// <summary>Update the data for a path</summary>
		/// <param name="path">path of operation</param>
		/// <param name="data">new data</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ZkUpdate(string path, byte[] data)
		{
			Preconditions.CheckArgument(data != null, "null data");
			CheckServiceLive();
			path = CreateFullPath(path);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Updating {} with {} bytes", path, data.Length);
				}
				curator.SetData().ForPath(path, data);
			}
			catch (Exception e)
			{
				throw OperationFailure(path, "update()", e);
			}
		}

		/// <summary>Create or update an entry</summary>
		/// <param name="path">path</param>
		/// <param name="data">data</param>
		/// <param name="acl">ACL for path -used when creating a new entry</param>
		/// <param name="overwrite">enable overwrite</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>true if the entry was created, false if it was simply updated.</returns>
		public virtual bool ZkSet(string path, CreateMode mode, byte[] data, IList<ACL> acl
			, bool overwrite)
		{
			Preconditions.CheckArgument(data != null, "null data");
			CheckServiceLive();
			if (!ZkPathExists(path))
			{
				ZkCreate(path, mode, data, acl);
				return true;
			}
			else
			{
				if (overwrite)
				{
					ZkUpdate(path, data);
					return false;
				}
				else
				{
					throw new FileAlreadyExistsException(path);
				}
			}
		}

		/// <summary>Delete a directory/directory tree.</summary>
		/// <remarks>
		/// Delete a directory/directory tree.
		/// It is not an error to delete a path that does not exist
		/// </remarks>
		/// <param name="path">path of operation</param>
		/// <param name="recursive">flag to trigger recursive deletion</param>
		/// <param name="backgroundCallback">
		/// callback; this being set converts the operation
		/// into an async/background operation.
		/// task
		/// </param>
		/// <exception cref="System.IO.IOException">on problems other than no-such-path</exception>
		public virtual void ZkDelete(string path, bool recursive, BackgroundCallback backgroundCallback
			)
		{
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Deleting {}", fullpath);
				}
				DeleteBuilder delete = curator.Delete();
				if (recursive)
				{
					delete.DeletingChildrenIfNeeded();
				}
				if (backgroundCallback != null)
				{
					delete.InBackground(backgroundCallback);
				}
				delete.ForPath(fullpath);
			}
			catch (KeeperException.NoNodeException)
			{
			}
			catch (Exception e)
			{
				// not an error
				throw OperationFailure(fullpath, "delete()", e);
			}
		}

		/// <summary>List all children of a path</summary>
		/// <param name="path">path of operation</param>
		/// <returns>a possibly empty list of children</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<string> ZkList(string path)
		{
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("ls {}", fullpath);
				}
				GetChildrenBuilder builder = curator.GetChildren();
				IList<string> children = builder.ForPath(fullpath);
				return children;
			}
			catch (Exception e)
			{
				throw OperationFailure(path, "ls()", e);
			}
		}

		/// <summary>Read data on a path</summary>
		/// <param name="path">path of operation</param>
		/// <returns>the data</returns>
		/// <exception cref="System.IO.IOException">read failure</exception>
		public virtual byte[] ZkRead(string path)
		{
			CheckServiceLive();
			string fullpath = CreateFullPath(path);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Reading {}", fullpath);
				}
				return curator.GetData().ForPath(fullpath);
			}
			catch (Exception e)
			{
				throw OperationFailure(fullpath, "read()", e);
			}
		}

		/// <summary>
		/// Return a path dumper instance which can do a full dump
		/// of the registry tree in its <code>toString()</code>
		/// operation
		/// </summary>
		/// <returns>a class to dump the registry</returns>
		/// <param name="verbose">verbose flag - includes more details (such as ACLs)</param>
		public virtual ZKPathDumper DumpPath(bool verbose)
		{
			return new ZKPathDumper(curator, registryRoot, verbose);
		}

		/// <summary>Add a new write access entry for all future write operations.</summary>
		/// <param name="id">ID to use</param>
		/// <param name="pass">password</param>
		/// <exception cref="System.IO.IOException">on any failure to build the digest</exception>
		public virtual bool AddWriteAccessor(string id, string pass)
		{
			RegistrySecurity security = GetRegistrySecurity();
			ACL digestACL = new ACL(ZooDefs.Perms.All, security.ToDigestId(security.Digest(id
				, pass)));
			return security.AddDigestACL(digestACL);
		}

		/// <summary>Clear all write accessors</summary>
		public virtual void ClearWriteAccessors()
		{
			GetRegistrySecurity().ResetDigestACLs();
		}

		/// <summary>Diagnostics method to dump a registry robustly.</summary>
		/// <remarks>
		/// Diagnostics method to dump a registry robustly.
		/// Any exception raised is swallowed
		/// </remarks>
		/// <param name="verbose">verbose path dump</param>
		/// <returns>the registry tree</returns>
		protected internal virtual string DumpRegistryRobustly(bool verbose)
		{
			try
			{
				ZKPathDumper pathDumper = DumpPath(verbose);
				return pathDumper.ToString();
			}
			catch (Exception e)
			{
				// ignore
				Log.Debug("Ignoring exception:  {}", e);
			}
			return string.Empty;
		}
	}
}
