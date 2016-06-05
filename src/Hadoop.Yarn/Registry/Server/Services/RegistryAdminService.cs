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
using System.Threading;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Binding;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Client.Types;
using Org.Apache.Hadoop.Service;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Server.Services
{
	/// <summary>Administrator service for the registry.</summary>
	/// <remarks>
	/// Administrator service for the registry. This is the one with
	/// permissions to create the base directories and those for users.
	/// It also includes support for asynchronous operations, so that
	/// zookeeper connectivity problems do not hold up the server code
	/// performing the actions.
	/// Any action queued via
	/// <see cref="Submit{V}(Sharpen.Callable{V})"/>
	/// will be
	/// run asynchronously. The
	/// <see cref="CreateDirAsync(string, System.Collections.Generic.IList{E}, bool)"/>
	/// is an example of such an an action
	/// A key async action is the depth-first tree purge, which supports
	/// pluggable policies for deleting entries. The method
	/// <see cref="Purge(string, NodeSelector, PurgePolicy, Org.Apache.Curator.Framework.Api.BackgroundCallback)
	/// 	"/>
	/// implements the recursive purge operation —the class
	/// {{AsyncPurge}} provides the asynchronous scheduling of this.
	/// </remarks>
	public class RegistryAdminService : RegistryOperationsService
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Server.Services.RegistryAdminService
			));

		/// <summary>The ACL permissions for the user's homedir ACL.</summary>
		public const int UserHomedirAclPermissions = ZooDefs.Perms.Read | ZooDefs.Perms.Write
			 | ZooDefs.Perms.Create | ZooDefs.Perms.Delete;

		/// <summary>Executor for async operations</summary>
		protected internal readonly ExecutorService executor;

		/// <summary>Construct an instance of the service</summary>
		/// <param name="name">service name</param>
		public RegistryAdminService(string name)
			: this(name, null)
		{
		}

		/// <summary>
		/// construct an instance of the service, using the
		/// specified binding source to bond to ZK
		/// </summary>
		/// <param name="name">service name</param>
		/// <param name="bindingSource">provider of ZK binding information</param>
		public RegistryAdminService(string name, RegistryBindingSource bindingSource)
			: base(name, bindingSource)
		{
			executor = Executors.NewCachedThreadPool(new _ThreadFactory_113());
		}

		private sealed class _ThreadFactory_113 : ThreadFactory
		{
			public _ThreadFactory_113()
			{
				this.counter = new AtomicInteger(1);
			}

			private AtomicInteger counter;

			public Sharpen.Thread NewThread(Runnable r)
			{
				return new Sharpen.Thread(r, "RegistryAdminService " + this.counter.GetAndIncrement
					());
			}
		}

		/// <summary>Stop the service: halt the executor.</summary>
		/// <exception cref="System.Exception">exception.</exception>
		protected override void ServiceStop()
		{
			StopExecutor();
			base.ServiceStop();
		}

		/// <summary>Stop the executor if it is not null.</summary>
		/// <remarks>
		/// Stop the executor if it is not null.
		/// This uses
		/// <see cref="Sharpen.ExecutorService.ShutdownNow()"/>
		/// and so does not block until they have completed.
		/// </remarks>
		protected internal virtual void StopExecutor()
		{
			lock (this)
			{
				if (executor != null)
				{
					executor.ShutdownNow();
				}
			}
		}

		/// <summary>Get the executor</summary>
		/// <returns>the executor</returns>
		protected internal virtual ExecutorService GetExecutor()
		{
			return executor;
		}

		/// <summary>Submit a callable</summary>
		/// <param name="callable">callable</param>
		/// <?/>
		/// <returns>a future to wait on</returns>
		public virtual Future<V> Submit<V>(Callable<V> callable)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Submitting {}", callable);
			}
			return GetExecutor().Submit(callable);
		}

		/// <summary>Asynchronous operation to create a directory</summary>
		/// <param name="path">path</param>
		/// <param name="acls">ACL list</param>
		/// <param name="createParents">
		/// flag to indicate parent dirs should be created
		/// as needed
		/// </param>
		/// <returns>
		/// the future which will indicate whether or not the operation
		/// succeeded —and propagate any exceptions
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Future<bool> CreateDirAsync(string path, IList<ACL> acls, bool createParents
			)
		{
			return Submit(new _Callable_179(this, path, acls, createParents));
		}

		private sealed class _Callable_179 : Callable<bool>
		{
			public _Callable_179(RegistryAdminService _enclosing, string path, IList<ACL> acls
				, bool createParents)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.acls = acls;
				this.createParents = createParents;
			}

			/// <exception cref="System.Exception"/>
			public bool Call()
			{
				return this._enclosing.MaybeCreate(path, CreateMode.Persistent, acls, createParents
					);
			}

			private readonly RegistryAdminService _enclosing;

			private readonly string path;

			private readonly IList<ACL> acls;

			private readonly bool createParents;
		}

		/// <summary>Init operation sets up the system ACLs.</summary>
		/// <param name="conf">configuration of the service</param>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			RegistrySecurity registrySecurity = GetRegistrySecurity();
			if (registrySecurity.IsSecureRegistry())
			{
				ACL sasl = registrySecurity.CreateSaslACLFromCurrentUser(ZooDefs.Perms.All);
				registrySecurity.AddSystemACL(sasl);
				Log.Info("Registry System ACLs:", RegistrySecurity.AclsToString(registrySecurity.
					GetSystemACLs()));
			}
		}

		/// <summary>Start the service, including creating base directories with permissions</summary>
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			base.ServiceStart();
			// create the root directories
			try
			{
				CreateRootRegistryPaths();
			}
			catch (NoPathPermissionsException e)
			{
				string message = string.Format(Sharpen.Extensions.GetEnglishCulture(), "Failed to create root paths {%s};"
					 + "%ndiagnostics={%s}" + "%ncurrent registry is:" + "%n{%s}", e, BindingDiagnosticDetails
					(), DumpRegistryRobustly(true));
				Log.Error(" Failure {}", e, e);
				Log.Error(message);
				// TODO: this is something temporary to deal with the problem
				// that jenkins is failing this test
				throw new NoPathPermissionsException(e.GetPath().ToString(), message, e);
			}
		}

		/// <summary>Create the initial registry paths</summary>
		/// <exception cref="System.IO.IOException">any failure</exception>
		[VisibleForTesting]
		public virtual void CreateRootRegistryPaths()
		{
			IList<ACL> systemACLs = GetRegistrySecurity().GetSystemACLs();
			Log.Info("System ACLs {}", RegistrySecurity.AclsToString(systemACLs));
			MaybeCreate(string.Empty, CreateMode.Persistent, systemACLs, false);
			MaybeCreate(PathUsers, CreateMode.Persistent, systemACLs, false);
			MaybeCreate(PathSystemServices, CreateMode.Persistent, systemACLs, false);
		}

		/// <summary>Get the path to a user's home dir</summary>
		/// <param name="username">username</param>
		/// <returns>a path for services underneath</returns>
		protected internal virtual string HomeDir(string username)
		{
			return RegistryUtils.HomePathForUser(username);
		}

		/// <summary>Set up the ACL for the user.</summary>
		/// <remarks>
		/// Set up the ACL for the user.
		/// <b>Important: this must run client-side as it needs
		/// to know the id:pass tuple for a user</b>
		/// </remarks>
		/// <param name="username">user name</param>
		/// <param name="perms">permissions</param>
		/// <returns>an ACL list</returns>
		/// <exception cref="System.IO.IOException">ACL creation/parsing problems</exception>
		public virtual IList<ACL> AclsForUser(string username, int perms)
		{
			IList<ACL> clientACLs = GetClientAcls();
			RegistrySecurity security = GetRegistrySecurity();
			if (security.IsSecureRegistry())
			{
				clientACLs.AddItem(security.CreateACLfromUsername(username, perms));
			}
			return clientACLs;
		}

		/// <summary>
		/// Start an async operation to create the home path for a user
		/// if it does not exist
		/// </summary>
		/// <param name="shortname">username, without any @REALM in kerberos</param>
		/// <returns>the path created</returns>
		/// <exception cref="System.IO.IOException">any failure while setting up the operation
		/// 	</exception>
		public virtual Future<bool> InitUserRegistryAsync(string shortname)
		{
			string homeDir = HomeDir(shortname);
			if (!Exists(homeDir))
			{
				// create the directory. The user does not
				return CreateDirAsync(homeDir, AclsForUser(shortname, UserHomedirAclPermissions), 
					false);
			}
			return null;
		}

		/// <summary>Create the home path for a user if it does not exist.</summary>
		/// <remarks>
		/// Create the home path for a user if it does not exist.
		/// This uses
		/// <see cref="InitUserRegistryAsync(string)"/>
		/// and then waits for the
		/// result ... the code path is the same as the async operation; this just
		/// picks up and relays/converts exceptions
		/// </remarks>
		/// <param name="username">username</param>
		/// <returns>the path created</returns>
		/// <exception cref="System.IO.IOException">any failure</exception>
		public virtual string InitUserRegistry(string username)
		{
			try
			{
				Future<bool> future = InitUserRegistryAsync(username);
				future.Get();
			}
			catch (Exception e)
			{
				throw (ThreadInterruptedException)(Sharpen.Extensions.InitCause(new ThreadInterruptedException
					(e.ToString()), e));
			}
			catch (ExecutionException e)
			{
				Exception cause = e.InnerException;
				if (cause is IOException)
				{
					throw (IOException)(cause);
				}
				else
				{
					throw new IOException(cause.ToString(), cause);
				}
			}
			return HomeDir(username);
		}

		/// <summary>Method to validate the validity of the kerberos realm.</summary>
		/// <remarks>
		/// Method to validate the validity of the kerberos realm.
		/// <ul>
		/// <li>Insecure: not needed.</li>
		/// <li>Secure: must have been determined.</li>
		/// </ul>
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Service.ServiceStateException"/>
		protected internal virtual void VerifyRealmValidity()
		{
			if (IsSecure())
			{
				string realm = GetRegistrySecurity().GetKerberosRealm();
				if (StringUtils.IsEmpty(realm))
				{
					throw new ServiceStateException("Cannot determine service realm");
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Started Registry operations in realm {}", realm);
				}
			}
		}

		/// <summary>Policy to purge entries</summary>
		public enum PurgePolicy
		{
			PurgeAll,
			FailOnChildren,
			SkipOnChildren
		}

		/// <summary>Recursive operation to purge all matching records under a base path.</summary>
		/// <remarks>
		/// Recursive operation to purge all matching records under a base path.
		/// <ol>
		/// <li>Uses a depth first search</li>
		/// <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
		/// <li>If a record matches then it is deleted without any child searches</li>
		/// <li>Deletions will be asynchronous if a callback is provided</li>
		/// </ol>
		/// The code is designed to be robust against parallel deletions taking place;
		/// in such a case it will stop attempting that part of the tree. This
		/// avoid the situation of more than 1 purge happening in parallel and
		/// one of the purge operations deleteing the node tree above the other.
		/// </remarks>
		/// <param name="path">base path</param>
		/// <param name="selector">selector for the purge policy</param>
		/// <param name="purgePolicy">what to do if there is a matching record with children</param>
		/// <param name="callback">optional curator callback</param>
		/// <returns>
		/// the number of delete operations perfomed. As deletes may be for
		/// everything under a path, this may be less than the number of records
		/// actually deleted
		/// </returns>
		/// <exception cref="System.IO.IOException">problems</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIsNotEmptyDirectoryException">
		/// if an entry cannot be deleted
		/// as it has children and the purge policy is FailOnChildren
		/// </exception>
		[VisibleForTesting]
		public virtual int Purge(string path, RegistryAdminService.NodeSelector selector, 
			RegistryAdminService.PurgePolicy purgePolicy, BackgroundCallback callback)
		{
			bool toDelete = false;
			// look at self to see if it has a service record
			IDictionary<string, RegistryPathStatus> childEntries;
			ICollection<RegistryPathStatus> entries;
			try
			{
				// list this path's children
				childEntries = RegistryUtils.StatChildren(this, path);
				entries = childEntries.Values;
			}
			catch (PathNotFoundException)
			{
				// there's no record here, it may have been deleted already.
				// exit
				return 0;
			}
			try
			{
				RegistryPathStatus registryPathStatus = Stat(path);
				ServiceRecord serviceRecord = Resolve(path);
				// there is now an entry here.
				toDelete = selector.ShouldSelect(path, registryPathStatus, serviceRecord);
			}
			catch (EOFException)
			{
			}
			catch (InvalidRecordException)
			{
			}
			catch (NoRecordException)
			{
			}
			catch (PathNotFoundException)
			{
				// ignore
				// ignore
				// ignore
				// there's no record here, it may have been deleted already.
				// exit
				return 0;
			}
			if (toDelete && !entries.IsEmpty())
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Match on record @ {} with children ", path);
				}
				switch (purgePolicy)
				{
					case RegistryAdminService.PurgePolicy.SkipOnChildren:
					{
						// there's children
						// don't do the deletion... continue to next record
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Skipping deletion");
						}
						toDelete = false;
						break;
					}

					case RegistryAdminService.PurgePolicy.PurgeAll:
					{
						// mark for deletion
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Scheduling for deletion with children");
						}
						toDelete = true;
						entries = new AList<RegistryPathStatus>(0);
						break;
					}

					case RegistryAdminService.PurgePolicy.FailOnChildren:
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Failing deletion operation");
						}
						throw new PathIsNotEmptyDirectoryException(path);
					}
				}
			}
			int deleteOps = 0;
			if (toDelete)
			{
				try
				{
					ZkDelete(path, true, callback);
				}
				catch (PathNotFoundException)
				{
					// sign that the path was deleted during the operation.
					// this is a no-op, and all children can be skipped
					return deleteOps;
				}
				deleteOps++;
			}
			// now go through the children
			foreach (RegistryPathStatus status in entries)
			{
				string childname = status.path;
				string childpath = RegistryPathUtils.Join(path, childname);
				deleteOps += Purge(childpath, selector, purgePolicy, callback);
			}
			return deleteOps;
		}

		/// <summary>Comparator used for purge logic</summary>
		public interface NodeSelector
		{
			bool ShouldSelect(string path, RegistryPathStatus registryPathStatus, ServiceRecord
				 serviceRecord);
		}

		/// <summary>
		/// An async registry purge action taking
		/// a selector which decides what to delete
		/// </summary>
		public class AsyncPurge : Callable<int>
		{
			private readonly BackgroundCallback callback;

			private readonly RegistryAdminService.NodeSelector selector;

			private readonly string path;

			private readonly RegistryAdminService.PurgePolicy purgePolicy;

			public AsyncPurge(RegistryAdminService _enclosing, string path, RegistryAdminService.NodeSelector
				 selector, RegistryAdminService.PurgePolicy purgePolicy, BackgroundCallback callback
				)
			{
				this._enclosing = _enclosing;
				this.callback = callback;
				this.selector = selector;
				this.path = path;
				this.purgePolicy = purgePolicy;
			}

			/// <exception cref="System.Exception"/>
			public virtual int Call()
			{
				if (RegistryAdminService.Log.IsDebugEnabled())
				{
					RegistryAdminService.Log.Debug("Executing {}", this);
				}
				return this._enclosing.Purge(this.path, this.selector, this.purgePolicy, this.callback
					);
			}

			public override string ToString()
			{
				return string.Format("Record purge under %s with selector %s", this.path, this.selector
					);
			}

			private readonly RegistryAdminService _enclosing;
		}
	}
}
