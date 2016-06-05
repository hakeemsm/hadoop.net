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
using Com.Google.Common.Annotations;
using Org.Apache.Curator.Framework.Api;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Hadoop.Registry.Client.Types.Yarn;
using Org.Apache.Hadoop.Registry.Server.Services;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Server.Integration
{
	/// <summary>
	/// Handle RM events by updating the registry
	/// <p>
	/// These actions are all implemented as event handlers to operations
	/// which come from the RM.
	/// </summary>
	/// <remarks>
	/// Handle RM events by updating the registry
	/// <p>
	/// These actions are all implemented as event handlers to operations
	/// which come from the RM.
	/// <p>
	/// This service is expected to be executed by a user with the permissions
	/// to manipulate the entire registry,
	/// </remarks>
	public class RMRegistryOperationsService : RegistryAdminService
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Registry.Server.Integration.RMRegistryOperationsService
			));

		private RegistryAdminService.PurgePolicy purgeOnCompletionPolicy = RegistryAdminService.PurgePolicy
			.PurgeAll;

		public RMRegistryOperationsService(string name)
			: this(name, null)
		{
		}

		public RMRegistryOperationsService(string name, RegistryBindingSource bindingSource
			)
			: base(name, bindingSource)
		{
		}

		/// <summary>
		/// Extend the parent service initialization by verifying that the
		/// service knows —in a secure cluster— the realm in which it is executing.
		/// </summary>
		/// <remarks>
		/// Extend the parent service initialization by verifying that the
		/// service knows —in a secure cluster— the realm in which it is executing.
		/// It needs this to properly build up the user names and hence their
		/// access rights.
		/// </remarks>
		/// <param name="conf">configuration of the service</param>
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			VerifyRealmValidity();
		}

		public virtual RegistryAdminService.PurgePolicy GetPurgeOnCompletionPolicy()
		{
			return purgeOnCompletionPolicy;
		}

		public virtual void SetPurgeOnCompletionPolicy(RegistryAdminService.PurgePolicy purgeOnCompletionPolicy
			)
		{
			this.purgeOnCompletionPolicy = purgeOnCompletionPolicy;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void OnApplicationAttemptRegistered(ApplicationAttemptId attemptId
			, string host, int rpcport, string trackingurl)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void OnApplicationLaunched(ApplicationId id)
		{
		}

		/// <summary>Actions to take as an AM registers itself with the RM.</summary>
		/// <param name="attemptId">attempt ID</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnApplicationMasterRegistered(ApplicationAttemptId attemptId)
		{
		}

		/// <summary>Actions to take when the AM container is completed</summary>
		/// <param name="containerId">container ID</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnAMContainerFinished(ContainerId containerId)
		{
			Log.Info("AM Container {} finished, purging application attempt records", containerId
				);
			// remove all application attempt entries
			PurgeAppAttemptRecords(containerId.GetApplicationAttemptId());
			// also treat as a container finish to remove container
			// level records for the AM container
			OnContainerFinished(containerId);
		}

		/// <summary>remove all application attempt entries</summary>
		/// <param name="attemptId">attempt ID</param>
		protected internal virtual void PurgeAppAttemptRecords(ApplicationAttemptId attemptId
			)
		{
			PurgeRecordsAsync("/", attemptId.ToString(), PersistencePolicies.ApplicationAttempt
				);
		}

		/// <summary>Actions to take when an application attempt is completed</summary>
		/// <param name="attemptId">application  ID</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnApplicationAttemptUnregistered(ApplicationAttemptId attemptId
			)
		{
			Log.Info("Application attempt {} unregistered, purging app attempt records", attemptId
				);
			PurgeAppAttemptRecords(attemptId);
		}

		/// <summary>Actions to take when an application is completed</summary>
		/// <param name="id">application  ID</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnApplicationCompleted(ApplicationId id)
		{
			Log.Info("Application {} completed, purging application-level records", id);
			PurgeRecordsAsync("/", id.ToString(), PersistencePolicies.Application);
		}

		public virtual void OnApplicationAttemptAdded(ApplicationAttemptId appAttemptId)
		{
		}

		/// <summary>
		/// This is the event where the user is known, so the user directory
		/// can be created
		/// </summary>
		/// <param name="applicationId">application  ID</param>
		/// <param name="user">username</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnStateStoreEvent(ApplicationId applicationId, string user)
		{
			InitUserRegistryAsync(user);
		}

		/// <summary>Actions to take when the AM container is completed</summary>
		/// <param name="id">container ID</param>
		/// <exception cref="System.IO.IOException">problems</exception>
		public virtual void OnContainerFinished(ContainerId id)
		{
			Log.Info("Container {} finished, purging container-level records", id);
			PurgeRecordsAsync("/", id.ToString(), PersistencePolicies.Container);
		}

		/// <summary>Queue an async operation to purge all matching records under a base path.
		/// 	</summary>
		/// <remarks>
		/// Queue an async operation to purge all matching records under a base path.
		/// <ol>
		/// <li>Uses a depth first search</li>
		/// <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
		/// <li>If a record matches then it is deleted without any child searches</li>
		/// <li>Deletions will be asynchronous if a callback is provided</li>
		/// </ol>
		/// </remarks>
		/// <param name="path">base path</param>
		/// <param name="id">ID for service record.id</param>
		/// <param name="persistencePolicyMatch">
		/// ID for the persistence policy to match:
		/// no match, no delete.
		/// </param>
		/// <returns>a future that returns the #of records deleted</returns>
		[VisibleForTesting]
		public virtual Future<int> PurgeRecordsAsync(string path, string id, string persistencePolicyMatch
			)
		{
			return PurgeRecordsAsync(path, id, persistencePolicyMatch, purgeOnCompletionPolicy
				, new DeleteCompletionCallback());
		}

		/// <summary>Queue an async operation to purge all matching records under a base path.
		/// 	</summary>
		/// <remarks>
		/// Queue an async operation to purge all matching records under a base path.
		/// <ol>
		/// <li>Uses a depth first search</li>
		/// <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
		/// <li>If a record matches then it is deleted without any child searches</li>
		/// <li>Deletions will be asynchronous if a callback is provided</li>
		/// </ol>
		/// </remarks>
		/// <param name="path">base path</param>
		/// <param name="id">ID for service record.id</param>
		/// <param name="persistencePolicyMatch">
		/// ID for the persistence policy to match:
		/// no match, no delete.
		/// </param>
		/// <param name="purgePolicy">how to react to children under the entry</param>
		/// <param name="callback">an optional callback</param>
		/// <returns>a future that returns the #of records deleted</returns>
		[VisibleForTesting]
		public virtual Future<int> PurgeRecordsAsync(string path, string id, string persistencePolicyMatch
			, RegistryAdminService.PurgePolicy purgePolicy, BackgroundCallback callback)
		{
			Log.Info(" records under {} with ID {} and policy {}: {}", path, id, persistencePolicyMatch
				);
			return Submit(new RegistryAdminService.AsyncPurge(this, path, new SelectByYarnPersistence
				(id, persistencePolicyMatch), purgePolicy, callback));
		}
	}
}
