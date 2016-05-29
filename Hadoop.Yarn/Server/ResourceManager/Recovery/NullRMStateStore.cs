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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class NullRMStateStore : RMStateStore
	{
		/// <exception cref="System.Exception"/>
		protected internal override void InitInternal(Configuration conf)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void StartInternal()
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void CloseInternal()
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		public override long GetAndIncrementEpoch()
		{
			lock (this)
			{
				return 0L;
			}
		}

		/// <exception cref="System.Exception"/>
		public override RMStateStore.RMState LoadState()
		{
			throw new NotSupportedException("Cannot load state from null store");
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void RemoveApplicationStateInternal(ApplicationStateData
			 appState)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData)
		{
		}

		// Do nothing 
		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData)
		{
		}

		/// <exception cref="System.Exception"/>
		public override void CheckVersion()
		{
		}

		// Do nothing
		/// <exception cref="System.Exception"/>
		protected internal override Version LoadVersion()
		{
			// Do nothing
			return null;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreVersion()
		{
		}

		// Do nothing
		protected internal override Version GetCurrentVersion()
		{
			// Do nothing
			return null;
		}

		protected internal override void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 state, bool isUpdate)
		{
		}

		//DO Nothing
		/// <exception cref="System.Exception"/>
		public override void DeleteStore()
		{
		}
		// Do nothing
	}
}
