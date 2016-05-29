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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class MemoryRMStateStore : RMStateStore
	{
		internal RMStateStore.RMState state = new RMStateStore.RMState();

		private long epoch = 0L;

		[VisibleForTesting]
		public virtual RMStateStore.RMState GetState()
		{
			return state;
		}

		/// <exception cref="System.Exception"/>
		public override void CheckVersion()
		{
		}

		/// <exception cref="System.Exception"/>
		public override long GetAndIncrementEpoch()
		{
			lock (this)
			{
				long currentEpoch = epoch;
				epoch = epoch + 1;
				return currentEpoch;
			}
		}

		/// <exception cref="System.Exception"/>
		public override RMStateStore.RMState LoadState()
		{
			lock (this)
			{
				// return a copy of the state to allow for modification of the real state
				RMStateStore.RMState returnState = new RMStateStore.RMState();
				returnState.appState.PutAll(state.appState);
				Sharpen.Collections.AddAll(returnState.rmSecretManagerState.GetMasterKeyState(), 
					state.rmSecretManagerState.GetMasterKeyState());
				returnState.rmSecretManagerState.GetTokenState().PutAll(state.rmSecretManagerState
					.GetTokenState());
				returnState.rmSecretManagerState.dtSequenceNumber = state.rmSecretManagerState.dtSequenceNumber;
				returnState.amrmTokenSecretManagerState = state.amrmTokenSecretManagerState == null
					 ? null : AMRMTokenSecretManagerState.NewInstance(state.amrmTokenSecretManagerState
					);
				return returnState;
			}
		}

		protected internal override void InitInternal(Configuration conf)
		{
			lock (this)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StartInternal()
		{
			lock (this)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void CloseInternal()
		{
			lock (this)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appState)
		{
			lock (this)
			{
				state.appState[appId] = appState;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appState)
		{
			lock (this)
			{
				Log.Info("Updating final state " + appState.GetState() + " for app: " + appId);
				if (state.appState[appId] != null)
				{
					// add the earlier attempts back
					appState.attempts.PutAll(state.appState[appId].attempts);
				}
				state.appState[appId] = appState;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptState)
		{
			lock (this)
			{
				ApplicationStateData appState = state.GetApplicationState()[attemptState.GetAttemptId
					().GetApplicationId()];
				if (appState == null)
				{
					throw new YarnRuntimeException("Application doesn't exist");
				}
				appState.attempts[attemptState.GetAttemptId()] = attemptState;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptState)
		{
			lock (this)
			{
				ApplicationStateData appState = state.GetApplicationState()[appAttemptId.GetApplicationId
					()];
				if (appState == null)
				{
					throw new YarnRuntimeException("Application doesn't exist");
				}
				Log.Info("Updating final state " + attemptState.GetState() + " for attempt: " + attemptState
					.GetAttemptId());
				appState.attempts[attemptState.GetAttemptId()] = attemptState;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveApplicationStateInternal(ApplicationStateData
			 appState)
		{
			lock (this)
			{
				ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
					();
				ApplicationStateData removed = Sharpen.Collections.Remove(state.appState, appId);
				if (removed == null)
				{
					throw new YarnRuntimeException("Removing non-exsisting application state");
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void StoreOrUpdateRMDT(RMDelegationTokenIdentifier rmDTIdentifier, long renewDate
			, bool isUpdate)
		{
			IDictionary<RMDelegationTokenIdentifier, long> rmDTState = state.rmSecretManagerState
				.GetTokenState();
			if (rmDTState.Contains(rmDTIdentifier))
			{
				IOException e = new IOException("RMDelegationToken: " + rmDTIdentifier + "is already stored."
					);
				Log.Info("Error storing info for RMDelegationToken: " + rmDTIdentifier, e);
				throw e;
			}
			rmDTState[rmDTIdentifier] = renewDate;
			if (!isUpdate)
			{
				state.rmSecretManagerState.dtSequenceNumber = rmDTIdentifier.GetSequenceNumber();
			}
			Log.Info("Store RMDT with sequence number " + rmDTIdentifier.GetSequenceNumber());
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
			lock (this)
			{
				StoreOrUpdateRMDT(rmDTIdentifier, renewDate, false);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier)
		{
			lock (this)
			{
				IDictionary<RMDelegationTokenIdentifier, long> rmDTState = state.rmSecretManagerState
					.GetTokenState();
				Sharpen.Collections.Remove(rmDTState, rmDTIdentifier);
				Log.Info("Remove RMDT with sequence number " + rmDTIdentifier.GetSequenceNumber()
					);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
			lock (this)
			{
				RemoveRMDelegationTokenState(rmDTIdentifier);
				StoreOrUpdateRMDT(rmDTIdentifier, renewDate, true);
				Log.Info("Update RMDT with sequence number " + rmDTIdentifier.GetSequenceNumber()
					);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
			lock (this)
			{
				ICollection<DelegationKey> rmDTMasterKeyState = state.rmSecretManagerState.GetMasterKeyState
					();
				if (rmDTMasterKeyState.Contains(delegationKey))
				{
					IOException e = new IOException("RMDTMasterKey with keyID: " + delegationKey.GetKeyId
						() + " is already stored");
					Log.Info("Error storing info for RMDTMasterKey with keyID: " + delegationKey.GetKeyId
						(), e);
					throw e;
				}
				state.GetRMDTSecretManagerState().GetMasterKeyState().AddItem(delegationKey);
				Log.Info("Store RMDT master key with key id: " + delegationKey.GetKeyId() + ". Currently rmDTMasterKeyState size: "
					 + rmDTMasterKeyState.Count);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
			lock (this)
			{
				Log.Info("Remove RMDT master key with key id: " + delegationKey.GetKeyId());
				ICollection<DelegationKey> rmDTMasterKeyState = state.rmSecretManagerState.GetMasterKeyState
					();
				rmDTMasterKeyState.Remove(delegationKey);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override Version LoadVersion()
		{
			return null;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreVersion()
		{
		}

		protected internal override Version GetCurrentVersion()
		{
			return null;
		}

		protected internal override void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 amrmTokenSecretManagerState, bool isUpdate)
		{
			lock (this)
			{
				if (amrmTokenSecretManagerState != null)
				{
					state.amrmTokenSecretManagerState = AMRMTokenSecretManagerState.NewInstance(amrmTokenSecretManagerState
						);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public override void DeleteStore()
		{
		}
	}
}
