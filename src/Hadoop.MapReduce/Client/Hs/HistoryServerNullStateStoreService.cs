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
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HistoryServerNullStateStoreService : HistoryServerStateStoreService
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override HistoryServerStateStoreService.HistoryServerState LoadState()
		{
			throw new NotSupportedException("Cannot load state from null store");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(MRDelegationTokenIdentifier tokenId)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void StoreTokenMasterKey(DelegationKey key)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void RemoveTokenMasterKey(DelegationKey key)
		{
		}
		// Do nothing
	}
}
