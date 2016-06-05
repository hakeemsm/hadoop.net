/* Licensed to the Apache Software Foundation (ASF) under one
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
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// This class defines a partial listing of a directory to support
	/// iterative directory listing.
	/// </summary>
	public class DirectoryListing
	{
		private HdfsFileStatus[] partialListing;

		private int remainingEntries;

		/// <summary>constructor</summary>
		/// <param name="partialListing">a partial listing of a directory</param>
		/// <param name="remainingEntries">number of entries that are left to be listed</param>
		public DirectoryListing(HdfsFileStatus[] partialListing, int remainingEntries)
		{
			if (partialListing == null)
			{
				throw new ArgumentException("partial listing should not be null");
			}
			if (partialListing.Length == 0 && remainingEntries != 0)
			{
				throw new ArgumentException("Partial listing is empty but " + "the number of remaining entries is not zero"
					);
			}
			this.partialListing = partialListing;
			this.remainingEntries = remainingEntries;
		}

		/// <summary>Get the partial listing of file status</summary>
		/// <returns>the partial listing of file status</returns>
		public virtual HdfsFileStatus[] GetPartialListing()
		{
			return partialListing;
		}

		/// <summary>Get the number of remaining entries that are left to be listed</summary>
		/// <returns>the number of remaining entries that are left to be listed</returns>
		public virtual int GetRemainingEntries()
		{
			return remainingEntries;
		}

		/// <summary>Check if there are more entries that are left to be listed</summary>
		/// <returns>
		/// true if there are more entries that are left to be listed;
		/// return false otherwise.
		/// </returns>
		public virtual bool HasMore()
		{
			return remainingEntries != 0;
		}

		/// <summary>Get the last name in this list</summary>
		/// <returns>the last name in the list if it is not empty; otherwise return null</returns>
		public virtual byte[] GetLastName()
		{
			if (partialListing.Length == 0)
			{
				return null;
			}
			return partialListing[partialListing.Length - 1].GetLocalNameInBytes();
		}
	}
}
