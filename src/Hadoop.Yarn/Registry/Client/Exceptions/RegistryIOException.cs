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
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Exceptions
{
	/// <summary>Base exception for registry operations.</summary>
	/// <remarks>
	/// Base exception for registry operations.
	/// <p>
	/// These exceptions include the path of the failing operation wherever possible;
	/// this can be retrieved via
	/// <see cref="Org.Apache.Hadoop.FS.PathIOException.GetPath()"/>
	/// .
	/// </remarks>
	[System.Serializable]
	public class RegistryIOException : PathIOException
	{
		/// <summary>Build an exception from any other Path IO Exception.</summary>
		/// <remarks>
		/// Build an exception from any other Path IO Exception.
		/// This propagates the path of the original exception
		/// </remarks>
		/// <param name="message">more specific text</param>
		/// <param name="cause">cause</param>
		public RegistryIOException(string message, PathIOException cause)
			: base(cause.GetPath() != null ? cause.GetPath().ToString() : string.Empty, message
				, cause)
		{
		}

		public RegistryIOException(string path, Exception cause)
			: base(path, cause)
		{
		}

		public RegistryIOException(string path, string error)
			: base(path, error)
		{
		}

		public RegistryIOException(string path, string error, Exception cause)
			: base(path, error, cause)
		{
		}
	}
}
