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
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Exceptions
{
	/// <summary>
	/// Raised if there is no
	/// <see cref="Org.Apache.Hadoop.Registry.Client.Types.ServiceRecord"/>
	/// resolved at the end
	/// of the specified path.
	/// <p>
	/// There may be valid data of some form at the end of the path, but it does
	/// not appear to be a Service Record.
	/// </summary>
	[System.Serializable]
	public class NoRecordException : RegistryIOException
	{
		public NoRecordException(string path, string error)
			: base(path, error)
		{
		}

		public NoRecordException(string path, string error, Exception cause)
			: base(path, error, cause)
		{
		}
	}
}
