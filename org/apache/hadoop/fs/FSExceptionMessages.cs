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
using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Standard strings to use in exception messages in filesystems
	/// HDFS is used as the reference source of the strings
	/// </summary>
	public class FSExceptionMessages
	{
		/// <summary>
		/// The operation failed because the stream is closed:
		/// <value/>
		/// </summary>
		public const string STREAM_IS_CLOSED = "Stream is closed!";

		/// <summary>
		/// Negative offset seek forbidden :
		/// <value/>
		/// </summary>
		public const string NEGATIVE_SEEK = "Cannot seek to a negative offset";

		/// <summary>
		/// Seeks :
		/// <value/>
		/// </summary>
		public const string CANNOT_SEEK_PAST_EOF = "Attempted to seek or read past the end of the file";
	}
}
