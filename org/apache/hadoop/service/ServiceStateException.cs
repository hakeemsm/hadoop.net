/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.service
{
	/// <summary>Exception that is raised on state change operations.</summary>
	[System.Serializable]
	public class ServiceStateException : System.Exception
	{
		private const long serialVersionUID = 1110000352259232646L;

		public ServiceStateException(string message)
			: base(message)
		{
		}

		public ServiceStateException(string message, System.Exception cause)
			: base(message, cause)
		{
		}

		public ServiceStateException(System.Exception cause)
			: base(cause)
		{
		}

		/// <summary>
		/// Convert any exception into a
		/// <see cref="System.Exception"/>
		/// .
		/// If the caught exception is already of that type, it is typecast to a
		/// <see cref="System.Exception"/>
		/// and returned.
		/// All other exception types are wrapped in a new instance of
		/// ServiceStateException
		/// </summary>
		/// <param name="fault">exception or throwable</param>
		/// <returns>a ServiceStateException to rethrow</returns>
		public static System.Exception convert(System.Exception fault)
		{
			if (fault is System.Exception)
			{
				return (System.Exception)fault;
			}
			else
			{
				return new org.apache.hadoop.service.ServiceStateException(fault);
			}
		}

		/// <summary>
		/// Convert any exception into a
		/// <see cref="System.Exception"/>
		/// .
		/// If the caught exception is already of that type, it is typecast to a
		/// <see cref="System.Exception"/>
		/// and returned.
		/// All other exception types are wrapped in a new instance of
		/// ServiceStateException
		/// </summary>
		/// <param name="text">text to use if a new exception is created</param>
		/// <param name="fault">exception or throwable</param>
		/// <returns>a ServiceStateException to rethrow</returns>
		public static System.Exception convert(string text, System.Exception fault)
		{
			if (fault is System.Exception)
			{
				return (System.Exception)fault;
			}
			else
			{
				return new org.apache.hadoop.service.ServiceStateException(text, fault);
			}
		}
	}
}
