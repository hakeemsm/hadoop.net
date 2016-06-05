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
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	public class ThreadUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ThreadUtil));

		/// <summary>
		/// Cause the current thread to sleep as close as possible to the provided
		/// number of milliseconds.
		/// </summary>
		/// <remarks>
		/// Cause the current thread to sleep as close as possible to the provided
		/// number of milliseconds. This method will log and ignore any
		/// <see cref="System.Exception"/>
		/// encountered.
		/// </remarks>
		/// <param name="millis">the number of milliseconds for the current thread to sleep</param>
		public static void SleepAtLeastIgnoreInterrupts(long millis)
		{
			long start = Time.Now();
			while (Time.Now() - start < millis)
			{
				long timeToSleep = millis - (Time.Now() - start);
				try
				{
					Thread.Sleep(timeToSleep);
				}
				catch (Exception ie)
				{
					Log.Warn("interrupted while sleeping", ie);
				}
			}
		}
	}
}
