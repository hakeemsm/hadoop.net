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
using System.Globalization;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>Helper class to handle resource bundles in a saner way</summary>
	public class ResourceBundles
	{
		/// <summary>Get a resource bundle</summary>
		/// <param name="bundleName">of the resource</param>
		/// <returns>the resource bundle</returns>
		/// <exception cref="Sharpen.MissingResourceException"/>
		public static ResourceBundle GetBundle(string bundleName)
		{
			return ResourceBundle.GetBundle(bundleName.Replace('$', '_'), CultureInfo.CurrentCulture
				, Sharpen.Thread.CurrentThread().GetContextClassLoader());
		}

		/// <summary>Get a resource given bundle name and key</summary>
		/// <?/>
		/// <param name="bundleName">name of the resource bundle</param>
		/// <param name="key">to lookup the resource</param>
		/// <param name="suffix">for the key to lookup</param>
		/// <param name="defaultValue">of the resource</param>
		/// <returns>the resource or the defaultValue</returns>
		/// <exception cref="System.InvalidCastException">if the resource found doesn't match T
		/// 	</exception>
		public static T GetValue<T>(string bundleName, string key, string suffix, T defaultValue
			)
		{
			lock (typeof(ResourceBundles))
			{
				T value;
				try
				{
					ResourceBundle bundle = GetBundle(bundleName);
					value = (T)bundle.GetObject(GetLookupKey(key, suffix));
				}
				catch (Exception)
				{
					return defaultValue;
				}
				return value;
			}
		}

		private static string GetLookupKey(string key, string suffix)
		{
			if (suffix == null || suffix.IsEmpty())
			{
				return key;
			}
			return key + suffix;
		}

		/// <summary>Get the counter group display name</summary>
		/// <param name="group">the group name to lookup</param>
		/// <param name="defaultValue">of the group</param>
		/// <returns>the group display name</returns>
		public static string GetCounterGroupName(string group, string defaultValue)
		{
			return GetValue(group, "CounterGroupName", string.Empty, defaultValue);
		}

		/// <summary>Get the counter display name</summary>
		/// <param name="group">the counter group name for the counter</param>
		/// <param name="counter">the counter name to lookup</param>
		/// <param name="defaultValue">of the counter</param>
		/// <returns>the counter display name</returns>
		public static string GetCounterName(string group, string counter, string defaultValue
			)
		{
			return GetValue(group, counter, ".name", defaultValue);
		}
	}
}
