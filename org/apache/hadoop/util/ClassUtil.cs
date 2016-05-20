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

namespace org.apache.hadoop.util
{
	public class ClassUtil
	{
		/// <summary>Find a jar that contains a class of the same name, if any.</summary>
		/// <remarks>
		/// Find a jar that contains a class of the same name, if any.
		/// It will return a jar file, even if that is not the first thing
		/// on the class path that has a class with the same name.
		/// </remarks>
		/// <param name="clazz">the class to find.</param>
		/// <returns>a jar file that contains the class, or null.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string findContainingJar(java.lang.Class clazz)
		{
			java.lang.ClassLoader loader = clazz.getClassLoader();
			string classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
			try
			{
				for (java.util.Enumeration<java.net.URL> itr = loader.getResources(classFile); itr
					.MoveNext(); )
				{
					java.net.URL url = itr.Current;
					if ("jar".Equals(url.getProtocol()))
					{
						string toReturn = url.getPath();
						if (toReturn.StartsWith("file:"))
						{
							toReturn = Sharpen.Runtime.substring(toReturn, "file:".Length);
						}
						toReturn = java.net.URLDecoder.decode(toReturn, "UTF-8");
						return toReturn.replaceAll("!.*$", string.Empty);
					}
				}
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
			return null;
		}
	}
}
