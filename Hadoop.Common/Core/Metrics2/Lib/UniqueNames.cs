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
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Generates predictable and user-friendly unique names</summary>
	public class UniqueNames
	{
		internal class Count
		{
			internal readonly string baseName;

			internal int value;

			internal Count(string name, int value)
			{
				baseName = name;
				this.value = value;
			}
		}

		internal static readonly Joiner joiner = Joiner.On('-');

		internal readonly IDictionary<string, UniqueNames.Count> map = Maps.NewHashMap();

		public virtual string UniqueName(string name)
		{
			lock (this)
			{
				UniqueNames.Count c = map[name];
				if (c == null)
				{
					c = new UniqueNames.Count(name, 0);
					map[name] = c;
					return name;
				}
				if (!c.baseName.Equals(name))
				{
					c = new UniqueNames.Count(name, 0);
				}
				do
				{
					string newName = joiner.Join(name, ++c.value);
					UniqueNames.Count c2 = map[newName];
					if (c2 == null)
					{
						map[newName] = c;
						return newName;
					}
				}
				while (true);
			}
		}
		// handle collisons, assume to be rare cases,
		// eg: people explicitly passed in name-\d+ names.
	}
}
