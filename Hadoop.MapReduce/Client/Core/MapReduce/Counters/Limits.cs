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
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	public class Limits
	{
		private int totalCounters;

		private LimitExceededException firstViolation;

		private static bool isInited;

		private static int GroupNameMax;

		private static int CounterNameMax;

		private static int GroupsMax;

		private static int CountersMax;

		public static void Init(Configuration conf)
		{
			lock (typeof(Limits))
			{
				if (!isInited)
				{
					if (conf == null)
					{
						conf = new JobConf();
					}
					GroupNameMax = conf.GetInt(CounterGroupNameMaxKey, CounterGroupNameMaxDefault);
					CounterNameMax = conf.GetInt(CounterNameMaxKey, CounterNameMaxDefault);
					GroupsMax = conf.GetInt(CounterGroupsMaxKey, CounterGroupsMaxDefault);
					CountersMax = conf.GetInt(CountersMaxKey, CountersMaxDefault);
				}
				isInited = true;
			}
		}

		public static int GetGroupNameMax()
		{
			if (!isInited)
			{
				Init(null);
			}
			return GroupNameMax;
		}

		public static int GetCounterNameMax()
		{
			if (!isInited)
			{
				Init(null);
			}
			return CounterNameMax;
		}

		public static int GetGroupsMax()
		{
			if (!isInited)
			{
				Init(null);
			}
			return GroupsMax;
		}

		public static int GetCountersMax()
		{
			if (!isInited)
			{
				Init(null);
			}
			return CountersMax;
		}

		public static string FilterName(string name, int maxLen)
		{
			return name.Length > maxLen ? Sharpen.Runtime.Substring(name, 0, maxLen - 1) : name;
		}

		public static string FilterCounterName(string name)
		{
			return FilterName(name, GetCounterNameMax());
		}

		public static string FilterGroupName(string name)
		{
			return FilterName(name, GetGroupNameMax());
		}

		public virtual void CheckCounters(int size)
		{
			lock (this)
			{
				if (firstViolation != null)
				{
					throw new LimitExceededException(firstViolation);
				}
				int countersMax = GetCountersMax();
				if (size > countersMax)
				{
					firstViolation = new LimitExceededException("Too many counters: " + size + " max="
						 + countersMax);
					throw firstViolation;
				}
			}
		}

		public virtual void IncrCounters()
		{
			lock (this)
			{
				CheckCounters(totalCounters + 1);
				++totalCounters;
			}
		}

		public virtual void CheckGroups(int size)
		{
			lock (this)
			{
				if (firstViolation != null)
				{
					throw new LimitExceededException(firstViolation);
				}
				int groupsMax = GetGroupsMax();
				if (size > groupsMax)
				{
					firstViolation = new LimitExceededException("Too many counter groups: " + size + 
						" max=" + groupsMax);
				}
			}
		}

		public virtual LimitExceededException Violation()
		{
			lock (this)
			{
				return firstViolation;
			}
		}
	}
}
