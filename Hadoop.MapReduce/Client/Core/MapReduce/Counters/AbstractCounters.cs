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
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract class to provide common implementation for the Counters
	/// container in both mapred and mapreduce packages.
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class AbstractCounters<C, G> : Writable, IEnumerable<G>
		where C : Counter
		where G : CounterGroupBase<C>
	{
		protected internal static readonly Log Log = LogFactory.GetLog("mapreduce.Counters"
			);

		/// <summary>A cache from enum values to the associated counter.</summary>
		private IDictionary<Enum<object>, C> cache = Maps.NewIdentityHashMap();

		private IDictionary<string, G> fgroups = new ConcurrentSkipListMap<string, G>();

		private IDictionary<string, G> groups = new ConcurrentSkipListMap<string, G>();

		private readonly CounterGroupFactory<C, G> groupFactory;

		internal enum GroupType
		{
			Framework,
			Filesystem
		}

		private bool writeAllCounters = true;

		private static readonly IDictionary<string, string> legacyMap = Maps.NewHashMap();

		static AbstractCounters()
		{
			//framework & fs groups
			// other groups
			// For framework counter serialization without strings
			// Writes only framework and fs counters if false.
			legacyMap["org.apache.hadoop.mapred.Task$Counter"] = typeof(TaskCounter).FullName;
			legacyMap["org.apache.hadoop.mapred.JobInProgress$Counter"] = typeof(JobCounter).
				FullName;
			legacyMap["FileSystemCounters"] = typeof(FileSystemCounter).FullName;
		}

		private readonly Org.Apache.Hadoop.Mapreduce.Counters.Limits limits = new Org.Apache.Hadoop.Mapreduce.Counters.Limits
			();

		[InterfaceAudience.Private]
		public AbstractCounters(CounterGroupFactory<C, G> gf)
		{
			groupFactory = gf;
		}

		/// <summary>Construct from another counters object.</summary>
		/// <?/>
		/// <?/>
		/// <param name="counters">the counters object to copy</param>
		/// <param name="groupFactory">the factory for new groups</param>
		[InterfaceAudience.Private]
		public AbstractCounters(Org.Apache.Hadoop.Mapreduce.Counters.AbstractCounters<C1, 
			G1> counters, CounterGroupFactory<C, G> groupFactory)
		{
			this.groupFactory = groupFactory;
			foreach (G1 group in counters)
			{
				string name = group.GetName();
				G newGroup = groupFactory.NewGroup(name, group.GetDisplayName(), limits);
				(CounterGroupFactory.IsFrameworkGroup(name) ? fgroups : groups)[name] = newGroup;
				foreach (Counter counter in group)
				{
					newGroup.AddCounter(counter.GetName(), counter.GetDisplayName(), counter.GetValue
						());
				}
			}
		}

		/// <summary>Add a group.</summary>
		/// <param name="group">object to add</param>
		/// <returns>the group</returns>
		[InterfaceAudience.Private]
		public virtual G AddGroup(G group)
		{
			lock (this)
			{
				string name = group.GetName();
				if (CounterGroupFactory.IsFrameworkGroup(name))
				{
					fgroups[name] = group;
				}
				else
				{
					limits.CheckGroups(groups.Count + 1);
					groups[name] = group;
				}
				return group;
			}
		}

		/// <summary>Add a new group</summary>
		/// <param name="name">of the group</param>
		/// <param name="displayName">of the group</param>
		/// <returns>the group</returns>
		[InterfaceAudience.Private]
		public virtual G AddGroup(string name, string displayName)
		{
			return AddGroup(groupFactory.NewGroup(name, displayName, limits));
		}

		/// <summary>Find a counter, create one if necessary</summary>
		/// <param name="groupName">of the counter</param>
		/// <param name="counterName">name of the counter</param>
		/// <returns>the matching counter</returns>
		public virtual C FindCounter(string groupName, string counterName)
		{
			G grp = GetGroup(groupName);
			return grp.FindCounter(counterName);
		}

		/// <summary>Find the counter for the given enum.</summary>
		/// <remarks>
		/// Find the counter for the given enum. The same enum will always return the
		/// same counter.
		/// </remarks>
		/// <param name="key">the counter key</param>
		/// <returns>the matching counter object</returns>
		public virtual C FindCounter<_T0>(Enum<_T0> key)
			where _T0 : Enum<E>
		{
			lock (this)
			{
				C counter = cache[key];
				if (counter == null)
				{
					counter = FindCounter(key.GetDeclaringClass().FullName, key.Name());
					cache[key] = counter;
				}
				return counter;
			}
		}

		/// <summary>Find the file system counter for the given scheme and enum.</summary>
		/// <param name="scheme">of the file system</param>
		/// <param name="key">the enum of the counter</param>
		/// <returns>the file system counter</returns>
		[InterfaceAudience.Private]
		public virtual C FindCounter(string scheme, FileSystemCounter key)
		{
			lock (this)
			{
				return ((FileSystemCounterGroup<C>)GetGroup(typeof(FileSystemCounter).FullName).GetUnderlyingGroup
					()).FindCounter(scheme, key);
			}
		}

		/// <summary>Returns the names of all counter classes.</summary>
		/// <returns>Set of counter names.</returns>
		public virtual IEnumerable<string> GetGroupNames()
		{
			lock (this)
			{
				HashSet<string> deprecated = new HashSet<string>();
				foreach (KeyValuePair<string, string> entry in legacyMap)
				{
					string newGroup = entry.Value;
					bool isFGroup = CounterGroupFactory.IsFrameworkGroup(newGroup);
					if (isFGroup ? fgroups.Contains(newGroup) : groups.Contains(newGroup))
					{
						deprecated.AddItem(entry.Key);
					}
				}
				return Iterables.Concat(fgroups.Keys, groups.Keys, deprecated);
			}
		}

		public virtual IEnumerator<G> GetEnumerator()
		{
			return Iterators.Concat(fgroups.Values.GetEnumerator(), groups.Values.GetEnumerator
				());
		}

		/// <summary>
		/// Returns the named counter group, or an empty group if there is none
		/// with the specified name.
		/// </summary>
		/// <param name="groupName">name of the group</param>
		/// <returns>the group</returns>
		public virtual G GetGroup(string groupName)
		{
			lock (this)
			{
				// filterGroupName
				bool groupNameInLegacyMap = true;
				string newGroupName = legacyMap[groupName];
				if (newGroupName == null)
				{
					groupNameInLegacyMap = false;
					newGroupName = Org.Apache.Hadoop.Mapreduce.Counters.Limits.FilterGroupName(groupName
						);
				}
				bool isFGroup = CounterGroupFactory.IsFrameworkGroup(newGroupName);
				G group = isFGroup ? fgroups[newGroupName] : groups[newGroupName];
				if (group == null)
				{
					group = groupFactory.NewGroup(newGroupName, limits);
					if (isFGroup)
					{
						fgroups[newGroupName] = group;
					}
					else
					{
						limits.CheckGroups(groups.Count + 1);
						groups[newGroupName] = group;
					}
					if (groupNameInLegacyMap)
					{
						Log.Warn("Group " + groupName + " is deprecated. Use " + newGroupName + " instead"
							);
					}
				}
				return group;
			}
		}

		/// <summary>
		/// Returns the total number of counters, by summing the number of counters
		/// in each group.
		/// </summary>
		/// <returns>the total number of counters</returns>
		public virtual int CountCounters()
		{
			lock (this)
			{
				int result = 0;
				foreach (G group in this)
				{
					result += group.Size();
				}
				return result;
			}
		}

		/// <summary>Write the set of groups.</summary>
		/// <remarks>
		/// Write the set of groups.
		/// Counters ::= version #fgroups (groupId, group)* #groups (group)
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			lock (this)
			{
				WritableUtils.WriteVInt(@out, groupFactory.Version());
				WritableUtils.WriteVInt(@out, fgroups.Count);
				// framework groups first
				foreach (G group in fgroups.Values)
				{
					if (group.GetUnderlyingGroup() is FrameworkCounterGroup<object, object>)
					{
						WritableUtils.WriteVInt(@out, (int)(AbstractCounters.GroupType.Framework));
						WritableUtils.WriteVInt(@out, CounterGroupFactory.GetFrameworkGroupId(group.GetName
							()));
						group.Write(@out);
					}
					else
					{
						if (group.GetUnderlyingGroup() is FileSystemCounterGroup<object>)
						{
							WritableUtils.WriteVInt(@out, (int)(AbstractCounters.GroupType.Filesystem));
							group.Write(@out);
						}
					}
				}
				if (writeAllCounters)
				{
					WritableUtils.WriteVInt(@out, groups.Count);
					foreach (G group_1 in groups.Values)
					{
						Text.WriteString(@out, group_1.GetName());
						group_1.Write(@out);
					}
				}
				else
				{
					WritableUtils.WriteVInt(@out, 0);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			lock (this)
			{
				int version = WritableUtils.ReadVInt(@in);
				if (version != groupFactory.Version())
				{
					throw new IOException("Counters version mismatch, expected " + groupFactory.Version
						() + " got " + version);
				}
				int numFGroups = WritableUtils.ReadVInt(@in);
				fgroups.Clear();
				AbstractCounters.GroupType[] groupTypes = AbstractCounters.GroupType.Values();
				while (numFGroups-- > 0)
				{
					AbstractCounters.GroupType groupType = groupTypes[WritableUtils.ReadVInt(@in)];
					G group;
					switch (groupType)
					{
						case AbstractCounters.GroupType.Filesystem:
						{
							// with nothing
							group = groupFactory.NewFileSystemGroup();
							break;
						}

						case AbstractCounters.GroupType.Framework:
						{
							// with group id
							group = groupFactory.NewFrameworkGroup(WritableUtils.ReadVInt(@in));
							break;
						}

						default:
						{
							// Silence dumb compiler, as it would've thrown earlier
							throw new IOException("Unexpected counter group type: " + groupType);
						}
					}
					group.ReadFields(@in);
					fgroups[group.GetName()] = group;
				}
				int numGroups = WritableUtils.ReadVInt(@in);
				while (numGroups-- > 0)
				{
					limits.CheckGroups(groups.Count + 1);
					G group = groupFactory.NewGenericGroup(StringInterner.WeakIntern(Text.ReadString(
						@in)), null, limits);
					group.ReadFields(@in);
					groups[group.GetName()] = group;
				}
			}
		}

		/// <summary>Return textual representation of the counter values.</summary>
		/// <returns>the string</returns>
		public override string ToString()
		{
			lock (this)
			{
				StringBuilder sb = new StringBuilder("Counters: " + CountCounters());
				foreach (G group in this)
				{
					sb.Append("\n\t").Append(group.GetDisplayName());
					foreach (Counter counter in group)
					{
						sb.Append("\n\t\t").Append(counter.GetDisplayName()).Append("=").Append(counter.GetValue
							());
					}
				}
				return sb.ToString();
			}
		}

		/// <summary>
		/// Increments multiple counters by their amounts in another Counters
		/// instance.
		/// </summary>
		/// <param name="other">the other Counters instance</param>
		public virtual void IncrAllCounters(Org.Apache.Hadoop.Mapreduce.Counters.AbstractCounters
			<C, G> other)
		{
			lock (this)
			{
				foreach (G right in other)
				{
					string groupName = right.GetName();
					G left = (CounterGroupFactory.IsFrameworkGroup(groupName) ? fgroups : groups)[groupName
						];
					if (left == null)
					{
						left = AddGroup(groupName, right.GetDisplayName());
					}
					left.IncrAllCounters(right);
				}
			}
		}

		public override bool Equals(object genericRight)
		{
			if (genericRight is Org.Apache.Hadoop.Mapreduce.Counters.AbstractCounters<object, 
				object>)
			{
				return Iterators.ElementsEqual(GetEnumerator(), ((Org.Apache.Hadoop.Mapreduce.Counters.AbstractCounters
					<C, G>)genericRight).GetEnumerator());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return groups.GetHashCode();
		}

		/// <summary>Set the "writeAllCounters" option to true or false</summary>
		/// <param name="send">
		/// if true all counters would be serialized, otherwise only
		/// framework counters would be serialized in
		/// <see cref="AbstractCounters{C, G}.Write(System.IO.DataOutput)"/>
		/// </param>
		[InterfaceAudience.Private]
		public virtual void SetWriteAllCounters(bool send)
		{
			writeAllCounters = send;
		}

		/// <summary>Get the "writeAllCounters" option</summary>
		/// <returns>true of all counters would serialized</returns>
		[InterfaceAudience.Private]
		public virtual bool GetWriteAllCounters()
		{
			return writeAllCounters;
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.Mapreduce.Counters.Limits Limits()
		{
			return limits;
		}
	}
}
