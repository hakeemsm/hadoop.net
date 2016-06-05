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
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>
	/// An abstract class to provide common implementation of the
	/// group factory in both mapred and mapreduce packages.
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class CounterGroupFactory<C, G>
		where C : Counter
		where G : CounterGroupBase<C>
	{
		public interface FrameworkGroupFactory<F>
		{
			F NewGroup(string name);
		}

		private static readonly IDictionary<string, int> s2i = Maps.NewHashMap();

		private static readonly IList<string> i2s = Lists.NewArrayList();

		private const int Version = 1;

		private static readonly string FsGroupName = typeof(FileSystemCounter).FullName;

		private readonly IDictionary<string, CounterGroupFactory.FrameworkGroupFactory<G>
			> fmap = Maps.NewHashMap();

		// Integer mapping (for serialization) for framework groups
		// Add builtin counter class here and the version when changed.
		// Initialize the framework counter group mapping
		private void AddFrameworkGroup<T>()
			where T : Enum<T>
		{
			System.Type cls = typeof(T);
			lock (this)
			{
				UpdateFrameworkGroupMapping(cls);
				fmap[cls.FullName] = NewFrameworkGroupFactory(cls);
			}
		}

		// Update static mappings (c2i, i2s) of framework groups
		private static void UpdateFrameworkGroupMapping(Type cls)
		{
			lock (typeof(CounterGroupFactory))
			{
				string name = cls.FullName;
				int i = s2i[name];
				if (i != null)
				{
					return;
				}
				i2s.AddItem(name);
				s2i[name] = i2s.Count - 1;
			}
		}

		/// <summary>Required override to return a new framework group factory</summary>
		/// <?/>
		/// <param name="cls">the counter enum class</param>
		/// <returns>a new framework group factory</returns>
		protected internal abstract CounterGroupFactory.FrameworkGroupFactory<G> NewFrameworkGroupFactory
			<T>()
			where T : Enum<T>;

		/// <summary>Create a new counter group</summary>
		/// <param name="name">of the group</param>
		/// <param name="limits">the counters limits policy object</param>
		/// <returns>a new counter group</returns>
		public virtual G NewGroup(string name, Limits limits)
		{
			return NewGroup(name, ResourceBundles.GetCounterGroupName(name, name), limits);
		}

		/// <summary>Create a new counter group</summary>
		/// <param name="name">of the group</param>
		/// <param name="displayName">of the group</param>
		/// <param name="limits">the counters limits policy object</param>
		/// <returns>a new counter group</returns>
		public virtual G NewGroup(string name, string displayName, Limits limits)
		{
			CounterGroupFactory.FrameworkGroupFactory<G> gf = fmap[name];
			if (gf != null)
			{
				return gf.NewGroup(name);
			}
			if (name.Equals(FsGroupName))
			{
				return NewFileSystemGroup();
			}
			else
			{
				if (s2i[name] != null)
				{
					return NewFrameworkGroup(s2i[name]);
				}
			}
			return NewGenericGroup(name, displayName, limits);
		}

		/// <summary>Create a new framework group</summary>
		/// <param name="id">of the group</param>
		/// <returns>a new framework group</returns>
		public virtual G NewFrameworkGroup(int id)
		{
			string name;
			lock (typeof(CounterGroupFactory))
			{
				if (id < 0 || id >= i2s.Count)
				{
					ThrowBadFrameGroupIdException(id);
				}
				name = i2s[id];
			}
			// should not throw here.
			CounterGroupFactory.FrameworkGroupFactory<G> gf = fmap[name];
			if (gf == null)
			{
				ThrowBadFrameGroupIdException(id);
			}
			return gf.NewGroup(name);
		}

		/// <summary>Get the id of a framework group</summary>
		/// <param name="name">of the group</param>
		/// <returns>the framework group id</returns>
		public static int GetFrameworkGroupId(string name)
		{
			lock (typeof(CounterGroupFactory))
			{
				int i = s2i[name];
				if (i == null)
				{
					ThrowBadFrameworkGroupNameException(name);
				}
				return i;
			}
		}

		/// <returns>the counter factory version</returns>
		public virtual int Version()
		{
			return Version;
		}

		/// <summary>
		/// Check whether a group name is a name of a framework group (including
		/// the filesystem group).
		/// </summary>
		/// <param name="name">to check</param>
		/// <returns>true for framework group names</returns>
		public static bool IsFrameworkGroup(string name)
		{
			lock (typeof(CounterGroupFactory))
			{
				return s2i[name] != null || name.Equals(FsGroupName);
			}
		}

		private static void ThrowBadFrameGroupIdException(int id)
		{
			throw new ArgumentException("bad framework group id: " + id);
		}

		private static void ThrowBadFrameworkGroupNameException(string name)
		{
			throw new ArgumentException("bad framework group name: " + name);
		}

		/// <summary>Abstract factory method to create a generic (vs framework) counter group
		/// 	</summary>
		/// <param name="name">of the group</param>
		/// <param name="displayName">of the group</param>
		/// <param name="limits">limits of the counters</param>
		/// <returns>a new generic counter group</returns>
		protected internal abstract G NewGenericGroup(string name, string displayName, Limits
			 limits);

		/// <summary>Abstract factory method to create a file system counter group</summary>
		/// <returns>a new file system counter group</returns>
		protected internal abstract G NewFileSystemGroup();

		public CounterGroupFactory()
		{
			{
				AddFrameworkGroup<TaskCounter>();
				AddFrameworkGroup<JobCounter>();
			}
		}
	}
}
