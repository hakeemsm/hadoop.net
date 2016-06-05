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
using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>
	/// Class representing a filesystem contract that a filesystem
	/// implementation is expected implement.
	/// </summary>
	/// <remarks>
	/// Class representing a filesystem contract that a filesystem
	/// implementation is expected implement.
	/// Part of this contract class is to allow FS implementations to
	/// provide specific opt outs and limits, so that tests can be
	/// skip unsupported features (e.g. case sensitivity tests),
	/// dangerous operations (e.g. trying to delete the root directory),
	/// and limit filesize and other numeric variables for scale tests
	/// </remarks>
	public abstract class AbstractFSContract : Configured
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.FS.Contract.AbstractFSContract
			));

		private bool enabled = true;

		/// <summary>Constructor: loads the authentication keys if found</summary>
		/// <param name="conf">configuration to work with</param>
		protected internal AbstractFSContract(Configuration conf)
			: base(conf)
		{
			if (MaybeAddConfResource(ContractOptions.ContractOptionsResource))
			{
				Log.Debug("Loaded authentication keys from {}", ContractOptions.ContractOptionsResource
					);
			}
			else
			{
				Log.Debug("Not loaded: {}", ContractOptions.ContractOptionsResource);
			}
		}

		/// <summary>Any initialisation logic can go here</summary>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void Init()
		{
		}

		/// <summary>Add a configuration resource to this instance's configuration</summary>
		/// <param name="resource">resource reference</param>
		/// <exception cref="System.Exception">if the resource was not found.</exception>
		protected internal virtual void AddConfResource(string resource)
		{
			bool found = MaybeAddConfResource(resource);
			Assert.True("Resource not found " + resource, found);
		}

		/// <summary>
		/// Add a configuration resource to this instance's configuration,
		/// return true if the resource was found
		/// </summary>
		/// <param name="resource">resource reference</param>
		protected internal virtual bool MaybeAddConfResource(string resource)
		{
			Uri url = this.GetType().GetClassLoader().GetResource(resource);
			bool found = url != null;
			if (found)
			{
				GetConf().AddResource(resource);
			}
			return found;
		}

		/// <summary>Get the FS from a URI.</summary>
		/// <remarks>
		/// Get the FS from a URI. The default implementation just retrieves
		/// it from the norrmal FileSystem factory/cache, with the local configuration
		/// </remarks>
		/// <param name="uri">URI of FS</param>
		/// <returns>the filesystem</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual FileSystem GetFileSystem(URI uri)
		{
			return FileSystem.Get(uri, GetConf());
		}

		/// <summary>Get the filesystem for these tests</summary>
		/// <returns>the test fs</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public abstract FileSystem GetTestFileSystem();

		/// <summary>Get the scheme of this FS</summary>
		/// <returns>the scheme this FS supports</returns>
		public abstract string GetScheme();

		/// <summary>Return the path string for tests, e.g.</summary>
		/// <remarks>Return the path string for tests, e.g. <code>file:///tmp</code></remarks>
		/// <returns>a path in the test FS</returns>
		public abstract Path GetTestPath();

		/// <summary>
		/// Boolean to indicate whether or not the contract test are enabled
		/// for this test run.
		/// </summary>
		/// <returns>true if the tests can be run.</returns>
		public virtual bool IsEnabled()
		{
			return enabled;
		}

		/// <summary>
		/// Boolean to indicate whether or not the contract test are enabled
		/// for this test run.
		/// </summary>
		/// <param name="enabled">flag which must be true if the tests can be run.</param>
		public virtual void SetEnabled(bool enabled)
		{
			this.enabled = enabled;
		}

		/// <summary>Query for a feature being supported.</summary>
		/// <remarks>Query for a feature being supported. This may include a probe for the feature
		/// 	</remarks>
		/// <param name="feature">feature to query</param>
		/// <param name="defval">default value</param>
		/// <returns>true if the feature is supported</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual bool IsSupported(string feature, bool defval)
		{
			return GetConf().GetBoolean(GetConfKey(feature), defval);
		}

		/// <summary>Query for a feature's limit.</summary>
		/// <remarks>Query for a feature's limit. This may include a probe for the feature</remarks>
		/// <param name="feature">feature to query</param>
		/// <param name="defval">default value</param>
		/// <returns>true if the feature is supported</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual int GetLimit(string feature, int defval)
		{
			return GetConf().GetInt(GetConfKey(feature), defval);
		}

		public virtual string GetOption(string feature, string defval)
		{
			return GetConf().Get(GetConfKey(feature), defval);
		}

		/// <summary>Build a configuration key</summary>
		/// <param name="feature">feature to query</param>
		/// <returns>the configuration key base with the feature appended</returns>
		public virtual string GetConfKey(string feature)
		{
			return ContractOptions.FsContractKey + feature;
		}

		/// <summary>Create a URI off the scheme</summary>
		/// <param name="path">path of URI</param>
		/// <returns>a URI</returns>
		/// <exception cref="System.IO.IOException">if the URI could not be created</exception>
		protected internal virtual URI ToURI(string path)
		{
			try
			{
				return new URI(GetScheme(), path, null);
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e.ToString() + " with " + path, e);
			}
		}

		public override string ToString()
		{
			return "FSContract for " + GetScheme();
		}
	}
}
