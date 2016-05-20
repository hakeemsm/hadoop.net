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

namespace org.apache.hadoop.fs.contract
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
	public abstract class AbstractFSContract : org.apache.hadoop.conf.Configured
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractFSContract
			)));

		private bool enabled = true;

		/// <summary>Constructor: loads the authentication keys if found</summary>
		/// <param name="conf">configuration to work with</param>
		protected internal AbstractFSContract(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			if (maybeAddConfResource(org.apache.hadoop.fs.contract.ContractOptions.CONTRACT_OPTIONS_RESOURCE
				))
			{
				LOG.debug("Loaded authentication keys from {}", org.apache.hadoop.fs.contract.ContractOptions
					.CONTRACT_OPTIONS_RESOURCE);
			}
			else
			{
				LOG.debug("Not loaded: {}", org.apache.hadoop.fs.contract.ContractOptions.CONTRACT_OPTIONS_RESOURCE
					);
			}
		}

		/// <summary>Any initialisation logic can go here</summary>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual void init()
		{
		}

		/// <summary>Add a configuration resource to this instance's configuration</summary>
		/// <param name="resource">resource reference</param>
		/// <exception cref="java.lang.AssertionError">if the resource was not found.</exception>
		protected internal virtual void addConfResource(string resource)
		{
			bool found = maybeAddConfResource(resource);
			NUnit.Framework.Assert.IsTrue("Resource not found " + resource, found);
		}

		/// <summary>
		/// Add a configuration resource to this instance's configuration,
		/// return true if the resource was found
		/// </summary>
		/// <param name="resource">resource reference</param>
		protected internal virtual bool maybeAddConfResource(string resource)
		{
			java.net.URL url = Sharpen.Runtime.getClassForObject(this).getClassLoader().getResource
				(resource);
			bool found = url != null;
			if (found)
			{
				getConf().addResource(resource);
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
		public virtual org.apache.hadoop.fs.FileSystem getFileSystem(java.net.URI uri)
		{
			return org.apache.hadoop.fs.FileSystem.get(uri, getConf());
		}

		/// <summary>Get the filesystem for these tests</summary>
		/// <returns>the test fs</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public abstract org.apache.hadoop.fs.FileSystem getTestFileSystem();

		/// <summary>Get the scheme of this FS</summary>
		/// <returns>the scheme this FS supports</returns>
		public abstract string getScheme();

		/// <summary>Return the path string for tests, e.g.</summary>
		/// <remarks>Return the path string for tests, e.g. <code>file:///tmp</code></remarks>
		/// <returns>a path in the test FS</returns>
		public abstract org.apache.hadoop.fs.Path getTestPath();

		/// <summary>
		/// Boolean to indicate whether or not the contract test are enabled
		/// for this test run.
		/// </summary>
		/// <returns>true if the tests can be run.</returns>
		public virtual bool isEnabled()
		{
			return enabled;
		}

		/// <summary>
		/// Boolean to indicate whether or not the contract test are enabled
		/// for this test run.
		/// </summary>
		/// <param name="enabled">flag which must be true if the tests can be run.</param>
		public virtual void setEnabled(bool enabled)
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
		public virtual bool isSupported(string feature, bool defval)
		{
			return getConf().getBoolean(getConfKey(feature), defval);
		}

		/// <summary>Query for a feature's limit.</summary>
		/// <remarks>Query for a feature's limit. This may include a probe for the feature</remarks>
		/// <param name="feature">feature to query</param>
		/// <param name="defval">default value</param>
		/// <returns>true if the feature is supported</returns>
		/// <exception cref="System.IO.IOException">IO problems</exception>
		public virtual int getLimit(string feature, int defval)
		{
			return getConf().getInt(getConfKey(feature), defval);
		}

		public virtual string getOption(string feature, string defval)
		{
			return getConf().get(getConfKey(feature), defval);
		}

		/// <summary>Build a configuration key</summary>
		/// <param name="feature">feature to query</param>
		/// <returns>the configuration key base with the feature appended</returns>
		public virtual string getConfKey(string feature)
		{
			return org.apache.hadoop.fs.contract.ContractOptions.FS_CONTRACT_KEY + feature;
		}

		/// <summary>Create a URI off the scheme</summary>
		/// <param name="path">path of URI</param>
		/// <returns>a URI</returns>
		/// <exception cref="System.IO.IOException">if the URI could not be created</exception>
		protected internal virtual java.net.URI toURI(string path)
		{
			try
			{
				return new java.net.URI(getScheme(), path, null);
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.IO.IOException(e.ToString() + " with " + path, e);
			}
		}

		public override string ToString()
		{
			return "FSContract for " + getScheme();
		}
	}
}
