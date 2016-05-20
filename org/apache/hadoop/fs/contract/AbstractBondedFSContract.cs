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

namespace org.apache.hadoop.fs.contract
{
	/// <summary>
	/// This is a filesystem contract for any class that bonds to a filesystem
	/// through the configuration.
	/// </summary>
	/// <remarks>
	/// This is a filesystem contract for any class that bonds to a filesystem
	/// through the configuration.
	/// It looks for a definition of the test filesystem with the key
	/// derived from "fs.contract.test.fs.%s" -if found the value
	/// is converted to a URI and used to create a filesystem. If not -the
	/// tests are not enabled
	/// </remarks>
	public abstract class AbstractBondedFSContract : org.apache.hadoop.fs.contract.AbstractFSContract
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractBondedFSContract
			)));

		/// <summary>Pattern for the option for test filesystems from schema</summary>
		public const string FSNAME_OPTION = "test.fs.%s";

		/// <summary>Constructor: loads the authentication keys if found</summary>
		/// <param name="conf">configuration to work with</param>
		protected internal AbstractBondedFSContract(org.apache.hadoop.conf.Configuration 
			conf)
			: base(conf)
		{
		}

		private string fsName;

		private java.net.URI fsURI;

		private org.apache.hadoop.fs.FileSystem filesystem;

		/// <exception cref="System.IO.IOException"/>
		public override void init()
		{
			base.init();
			//this test is only enabled if the test FS is present
			fsName = loadFilesystemName(getScheme());
			setEnabled(!fsName.isEmpty());
			if (isEnabled())
			{
				try
				{
					fsURI = new java.net.URI(fsName);
					filesystem = org.apache.hadoop.fs.FileSystem.get(fsURI, getConf());
				}
				catch (java.net.URISyntaxException)
				{
					throw new System.IO.IOException("Invalid URI " + fsName);
				}
				catch (System.ArgumentException e)
				{
					throw new System.IO.IOException("Invalid URI " + fsName, e);
				}
			}
			else
			{
				LOG.info("skipping tests as FS name is not defined in " + getFilesystemConfKey());
			}
		}

		/// <summary>Load the name of a test filesystem.</summary>
		/// <param name="schema">schema to look up</param>
		/// <returns>the filesystem name -or "" if none was defined</returns>
		public virtual string loadFilesystemName(string schema)
		{
			return getOption(string.format(FSNAME_OPTION, schema), string.Empty);
		}

		/// <summary>Get the conf key for a filesystem</summary>
		protected internal virtual string getFilesystemConfKey()
		{
			return getConfKey(string.format(FSNAME_OPTION, getScheme()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileSystem getTestFileSystem()
		{
			return filesystem;
		}

		public override org.apache.hadoop.fs.Path getTestPath()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/test");
			return path;
		}

		public override string ToString()
		{
			return getScheme() + " Contract against " + fsName;
		}
	}
}
