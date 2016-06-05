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
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Contract
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
	public abstract class AbstractBondedFSContract : AbstractFSContract
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Contract.AbstractBondedFSContract
			));

		/// <summary>Pattern for the option for test filesystems from schema</summary>
		public const string FsnameOption = "test.fs.%s";

		/// <summary>Constructor: loads the authentication keys if found</summary>
		/// <param name="conf">configuration to work with</param>
		protected internal AbstractBondedFSContract(Configuration conf)
			: base(conf)
		{
		}

		private string fsName;

		private URI fsURI;

		private FileSystem filesystem;

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
			base.Init();
			//this test is only enabled if the test FS is present
			fsName = LoadFilesystemName(GetScheme());
			SetEnabled(!fsName.IsEmpty());
			if (IsEnabled())
			{
				try
				{
					fsURI = new URI(fsName);
					filesystem = FileSystem.Get(fsURI, GetConf());
				}
				catch (URISyntaxException)
				{
					throw new IOException("Invalid URI " + fsName);
				}
				catch (ArgumentException e)
				{
					throw new IOException("Invalid URI " + fsName, e);
				}
			}
			else
			{
				Log.Info("skipping tests as FS name is not defined in " + GetFilesystemConfKey());
			}
		}

		/// <summary>Load the name of a test filesystem.</summary>
		/// <param name="schema">schema to look up</param>
		/// <returns>the filesystem name -or "" if none was defined</returns>
		public virtual string LoadFilesystemName(string schema)
		{
			return GetOption(string.Format(FsnameOption, schema), string.Empty);
		}

		/// <summary>Get the conf key for a filesystem</summary>
		protected internal virtual string GetFilesystemConfKey()
		{
			return GetConfKey(string.Format(FsnameOption, GetScheme()));
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileSystem GetTestFileSystem()
		{
			return filesystem;
		}

		public override Path GetTestPath()
		{
			Path path = new Path("/test");
			return path;
		}

		public override string ToString()
		{
			return GetScheme() + " Contract against " + fsName;
		}
	}
}
