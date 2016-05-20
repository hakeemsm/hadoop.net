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
	/// <summary>This class returns build information about Hadoop components.</summary>
	public class VersionInfo
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.VersionInfo
			)));

		private java.util.Properties info;

		protected internal VersionInfo(string component)
		{
			info = new java.util.Properties();
			string versionInfoFile = component + "-version-info.properties";
			java.io.InputStream @is = null;
			try
			{
				@is = java.lang.Thread.currentThread().getContextClassLoader().getResourceAsStream
					(versionInfoFile);
				if (@is == null)
				{
					throw new System.IO.IOException("Resource not found");
				}
				info.load(@is);
			}
			catch (System.IO.IOException ex)
			{
				org.apache.commons.logging.LogFactory.getLog(Sharpen.Runtime.getClassForObject(this
					)).warn("Could not read '" + versionInfoFile + "', " + ex.ToString(), ex);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@is);
			}
		}

		protected internal virtual string _getVersion()
		{
			return info.getProperty("version", "Unknown");
		}

		protected internal virtual string _getRevision()
		{
			return info.getProperty("revision", "Unknown");
		}

		protected internal virtual string _getBranch()
		{
			return info.getProperty("branch", "Unknown");
		}

		protected internal virtual string _getDate()
		{
			return info.getProperty("date", "Unknown");
		}

		protected internal virtual string _getUser()
		{
			return info.getProperty("user", "Unknown");
		}

		protected internal virtual string _getUrl()
		{
			return info.getProperty("url", "Unknown");
		}

		protected internal virtual string _getSrcChecksum()
		{
			return info.getProperty("srcChecksum", "Unknown");
		}

		protected internal virtual string _getBuildVersion()
		{
			return getVersion() + " from " + _getRevision() + " by " + _getUser() + " source checksum "
				 + _getSrcChecksum();
		}

		protected internal virtual string _getProtocVersion()
		{
			return info.getProperty("protocVersion", "Unknown");
		}

		private static org.apache.hadoop.util.VersionInfo COMMON_VERSION_INFO = new org.apache.hadoop.util.VersionInfo
			("common");

		/// <summary>Get the Hadoop version.</summary>
		/// <returns>the Hadoop version string, eg. "0.6.3-dev"</returns>
		public static string getVersion()
		{
			return COMMON_VERSION_INFO._getVersion();
		}

		/// <summary>Get the subversion revision number for the root directory</summary>
		/// <returns>the revision number, eg. "451451"</returns>
		public static string getRevision()
		{
			return COMMON_VERSION_INFO._getRevision();
		}

		/// <summary>Get the branch on which this originated.</summary>
		/// <returns>The branch name, e.g. "trunk" or "branches/branch-0.20"</returns>
		public static string getBranch()
		{
			return COMMON_VERSION_INFO._getBranch();
		}

		/// <summary>The date that Hadoop was compiled.</summary>
		/// <returns>the compilation date in unix date format</returns>
		public static string getDate()
		{
			return COMMON_VERSION_INFO._getDate();
		}

		/// <summary>The user that compiled Hadoop.</summary>
		/// <returns>the username of the user</returns>
		public static string getUser()
		{
			return COMMON_VERSION_INFO._getUser();
		}

		/// <summary>Get the subversion URL for the root Hadoop directory.</summary>
		public static string getUrl()
		{
			return COMMON_VERSION_INFO._getUrl();
		}

		/// <summary>
		/// Get the checksum of the source files from which Hadoop was
		/// built.
		/// </summary>
		public static string getSrcChecksum()
		{
			return COMMON_VERSION_INFO._getSrcChecksum();
		}

		/// <summary>
		/// Returns the buildVersion which includes version,
		/// revision, user and date.
		/// </summary>
		public static string getBuildVersion()
		{
			return COMMON_VERSION_INFO._getBuildVersion();
		}

		/// <summary>Returns the protoc version used for the build.</summary>
		public static string getProtocVersion()
		{
			return COMMON_VERSION_INFO._getProtocVersion();
		}

		public static void Main(string[] args)
		{
			LOG.debug("version: " + getVersion());
			System.Console.Out.WriteLine("Hadoop " + getVersion());
			System.Console.Out.WriteLine("Subversion " + getUrl() + " -r " + getRevision());
			System.Console.Out.WriteLine("Compiled by " + getUser() + " on " + getDate());
			System.Console.Out.WriteLine("Compiled with protoc " + getProtocVersion());
			System.Console.Out.WriteLine("From source with checksum " + getSrcChecksum());
			System.Console.Out.WriteLine("This command was run using " + org.apache.hadoop.util.ClassUtil
				.findContainingJar(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.VersionInfo
				))));
		}
	}
}
