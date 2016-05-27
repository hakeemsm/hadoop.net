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
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>This class returns build information about Hadoop components.</summary>
	public class VersionInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.VersionInfo
			));

		private Properties info;

		protected internal VersionInfo(string component)
		{
			info = new Properties();
			string versionInfoFile = component + "-version-info.properties";
			InputStream @is = null;
			try
			{
				@is = Sharpen.Thread.CurrentThread().GetContextClassLoader().GetResourceAsStream(
					versionInfoFile);
				if (@is == null)
				{
					throw new IOException("Resource not found");
				}
				info.Load(@is);
			}
			catch (IOException ex)
			{
				LogFactory.GetLog(GetType()).Warn("Could not read '" + versionInfoFile + "', " + 
					ex.ToString(), ex);
			}
			finally
			{
				IOUtils.CloseStream(@is);
			}
		}

		protected internal virtual string _getVersion()
		{
			return info.GetProperty("version", "Unknown");
		}

		protected internal virtual string _getRevision()
		{
			return info.GetProperty("revision", "Unknown");
		}

		protected internal virtual string _getBranch()
		{
			return info.GetProperty("branch", "Unknown");
		}

		protected internal virtual string _getDate()
		{
			return info.GetProperty("date", "Unknown");
		}

		protected internal virtual string _getUser()
		{
			return info.GetProperty("user", "Unknown");
		}

		protected internal virtual string _getUrl()
		{
			return info.GetProperty("url", "Unknown");
		}

		protected internal virtual string _getSrcChecksum()
		{
			return info.GetProperty("srcChecksum", "Unknown");
		}

		protected internal virtual string _getBuildVersion()
		{
			return GetVersion() + " from " + _getRevision() + " by " + _getUser() + " source checksum "
				 + _getSrcChecksum();
		}

		protected internal virtual string _getProtocVersion()
		{
			return info.GetProperty("protocVersion", "Unknown");
		}

		private static Org.Apache.Hadoop.Util.VersionInfo CommonVersionInfo = new Org.Apache.Hadoop.Util.VersionInfo
			("common");

		/// <summary>Get the Hadoop version.</summary>
		/// <returns>the Hadoop version string, eg. "0.6.3-dev"</returns>
		public static string GetVersion()
		{
			return CommonVersionInfo._getVersion();
		}

		/// <summary>Get the subversion revision number for the root directory</summary>
		/// <returns>the revision number, eg. "451451"</returns>
		public static string GetRevision()
		{
			return CommonVersionInfo._getRevision();
		}

		/// <summary>Get the branch on which this originated.</summary>
		/// <returns>The branch name, e.g. "trunk" or "branches/branch-0.20"</returns>
		public static string GetBranch()
		{
			return CommonVersionInfo._getBranch();
		}

		/// <summary>The date that Hadoop was compiled.</summary>
		/// <returns>the compilation date in unix date format</returns>
		public static string GetDate()
		{
			return CommonVersionInfo._getDate();
		}

		/// <summary>The user that compiled Hadoop.</summary>
		/// <returns>the username of the user</returns>
		public static string GetUser()
		{
			return CommonVersionInfo._getUser();
		}

		/// <summary>Get the subversion URL for the root Hadoop directory.</summary>
		public static string GetUrl()
		{
			return CommonVersionInfo._getUrl();
		}

		/// <summary>
		/// Get the checksum of the source files from which Hadoop was
		/// built.
		/// </summary>
		public static string GetSrcChecksum()
		{
			return CommonVersionInfo._getSrcChecksum();
		}

		/// <summary>
		/// Returns the buildVersion which includes version,
		/// revision, user and date.
		/// </summary>
		public static string GetBuildVersion()
		{
			return CommonVersionInfo._getBuildVersion();
		}

		/// <summary>Returns the protoc version used for the build.</summary>
		public static string GetProtocVersion()
		{
			return CommonVersionInfo._getProtocVersion();
		}

		public static void Main(string[] args)
		{
			Log.Debug("version: " + GetVersion());
			System.Console.Out.WriteLine("Hadoop " + GetVersion());
			System.Console.Out.WriteLine("Subversion " + GetUrl() + " -r " + GetRevision());
			System.Console.Out.WriteLine("Compiled by " + GetUser() + " on " + GetDate());
			System.Console.Out.WriteLine("Compiled with protoc " + GetProtocVersion());
			System.Console.Out.WriteLine("From source with checksum " + GetSrcChecksum());
			System.Console.Out.WriteLine("This command was run using " + ClassUtil.FindContainingJar
				(typeof(Org.Apache.Hadoop.Util.VersionInfo)));
		}
	}
}
