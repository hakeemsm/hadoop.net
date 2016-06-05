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
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>This class finds the package info for Yarn.</summary>
	public class YarnVersionInfo : VersionInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Util.YarnVersionInfo
			));

		private static Org.Apache.Hadoop.Yarn.Util.YarnVersionInfo YarnVersionInfo = new 
			Org.Apache.Hadoop.Yarn.Util.YarnVersionInfo();

		protected internal YarnVersionInfo()
			: base("yarn")
		{
		}

		/// <summary>Get the Yarn version.</summary>
		/// <returns>the Yarn version string, eg. "0.6.3-dev"</returns>
		public static string GetVersion()
		{
			return YarnVersionInfo._getVersion();
		}

		/// <summary>Get the subversion revision number for the root directory</summary>
		/// <returns>the revision number, eg. "451451"</returns>
		public static string GetRevision()
		{
			return YarnVersionInfo._getRevision();
		}

		/// <summary>Get the branch on which this originated.</summary>
		/// <returns>The branch name, e.g. "trunk" or "branches/branch-0.20"</returns>
		public static string GetBranch()
		{
			return YarnVersionInfo._getBranch();
		}

		/// <summary>The date that Yarn was compiled.</summary>
		/// <returns>the compilation date in unix date format</returns>
		public static string GetDate()
		{
			return YarnVersionInfo._getDate();
		}

		/// <summary>The user that compiled Yarn.</summary>
		/// <returns>the username of the user</returns>
		public static string GetUser()
		{
			return YarnVersionInfo._getUser();
		}

		/// <summary>Get the subversion URL for the root Yarn directory.</summary>
		public static string GetUrl()
		{
			return YarnVersionInfo._getUrl();
		}

		/// <summary>
		/// Get the checksum of the source files from which Yarn was
		/// built.
		/// </summary>
		public static string GetSrcChecksum()
		{
			return YarnVersionInfo._getSrcChecksum();
		}

		/// <summary>
		/// Returns the buildVersion which includes version,
		/// revision, user and date.
		/// </summary>
		public static string GetBuildVersion()
		{
			return YarnVersionInfo._getBuildVersion();
		}

		public static void Main(string[] args)
		{
			Log.Debug("version: " + GetVersion());
			System.Console.Out.WriteLine("Yarn " + GetVersion());
			System.Console.Out.WriteLine("Subversion " + GetUrl() + " -r " + GetRevision());
			System.Console.Out.WriteLine("Compiled by " + GetUser() + " on " + GetDate());
			System.Console.Out.WriteLine("From source with checksum " + GetSrcChecksum());
		}
	}
}
