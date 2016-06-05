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
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Org.Apache.Hadoop.Registry.Client.Impl.ZK;
using Org.Apache.Zookeeper.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	/// <summary>
	/// Basic operations on paths: manipulating them and creating and validating
	/// path elements.
	/// </summary>
	public class RegistryPathUtils
	{
		/// <summary>Compiled down pattern to validate single entries in the path</summary>
		private static readonly Sharpen.Pattern PathEntryValidationPattern = Sharpen.Pattern
			.Compile(RegistryInternalConstants.ValidPathEntryPattern);

		/// <summary>
		/// Validate ZK path with the path itself included in
		/// the exception text
		/// </summary>
		/// <param name="path">path to validate</param>
		/// <returns>the path parameter</returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">if the pathname is invalid.</exception>
		public static string ValidateZKPath(string path)
		{
			try
			{
				PathUtils.ValidatePath(path);
			}
			catch (ArgumentException e)
			{
				throw new InvalidPathnameException(path, "Invalid Path \"" + path + "\" : " + e, 
					e);
			}
			return path;
		}

		/// <summary>Validate ZK path as valid for a DNS hostname.</summary>
		/// <param name="path">path to validate</param>
		/// <returns>the path parameter</returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">if the pathname is invalid.</exception>
		public static string ValidateElementsAsDNS(string path)
		{
			IList<string> splitpath = Split(path);
			foreach (string fragment in splitpath)
			{
				if (!PathEntryValidationPattern.Matcher(fragment).Matches())
				{
					throw new InvalidPathnameException(path, "Invalid Path element \"" + fragment + "\""
						);
				}
			}
			return path;
		}

		/// <summary>Create a full path from the registry root and the supplied subdir</summary>
		/// <param name="path">path of operation</param>
		/// <returns>an absolute path</returns>
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	">if the path is invalid</exception>
		public static string CreateFullPath(string @base, string path)
		{
			Preconditions.CheckArgument(path != null, "null path");
			Preconditions.CheckArgument(@base != null, "null path");
			return ValidateZKPath(Join(@base, path));
		}

		/// <summary>
		/// Join two paths, guaranteeing that there will not be exactly
		/// one separator between the two, and exactly one at the front
		/// of the path.
		/// </summary>
		/// <remarks>
		/// Join two paths, guaranteeing that there will not be exactly
		/// one separator between the two, and exactly one at the front
		/// of the path. There will be no trailing "/" except for the special
		/// case that this is the root path
		/// </remarks>
		/// <param name="base">base path</param>
		/// <param name="path">second path to add</param>
		/// <returns>a combined path.</returns>
		public static string Join(string @base, string path)
		{
			Preconditions.CheckArgument(path != null, "null path");
			Preconditions.CheckArgument(@base != null, "null path");
			StringBuilder fullpath = new StringBuilder();
			if (!@base.StartsWith("/"))
			{
				fullpath.Append('/');
			}
			fullpath.Append(@base);
			// guarantee a trailing /
			if (!fullpath.ToString().EndsWith("/"))
			{
				fullpath.Append("/");
			}
			// strip off any at the beginning
			if (path.StartsWith("/"))
			{
				// path starts with /, so append all other characters -if present
				if (path.Length > 1)
				{
					fullpath.Append(Sharpen.Runtime.Substring(path, 1));
				}
			}
			else
			{
				fullpath.Append(path);
			}
			//here there may be a trailing "/"
			string finalpath = fullpath.ToString();
			if (finalpath.EndsWith("/") && !"/".Equals(finalpath))
			{
				finalpath = Sharpen.Runtime.Substring(finalpath, 0, finalpath.Length - 1);
			}
			return finalpath;
		}

		/// <summary>split a path into elements, stripping empty elements</summary>
		/// <param name="path">the path</param>
		/// <returns>the split path</returns>
		public static IList<string> Split(string path)
		{
			//
			string[] pathelements = path.Split("/");
			IList<string> dirs = new AList<string>(pathelements.Length);
			foreach (string pathelement in pathelements)
			{
				if (!pathelement.IsEmpty())
				{
					dirs.AddItem(pathelement);
				}
			}
			return dirs;
		}

		/// <summary>
		/// Get the last entry in a path; for an empty path
		/// returns "".
		/// </summary>
		/// <remarks>
		/// Get the last entry in a path; for an empty path
		/// returns "". The split logic is that of
		/// <see cref="Split(string)"/>
		/// </remarks>
		/// <param name="path">path of operation</param>
		/// <returns>the last path entry or "" if none.</returns>
		public static string LastPathEntry(string path)
		{
			IList<string> splits = Split(path);
			if (splits.IsEmpty())
			{
				// empty path. Return ""
				return string.Empty;
			}
			else
			{
				return splits[splits.Count - 1];
			}
		}

		/// <summary>Get the parent of a path</summary>
		/// <param name="path">path to look at</param>
		/// <returns>the parent path</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathNotFoundException">if the path was at root.
		/// 	</exception>
		public static string ParentOf(string path)
		{
			IList<string> elements = Split(path);
			int size = elements.Count;
			if (size == 0)
			{
				throw new PathNotFoundException("No parent of " + path);
			}
			if (size == 1)
			{
				return "/";
			}
			elements.Remove(size - 1);
			StringBuilder parent = new StringBuilder(path.Length);
			foreach (string element in elements)
			{
				parent.Append("/");
				parent.Append(element);
			}
			return parent.ToString();
		}

		/// <summary>
		/// Perform any formatting for the registry needed to convert
		/// non-simple-DNS elements
		/// </summary>
		/// <param name="element">element to encode</param>
		/// <returns>an encoded string</returns>
		public static string EncodeForRegistry(string element)
		{
			return IDN.ToASCII(element);
		}

		/// <summary>
		/// Perform whatever transforms are needed to get a YARN ID into
		/// a DNS-compatible name
		/// </summary>
		/// <param name="yarnId">ID as string of YARN application, instance or container</param>
		/// <returns>a string suitable for use in registry paths.</returns>
		public static string EncodeYarnID(string yarnId)
		{
			return yarnId.Replace("_", "-");
		}
	}
}
