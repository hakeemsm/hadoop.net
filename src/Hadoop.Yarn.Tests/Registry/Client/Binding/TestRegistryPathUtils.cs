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
using NUnit.Framework;
using Org.Apache.Hadoop.Registry.Client.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Binding
{
	public class TestRegistryPathUtils : Assert
	{
		public const string Euro = "\u20AC";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatAscii()
		{
			string @in = "hostname01101101-1";
			AssertConverted(@in, @in);
		}

		/*
		* Euro symbol
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatEuroSymbol()
		{
			AssertConverted("xn--lzg", Euro);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatIdempotent()
		{
			AssertConverted("xn--lzg", RegistryPathUtils.EncodeForRegistry(Euro));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatCyrillicSpaced()
		{
			AssertConverted("xn--pa 3-k4di", "\u0413PA\u0414 3");
		}

		protected internal virtual void AssertConverted(string expected, string @in)
		{
			string @out = RegistryPathUtils.EncodeForRegistry(@in);
			NUnit.Framework.Assert.AreEqual("Conversion of " + @in, expected, @out);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPaths()
		{
			AssertCreatedPathEquals("/", "/", string.Empty);
			AssertCreatedPathEquals("/", string.Empty, string.Empty);
			AssertCreatedPathEquals("/", string.Empty, "/");
			AssertCreatedPathEquals("/", "/", "/");
			AssertCreatedPathEquals("/a", "/a", string.Empty);
			AssertCreatedPathEquals("/a", "/", "a");
			AssertCreatedPathEquals("/a/b", "/a", "b");
			AssertCreatedPathEquals("/a/b", "/a/", "b");
			AssertCreatedPathEquals("/a/b", "/a", "/b");
			AssertCreatedPathEquals("/a/b", "/a", "/b/");
			AssertCreatedPathEquals("/a", "/a", "/");
			AssertCreatedPathEquals("/alice", "/", "/alice");
			AssertCreatedPathEquals("/alice", "/alice", "/");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestComplexPaths()
		{
			AssertCreatedPathEquals("/", string.Empty, string.Empty);
			AssertCreatedPathEquals("/yarn/registry/users/hadoop/org-apache-hadoop", "/yarn/registry"
				, "users/hadoop/org-apache-hadoop/");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void AssertCreatedPathEquals(string expected, string @base, string
			 path)
		{
			string fullPath = RegistryPathUtils.CreateFullPath(@base, path);
			NUnit.Framework.Assert.AreEqual("\"" + @base + "\" + \"" + path + "\" =\"" + fullPath
				 + "\"", expected, fullPath);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplittingEmpty()
		{
			NUnit.Framework.Assert.AreEqual(0, RegistryPathUtils.Split(string.Empty).Count);
			NUnit.Framework.Assert.AreEqual(0, RegistryPathUtils.Split("/").Count);
			NUnit.Framework.Assert.AreEqual(0, RegistryPathUtils.Split("///").Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitting()
		{
			NUnit.Framework.Assert.AreEqual(1, RegistryPathUtils.Split("/a").Count);
			NUnit.Framework.Assert.AreEqual(0, RegistryPathUtils.Split("/").Count);
			NUnit.Framework.Assert.AreEqual(3, RegistryPathUtils.Split("/a/b/c").Count);
			NUnit.Framework.Assert.AreEqual(3, RegistryPathUtils.Split("/a/b/c/").Count);
			NUnit.Framework.Assert.AreEqual(3, RegistryPathUtils.Split("a/b/c").Count);
			NUnit.Framework.Assert.AreEqual(3, RegistryPathUtils.Split("/a/b//c").Count);
			NUnit.Framework.Assert.AreEqual(3, RegistryPathUtils.Split("//a/b/c/").Count);
			IList<string> split = RegistryPathUtils.Split("//a/b/c/");
			NUnit.Framework.Assert.AreEqual("a", split[0]);
			NUnit.Framework.Assert.AreEqual("b", split[1]);
			NUnit.Framework.Assert.AreEqual("c", split[2]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParentOf()
		{
			NUnit.Framework.Assert.AreEqual("/", RegistryPathUtils.ParentOf("/a"));
			NUnit.Framework.Assert.AreEqual("/", RegistryPathUtils.ParentOf("/a/"));
			NUnit.Framework.Assert.AreEqual("/a", RegistryPathUtils.ParentOf("/a/b"));
			NUnit.Framework.Assert.AreEqual("/a/b", RegistryPathUtils.ParentOf("/a/b/c"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLastPathEntry()
		{
			NUnit.Framework.Assert.AreEqual(string.Empty, RegistryPathUtils.LastPathEntry("/"
				));
			NUnit.Framework.Assert.AreEqual(string.Empty, RegistryPathUtils.LastPathEntry("//"
				));
			NUnit.Framework.Assert.AreEqual("c", RegistryPathUtils.LastPathEntry("/a/b/c"));
			NUnit.Framework.Assert.AreEqual("c", RegistryPathUtils.LastPathEntry("/a/b/c/"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestParentOfRoot()
		{
			RegistryPathUtils.ParentOf("/");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidPaths()
		{
			AssertValidPath("/");
			AssertValidPath("/a/b/c");
			AssertValidPath("/users/drwho/org-apache-hadoop/registry/appid-55-55");
			AssertValidPath("/a50");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidPaths()
		{
			AssertInvalidPath("/a_b");
			AssertInvalidPath("/UpperAndLowerCase");
			AssertInvalidPath("/space in string");
		}

		// Is this valid?    assertInvalidPath("/50");
		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	"/>
		private void AssertValidPath(string path)
		{
			RegistryPathUtils.ValidateZKPath(path);
		}

		/// <exception cref="Org.Apache.Hadoop.Registry.Client.Exceptions.InvalidPathnameException
		/// 	"/>
		private void AssertInvalidPath(string path)
		{
			try
			{
				RegistryPathUtils.ValidateElementsAsDNS(path);
				NUnit.Framework.Assert.Fail("path considered valid: " + path);
			}
			catch (InvalidPathnameException)
			{
			}
		}
		// expected
	}
}
