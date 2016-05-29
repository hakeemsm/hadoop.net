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
using Org.Apache.Hadoop.Registry;
using Org.Apache.Hadoop.Registry.Operations;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Cli
{
	public class TestRegistryCli : AbstractRegistryTest
	{
		protected internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRegistryOperations
			));

		private ByteArrayOutputStream sysOutStream;

		private TextWriter sysOut;

		private ByteArrayOutputStream sysErrStream;

		private TextWriter sysErr;

		private RegistryCli cli;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			sysOutStream = new ByteArrayOutputStream();
			sysOut = new TextWriter(sysOutStream);
			sysErrStream = new ByteArrayOutputStream();
			sysErr = new TextWriter(sysErrStream);
			Runtime.SetOut(sysOut);
			cli = new RegistryCli(operations, CreateRegistryConfiguration(), sysOut, sysErr);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			cli.Close();
		}

		/// <exception cref="System.Exception"/>
		private void AssertResult(RegistryCli cli, int code, params string[] args)
		{
			int result = cli.Run(args);
			NUnit.Framework.Assert.AreEqual(code, result);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadCommands()
		{
			AssertResult(cli, -1, new string[] {  });
			AssertResult(cli, -1, "foo");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidNumArgs()
		{
			AssertResult(cli, -1, "ls");
			AssertResult(cli, -1, "ls", "/path", "/extraPath");
			AssertResult(cli, -1, "resolve");
			AssertResult(cli, -1, "resolve", "/path", "/extraPath");
			AssertResult(cli, -1, "mknode");
			AssertResult(cli, -1, "mknode", "/path", "/extraPath");
			AssertResult(cli, -1, "rm");
			AssertResult(cli, -1, "rm", "/path", "/extraPath");
			AssertResult(cli, -1, "bind");
			AssertResult(cli, -1, "bind", "foo");
			AssertResult(cli, -1, "bind", "-inet", "foo");
			AssertResult(cli, -1, "bind", "-inet", "-api", "-p", "378", "-h", "host", "/foo");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "-h", "host", "/foo");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "/foo");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host");
			AssertResult(cli, -1, "bind", "-api", "Api", "-p", "378", "-h", "host", "/foo");
			AssertResult(cli, -1, "bind", "-webui", "foo");
			AssertResult(cli, -1, "bind", "-webui", "-api", "Api", "/foo");
			AssertResult(cli, -1, "bind", "-webui", "uriString", "-api", "/foo");
			AssertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api");
			AssertResult(cli, -1, "bind", "-rest", "foo");
			AssertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api");
			AssertResult(cli, -1, "bind", "-rest", "-api", "Api", "/foo");
			AssertResult(cli, -1, "bind", "-rest", "uriString", "-api", "/foo");
			AssertResult(cli, -1, "bind", "uriString", "-api", "Api", "/foo");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadArgType()
		{
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "fooPort", "-h", "host"
				, "/dir");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadPath()
		{
			AssertResult(cli, -1, "ls", "NonSlashPath");
			AssertResult(cli, -1, "ls", "//");
			AssertResult(cli, -1, "resolve", "NonSlashPath");
			AssertResult(cli, -1, "resolve", "//");
			AssertResult(cli, -1, "mknode", "NonSlashPath");
			AssertResult(cli, -1, "mknode", "//");
			AssertResult(cli, -1, "rm", "NonSlashPath");
			AssertResult(cli, -1, "rm", "//");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", 
				"NonSlashPath");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", 
				"//");
			AssertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "NonSlashPath"
				);
			AssertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "//");
			AssertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "NonSlashPath"
				);
			AssertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "//");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotExistingPaths()
		{
			AssertResult(cli, -1, "ls", "/nonexisting_path");
			AssertResult(cli, -1, "ls", "/NonExistingDir/nonexisting_path");
			AssertResult(cli, -1, "resolve", "/nonexisting_path");
			AssertResult(cli, -1, "resolve", "/NonExistingDir/nonexisting_path");
			AssertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", 
				"/NonExistingDir/nonexisting_path");
			AssertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "/NonExistingDir/nonexisting_path"
				);
			AssertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "/NonExistingDir/nonexisting_path"
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidCommands()
		{
			AssertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/foo"
				);
			AssertResult(cli, 0, "resolve", "/foo");
			AssertResult(cli, 0, "rm", "/foo");
			AssertResult(cli, -1, "resolve", "/foo");
			AssertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/foo");
			AssertResult(cli, 0, "resolve", "/foo");
			AssertResult(cli, 0, "rm", "/foo");
			AssertResult(cli, -1, "resolve", "/foo");
			AssertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/foo");
			AssertResult(cli, 0, "resolve", "/foo");
			AssertResult(cli, 0, "rm", "/foo");
			AssertResult(cli, -1, "resolve", "/foo");
			//Test Sub Directories Binds
			AssertResult(cli, 0, "mknode", "/subdir");
			AssertResult(cli, -1, "resolve", "/subdir");
			AssertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/subdir/foo"
				);
			AssertResult(cli, 0, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "rm", "/subdir/foo");
			AssertResult(cli, -1, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/subdir/foo");
			AssertResult(cli, 0, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "rm", "/subdir/foo");
			AssertResult(cli, -1, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/subdir/foo");
			AssertResult(cli, 0, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "rm", "/subdir/foo");
			AssertResult(cli, -1, "resolve", "/subdir/foo");
			AssertResult(cli, 0, "rm", "/subdir");
			AssertResult(cli, -1, "resolve", "/subdir");
			//Test Bind that the dir itself
			AssertResult(cli, 0, "mknode", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/dir"
				);
			AssertResult(cli, 0, "resolve", "/dir");
			AssertResult(cli, 0, "rm", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "mknode", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/dir");
			AssertResult(cli, 0, "resolve", "/dir");
			AssertResult(cli, 0, "rm", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "mknode", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/dir");
			AssertResult(cli, 0, "resolve", "/dir");
			AssertResult(cli, 0, "rm", "/dir");
			AssertResult(cli, -1, "resolve", "/dir");
			AssertResult(cli, 0, "rm", "/Nonexitent");
		}
	}
}
