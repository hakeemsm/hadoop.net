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

namespace org.apache.hadoop.cli.util
{
	public class FSCmdExecutor : org.apache.hadoop.cli.util.CommandExecutor
	{
		protected internal string namenode = null;

		protected internal org.apache.hadoop.fs.FsShell shell = null;

		public FSCmdExecutor(string namenode, org.apache.hadoop.fs.FsShell shell)
		{
			this.namenode = namenode;
			this.shell = shell;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void execute(string cmd)
		{
			string[] args = getCommandAsArgs(cmd, "NAMENODE", this.namenode);
			org.apache.hadoop.util.ToolRunner.run(shell, args);
		}
	}
}
