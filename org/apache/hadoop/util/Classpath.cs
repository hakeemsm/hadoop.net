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
	/// <summary>
	/// Command-line utility for getting the full classpath needed to launch a Hadoop
	/// client application.
	/// </summary>
	/// <remarks>
	/// Command-line utility for getting the full classpath needed to launch a Hadoop
	/// client application.  If the hadoop script is called with "classpath" as the
	/// command, then it simply prints the classpath and exits immediately without
	/// launching a JVM.  The output likely will include wildcards in the classpath.
	/// If there are arguments passed to the classpath command, then this class gets
	/// called.  With the --glob argument, it prints the full classpath with wildcards
	/// expanded.  This is useful in situations where wildcard syntax isn't usable.
	/// With the --jar argument, it writes the classpath as a manifest in a jar file.
	/// This is useful in environments with short limitations on the maximum command
	/// line length, where it may not be possible to specify the full classpath in a
	/// command.  For example, the maximum command line length on Windows is 8191
	/// characters.
	/// </remarks>
	public sealed class Classpath
	{
		private const string usage = "classpath [--glob|--jar <path>|-h|--help] :\n" + "  Prints the classpath needed to get the Hadoop jar and the required\n"
			 + "  libraries.\n" + "  Options:\n" + "\n" + "  --glob       expand wildcards\n"
			 + "  --jar <path> write classpath as manifest in jar named <path>\n" + "  -h, --help   print help\n";

		/// <summary>Main entry point.</summary>
		/// <param name="args">command-line arguments</param>
		public static void Main(string[] args)
		{
			if (args.Length < 1 || args[0].Equals("-h") || args[0].Equals("--help"))
			{
				System.Console.Out.WriteLine(usage);
				return;
			}
			// Copy args, because CommandFormat mutates the list.
			System.Collections.Generic.IList<string> argsList = new System.Collections.Generic.List
				<string>(java.util.Arrays.asList(args));
			org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
				(0, int.MaxValue, "-glob", "-jar");
			try
			{
				cf.parse(argsList);
			}
			catch (org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException)
			{
				terminate(1, "unrecognized option");
				return;
			}
			string classPath = Sharpen.Runtime.getProperty("java.class.path");
			if (cf.getOpt("-glob"))
			{
				// The classpath returned from the property has been globbed already.
				System.Console.Out.WriteLine(classPath);
			}
			else
			{
				if (cf.getOpt("-jar"))
				{
					if (argsList.isEmpty() || argsList[0] == null || argsList[0].isEmpty())
					{
						terminate(1, "-jar option requires path of jar file to write");
						return;
					}
					// Write the classpath into the manifest of a temporary jar file.
					org.apache.hadoop.fs.Path workingDir = new org.apache.hadoop.fs.Path(Sharpen.Runtime
						.getProperty("user.dir"));
					string tmpJarPath;
					try
					{
						tmpJarPath = org.apache.hadoop.fs.FileUtil.createJarWithClassPath(classPath, workingDir
							, Sharpen.Runtime.getenv())[0];
					}
					catch (System.IO.IOException e)
					{
						terminate(1, "I/O error creating jar: " + e.Message);
						return;
					}
					// Rename the temporary file to its final location.
					string jarPath = argsList[0];
					try
					{
						org.apache.hadoop.fs.FileUtil.replaceFile(new java.io.File(tmpJarPath), new java.io.File
							(jarPath));
					}
					catch (System.IO.IOException e)
					{
						terminate(1, "I/O error renaming jar temporary file to path: " + e.Message);
						return;
					}
				}
			}
		}

		/// <summary>Prints a message to stderr and exits with a status code.</summary>
		/// <param name="status">exit code</param>
		/// <param name="msg">message</param>
		private static void terminate(int status, string msg)
		{
			System.Console.Error.WriteLine(msg);
			org.apache.hadoop.util.ExitUtil.terminate(status, msg);
		}
	}
}
