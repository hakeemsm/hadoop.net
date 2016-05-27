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
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.Util
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
			IList<string> argsList = new AList<string>(Arrays.AsList(args));
			CommandFormat cf = new CommandFormat(0, int.MaxValue, "-glob", "-jar");
			try
			{
				cf.Parse(argsList);
			}
			catch (CommandFormat.UnknownOptionException)
			{
				Terminate(1, "unrecognized option");
				return;
			}
			string classPath = Runtime.GetProperty("java.class.path");
			if (cf.GetOpt("-glob"))
			{
				// The classpath returned from the property has been globbed already.
				System.Console.Out.WriteLine(classPath);
			}
			else
			{
				if (cf.GetOpt("-jar"))
				{
					if (argsList.IsEmpty() || argsList[0] == null || argsList[0].IsEmpty())
					{
						Terminate(1, "-jar option requires path of jar file to write");
						return;
					}
					// Write the classpath into the manifest of a temporary jar file.
					Path workingDir = new Path(Runtime.GetProperty("user.dir"));
					string tmpJarPath;
					try
					{
						tmpJarPath = FileUtil.CreateJarWithClassPath(classPath, workingDir, Sharpen.Runtime.GetEnv
							())[0];
					}
					catch (IOException e)
					{
						Terminate(1, "I/O error creating jar: " + e.Message);
						return;
					}
					// Rename the temporary file to its final location.
					string jarPath = argsList[0];
					try
					{
						FileUtil.ReplaceFile(new FilePath(tmpJarPath), new FilePath(jarPath));
					}
					catch (IOException e)
					{
						Terminate(1, "I/O error renaming jar temporary file to path: " + e.Message);
						return;
					}
				}
			}
		}

		/// <summary>Prints a message to stderr and exits with a status code.</summary>
		/// <param name="status">exit code</param>
		/// <param name="msg">message</param>
		private static void Terminate(int status, string msg)
		{
			System.Console.Error.WriteLine(msg);
			ExitUtil.Terminate(status, msg);
		}
	}
}
