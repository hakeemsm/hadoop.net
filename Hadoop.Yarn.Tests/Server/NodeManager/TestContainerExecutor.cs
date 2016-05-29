using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestContainerExecutor
	{
		private ContainerExecutor containerExecutor = new DefaultContainerExecutor();

		/// <exception cref="System.Exception"/>
		public virtual void TestRunCommandNoPriority()
		{
			Configuration conf = new Configuration();
			string[] command = containerExecutor.GetRunCommand("echo", "group1", "user", null
				, conf);
			NUnit.Framework.Assert.IsTrue("first command should be the run command for the platform"
				, command[0].Equals(Shell.Winutils) || command[0].Equals("bash"));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRunCommandwithPriority()
		{
			Configuration conf = new Configuration();
			conf.SetInt(YarnConfiguration.NmContainerExecutorSchedPriority, 2);
			string[] command = containerExecutor.GetRunCommand("echo", "group1", "user", null
				, conf);
			if (Shell.Windows)
			{
				// windows doesn't currently support
				NUnit.Framework.Assert.AreEqual("first command should be the run command for the platform"
					, Shell.Winutils, command[0]);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("first command should be nice", "nice", command[0
					]);
				NUnit.Framework.Assert.AreEqual("second command should be -n", "-n", command[1]);
				NUnit.Framework.Assert.AreEqual("third command should be the priority", Sharpen.Extensions.ToString
					(2), command[2]);
			}
			// test with negative number
			conf.SetInt(YarnConfiguration.NmContainerExecutorSchedPriority, -5);
			command = containerExecutor.GetRunCommand("echo", "group1", "user", null, conf);
			if (Shell.Windows)
			{
				// windows doesn't currently support
				NUnit.Framework.Assert.AreEqual("first command should be the run command for the platform"
					, Shell.Winutils, command[0]);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("first command should be nice", "nice", command[0
					]);
				NUnit.Framework.Assert.AreEqual("second command should be -n", "-n", command[1]);
				NUnit.Framework.Assert.AreEqual("third command should be the priority", Sharpen.Extensions.ToString
					(-5), command[2]);
			}
		}

		public virtual void TestRunCommandWithNoResources()
		{
			// Windows only test
			Assume.AssumeTrue(Shell.Windows);
			Configuration conf = new Configuration();
			string[] command = containerExecutor.GetRunCommand("echo", "group1", null, null, 
				conf, Resource.NewInstance(1024, 1));
			// Assert the cpu and memory limits are set correctly in the command
			string[] expected = new string[] { Shell.Winutils, "task", "create", "-m", "-1", 
				"-c", "-1", "group1", "cmd /c " + "echo" };
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expected, command));
		}

		public virtual void TestRunCommandWithMemoryOnlyResources()
		{
			// Windows only test
			Assume.AssumeTrue(Shell.Windows);
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmWindowsContainerMemoryLimitEnabled, "true");
			string[] command = containerExecutor.GetRunCommand("echo", "group1", null, null, 
				conf, Resource.NewInstance(1024, 1));
			// Assert the cpu and memory limits are set correctly in the command
			string[] expected = new string[] { Shell.Winutils, "task", "create", "-m", "1024"
				, "-c", "-1", "group1", "cmd /c " + "echo" };
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expected, command));
		}

		public virtual void TestRunCommandWithCpuAndMemoryResources()
		{
			// Windows only test
			Assume.AssumeTrue(Shell.Windows);
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmWindowsContainerCpuLimitEnabled, "true");
			conf.Set(YarnConfiguration.NmWindowsContainerMemoryLimitEnabled, "true");
			string[] command = containerExecutor.GetRunCommand("echo", "group1", null, null, 
				conf, Resource.NewInstance(1024, 1));
			float yarnProcessors = NodeManagerHardwareUtils.GetContainersCores(ResourceCalculatorPlugin
				.GetResourceCalculatorPlugin(null, conf), conf);
			int cpuRate = Math.Min(10000, (int)((1 * 10000) / yarnProcessors));
			// Assert the cpu and memory limits are set correctly in the command
			string[] expected = new string[] { Shell.Winutils, "task", "create", "-m", "1024"
				, "-c", cpuRate.ToString(), "group1", "cmd /c " + "echo" };
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(expected, command));
		}
	}
}
